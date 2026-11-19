# -*- coding: utf-8 -*-
"""wb_querywords 每日 worker：纯 input 驱动的推荐词抓取。

流程（单常驻循环）:
    1. 建 OSS bucket（input 源，必需）。
    2. _ensure_current：扫描 OSS input 取最大日期 max_date
       - 本地比 cur 旧的分片先统一上传（崩溃/重启补传）。
       - cur 为空 → 设为 max_date、下载 input、归零断点。
       - cur < max_date（扫到更新）→ 统一上传 cur → 切 max_date。
    3. _process_current：逐行处理 cur（按搜索量降序），每词对每个 recomType 各调一次，
       结果即时 append 到 output/<recomType>/ 下临时分片（每 SHARD_SIZE 次调用一片）。
       每 INPUT_SCAN_INTERVAL 秒重扫：出现更新 → 统一上传 cur → break 回主循环切新日期
       （cur 剩余未跑词丢弃）。cur 跑完 → 不上传，空转等被更新 input 取代。

容错:
    - 429：interval 翻倍至 BACKOFF_CAP，连续成功 CLEAN_STREAK 缩回基线。
    - 401/403：连续 3 次 → 登录态失效，停 client 退避重试。
    - 单行某 recomType 连续失败达 MAX_RETRY_PER_ROW：写空占位行、前进。
    - 崩溃/重启：state.json 记 cur + processed_index + 各类型分片进度；未上传分片留本地，
      下次启动由 _ensure_current 的补传逻辑处理。合并上传幂等（分片传成即删，台账判重）。
"""
import glob
import os
import threading
import time
import traceback

from wb_querywords import config, csv_io, oss_uploader, state
from wb_querywords.api_client import RecoClient


class DailyWorker:
    def __init__(self, store_id=None, resume=False, progress=None, one_shot=False):
        self.store_id = store_id or config.STORE_ID
        self.resume = resume
        self.one_shot = one_shot          # True：跑完当天 input 或命中日配额即退出（供计划任务链批处理调用）
        self.progress = progress if progress is not None else {"snap": {}}
        self._stop_event = threading.Event()
        self._writers = {}
        self._last_scan = 0.0
        self._quota_exhausted = False      # 命中 limitExceeded（日配额用尽）置真，run 循环据此退出
        self._pass_done = False            # one_shot 下当天 input 全部处理完置真

    def stop(self):
        self._stop_event.set()

    # ------------------------------------------------------------ 主循环
    def run(self):
        state._set_snap(self.progress, status="starting", input_date=None,
                        processed_index=0, error=None, last_error=None)
        bucket = None
        err_streak = 0
        while not self._stop_event.is_set():
            try:
                if bucket is None:
                    bucket = oss_uploader.get_bucket()
                    print("[OSS] bucket 就绪")
                err_streak = 0
                self._ensure_current(bucket)
                st_now = self.progress["snap"].get("status")
                if st_now == "waiting_input":
                    if self.one_shot:
                        print("[BATCH] 无可用 input，退出")
                        break
                    continue
                if st_now == "error":
                    continue
                self._process_current(bucket)
                if self._quota_exhausted:
                    print("[QUOTA] 命中每日对比推荐配额上限，本轮收尾退出（未跑完的词留待明日重置后补）")
                    break
                if self.one_shot and self._pass_done:
                    print("[BATCH] 当天 input 已全部处理完，退出")
                    break
            except Exception as exc:
                err_streak += 1
                backoff = min(600, 15 * err_streak)
                tb = traceback.format_exc().replace("\n", " ")[:300]
                msg = f"{type(exc).__name__}: {str(exc)[:150]}"
                print(f"[ERROR] 主循环异常（连续 {err_streak} 次）: {msg} | {tb[:200]}")
                print(f"[ERROR] {backoff}s 后重试")
                state._set_snap(self.progress, status="error", last_error=msg)
                self._sleep(backoff)
        self._finish("stopped by user")

    # ------------------------------------------------------------ input 选择 / 切换 / 补传
    def _ensure_current(self, bucket):
        st = state.load_state()
        cur = st.get("input_date")
        max_date = oss_uploader.max_input_date(bucket)
        self._last_scan = time.time()
        if not max_date:
            print("[INPUT] OSS input 暂无日期文件，60s 后重扫")
            state._set_snap(self.progress, status="waiting_input", max_input_date=None)
            self._sleep(60)
            return

        # 崩溃/重启补传：本地不属于 cur 的旧分片按 (类型,日期) 统一上传
        self._backfill(bucket, cur)

        if cur is None:
            print(f"[INPUT] 选定 input 日期 {max_date}")
            state.reset_state_for_new_input(max_date)
            self._download(bucket, max_date)
            state._set_snap(self.progress, status="running", input_date=max_date,
                            max_input_date=max_date, processed_index=0)
            return

        if cur < max_date:
            print(f"[SWITCH] input 更新 {cur} -> {max_date}，先统一上传 {cur}")
            self._close_writers()
            self._finalize_date(bucket, cur)
            state.reset_state_for_new_input(max_date)
            self._download(bucket, max_date)
            state._set_snap(self.progress, status="running", input_date=max_date,
                            max_input_date=max_date, processed_index=0)
            return

        # cur == max_date
        self._download(bucket, cur, only_if_missing=True)
        state._set_snap(self.progress, status="running", input_date=cur, max_input_date=max_date)

    def _download(self, bucket, date_str, only_if_missing=False):
        local = config.local_input_path(date_str)
        if only_if_missing and os.path.exists(local):
            return local
        p = oss_uploader.download_input(bucket, date_str)
        print(f"[INPUT] 下载 {date_str}.csv -> {p}")
        return p

    def _backfill(self, bucket, cur):
        """把本地 output 里不属于 cur 的分片按 (类型,日期) 统一上传（旧日期收尾/崩溃补传）。"""
        seen = set()
        for sp in glob.glob(os.path.join(config.OUTPUT_DIR, "*", "*_shard_*.csv")):
            date = os.path.basename(sp)[:8]
            recom = os.path.basename(os.path.dirname(sp))
            if date == cur or (recom, date) in seen:
                continue
            seen.add((recom, date))
            self._upload_recom_date(bucket, recom, date)

    # ------------------------------------------------------------ 统一上传（合并 + 传 + 清分片）
    def _finalize_date(self, bucket, date):
        for recom in config.RECOM_TYPES:
            self._upload_recom_date(bucket, recom, date)

    def _upload_recom_date(self, bucket, recom, date):
        merged = csv_io.merge_shards(recom, date)
        if not merged:
            return
        ok, info = oss_uploader.upload_merged(bucket, recom, date, merged)
        if ok:
            print(f"[UPLOAD] {recom}/{date}.csv -> {info}")
            self._delete_shards(recom, date)
        else:
            print(f"[UPLOAD][X] {recom}/{date}.csv 失败: {info}（保留分片下次重试）")

    def _delete_shards(self, recom, date):
        for sp in glob.glob(csv_io.shard_glob(recom, date)):
            try:
                os.remove(sp)
            except OSError:
                pass

    # ------------------------------------------------------------ 处理当前日期
    def _process_current(self, bucket):
        st = state.load_state()
        cur = st.get("input_date")
        if not cur:
            return
        local = self._download(bucket, cur, only_if_missing=True)
        pairs = csv_io.read_pairs(local)
        total = len(pairs)
        if total == 0:
            print(f"[INPUT] {cur}.csv 无有效行，60s 后重扫")
            state._set_snap(self.progress, status="empty_input", input_rows=0)
            self._sleep(60)
            return

        processed = int(st.get("processed_index", 0))
        recom_idx = int(st.get("current_recom_index", 0))
        shard_state = st.get("shards", {}) or {}
        state._set_snap(self.progress, status="running", input_date=cur,
                        input_rows=total, processed_index=processed)
        print(f"[ROUND] input={cur} 行数={total} 起点 processed_index={processed} "
              f"recom_types={config.RECOM_TYPES}")

        writers = {r: csv_io.ShardWriter(r, cur) for r in config.RECOM_TYPES}
        self._writers = writers
        calls = {r: int(shard_state.get(r, {}).get("calls_in_shard", 0)) for r in config.RECOM_TYPES}
        shard_index = {r: int(shard_state.get(r, {}).get("shard_index", 1)) for r in config.RECOM_TYPES}
        for r in config.RECOM_TYPES:
            writers[r].ensure_open(shard_index[r])

        def snapshot():
            return {r: {"shard_index": shard_index[r], "calls_in_shard": calls[r]}
                    for r in config.RECOM_TYPES}

        client = RecoClient(store_id=self.store_id)
        try:
            client.start()
        except Exception as exc:
            self._close_writers()
            msg = f"{type(exc).__name__}: {str(exc)[:150]}"
            print(f"[CLIENT][X] 启动失败: {msg}")
            state._set_snap(self.progress, status="client_error", last_error=msg)
            state.save_state(cur, processed, recom_idx, snapshot())
            self._sleep(60)
            return

        interval = config.CALL_INTERVAL
        clean_streak = 0
        auth_fail = 0
        row_retry = {}
        switched = False

        try:
            while not self._stop_event.is_set():
                # 每小时重扫：出现更新 input → 统一上传 cur，break 回主循环切新
                if self._rescan_due(bucket, cur):
                    switched = True
                    break

                if processed >= total:
                    # 当前 input 跑完
                    if self.one_shot:
                        self._pass_done = True
                        return
                    # 常驻模式：不上传，等被更新 input 取代
                    state._set_snap(self.progress, status="idle_done", processed_index=processed)
                    self._sleep(20)
                    continue

                search_text = pairs[processed][0]
                recom = config.RECOM_TYPES[recom_idx]
                status, recs, raw, error_text = client.query(search_text, recom)

                if 200 <= status < 300:
                    auth_fail = 0
                    if error_text and "limitExceeded" in error_text:
                        # 每日对比推荐配额已用尽：不写占位、不前进（词留待明日重置后补），存断点后收尾退出
                        print(f"[QUOTA] limitExceeded 命中 '{search_text[:40]}'（{recom}）：当天配额用尽，存断点退出")
                        state.save_state(cur, processed, recom_idx, snapshot())
                        state._set_snap(self.progress, status="quota_exhausted",
                                        last_error="content.analytics.limitExceeded",
                                        processed_index=processed, total_rows=total)
                        self._quota_exhausted = True
                        return
                    if error_text:
                        # 其它业务错误（非配额）：按单行重试上限处理，超限写空占位前进
                        key = (processed, recom_idx)
                        n = row_retry.get(key, 0) + 1
                        row_retry[key] = n
                        clean_streak = 0
                        if n >= config.MAX_RETRY_PER_ROW:
                            print(f"[SKIP] ({processed},{recom}) '{search_text[:40]}' 业务错误 "
                                  f"errorText={error_text[:60]} 连续 {n} 次，写空占位并前进")
                            self._record(recom, search_text, [], writers, shard_index, calls)
                            row_retry.pop(key, None)
                            recom_idx += 1
                            if recom_idx >= len(config.RECOM_TYPES):
                                processed += 1
                                recom_idx = 0
                            state.save_state(cur, processed, recom_idx, snapshot())
                        else:
                            print(f"[WARN] ({processed},{recom}) 业务错误 errorText={error_text[:60]} "
                                  f"retry={n}/{config.MAX_RETRY_PER_ROW}")
                    else:
                        # 真成功（含 data=[] 真空结果，正常写占位并前进）
                        clean_streak += 1
                        if clean_streak >= config.CLEAN_STREAK and interval > config.CALL_INTERVAL:
                            interval = config.CALL_INTERVAL
                            print(f"[RATE] 连续成功满 {config.CLEAN_STREAK}，interval 缩回 {interval}s")
                        self._record(recom, search_text, recs or [], writers, shard_index, calls)
                        row_retry.pop((processed, recom_idx), None)
                        recom_idx += 1
                        if recom_idx >= len(config.RECOM_TYPES):
                            processed += 1
                            recom_idx = 0
                        state.save_state(cur, processed, recom_idx, snapshot())
                        state._set_snap(self.progress, status="running", processed_index=processed,
                                        total_rows=total, current_recom_index=recom_idx, interval=interval)

                elif status in (401, 403):
                    auth_fail += 1
                    print(f"[AUTH] HTTP {status}（连续 {auth_fail}）登录态可能失效")
                    if auth_fail >= 3:
                        print("[AUTH] 连续 3 次，停止 client 等待退避重试")
                        state.save_state(cur, processed, recom_idx, snapshot())
                        state._set_snap(self.progress, status="auth_failed",
                                        last_error=f"HTTP {status} 连续 {auth_fail} 次")
                        self._sleep(300)
                        return
                elif status == 429:
                    interval = min(interval * 2, config.BACKOFF_CAP)
                    clean_streak = 0
                    print(f"[429] 限流，interval 升到 {interval}s")
                else:
                    key = (processed, recom_idx)
                    n = row_retry.get(key, 0) + 1
                    row_retry[key] = n
                    clean_streak = 0
                    if n >= config.MAX_RETRY_PER_ROW:
                        print(f"[SKIP] ({processed},{recom}) '{search_text[:40]}' "
                              f"连续失败 {n} 次 status={status}，写空占位并前进")
                        self._record(recom, search_text, [], writers, shard_index, calls)
                        row_retry.pop(key, None)
                        recom_idx += 1
                        if recom_idx >= len(config.RECOM_TYPES):
                            processed += 1
                            recom_idx = 0
                        state.save_state(cur, processed, recom_idx, snapshot())
                    else:
                        print(f"[WARN] ({processed},{recom}) status={status} "
                              f"retry={n}/{config.MAX_RETRY_PER_ROW} raw={(raw or '')[:120]}")

                self._sleep(interval)
        finally:
            self._close_writers()
            client.stop()
            state.save_state(cur, processed, recom_idx, snapshot())
            state._set_snap(self.progress, processed_index=processed, total_rows=total)
        if switched:
            print(f"[SWITCH] 已切到新 input，回主循环重载")

    # ------------------------------------------------------------ 分片记录
    def _record(self, recom, search_text, recs, writers, shard_index, calls):
        """写一条结果到当前分片；每 SHARD_SIZE 次调用本地滚到下一片（仅本地，不即时上传）。"""
        w = writers[recom]
        si = shard_index[recom]
        w.append(si, search_text, recs or [])
        calls[recom] += 1
        if calls[recom] >= config.SHARD_SIZE:
            shard_index[recom] = si + 1
            calls[recom] = 0
            w.ensure_open(shard_index[recom])

    def _close_writers(self):
        for w in self._writers.values():
            w.close()
        self._writers = {}

    # ------------------------------------------------------------ 每小时重扫
    def _rescan_due(self, bucket, cur):
        """到点则重扫 input；若发现更新，统一上传 cur 并返回 True（调用方 break 回主循环）。"""
        now = time.time()
        if now - self._last_scan < config.INPUT_SCAN_INTERVAL:
            return False
        self._last_scan = now
        max_date = oss_uploader.max_input_date(bucket)
        if max_date and max_date > cur:
            print(f"[SWITCH] 扫描到更新 input {cur} -> {max_date}，统一上传 {cur}")
            self._finalize_date(bucket, cur)
            state.reset_state_for_new_input(max_date)
            self._download(bucket, max_date)
            state._set_snap(self.progress, status="switched", input_date=max_date)
            return True
        return False

    # ------------------------------------------------------------ 睡眠（可被 stop 打断）
    def _sleep(self, seconds):
        """分段睡眠 seconds 秒，其间若收到 stop_event 立即返回，保证优雅停止不卡长睡。"""
        if seconds <= 0:
            return
        end = time.time() + seconds
        while not self._stop_event.is_set():
            remain = end - time.time()
            if remain <= 0:
                break
            time.sleep(min(1.0, remain))

    # ------------------------------------------------------------ 收尾
    def _finish(self, reason):
        self._close_writers()
        st = state.load_state()
        state._set_snap(self.progress, status="stopped", reason=reason,
                        processed_index=st.get("processed_index"))
        print(f"[FINISH] {reason}")
