# -*- coding: utf-8 -*-
"""按 nmID 集合抓取 WB 商品数据（核心逻辑）。

流程:
    1. 从 OSS content-opt-pool/<taskId>/<sellerId>_{treatment_NNN|control}.csv 下载 nmID 列表
    2. 启动 Chrome（复用该店 profile）→ 选店 → 捕获首条 tableListv6 请求
    3. 对每个 nmID，把捕获请求体的 filter.search 替换为该 nmID 后重放
    4. 命中(有 cards)加入 buffer，未命中直接丢弃
    5. 每 SHARD_SIZE 次查询写一个分片，异步转 CSV 上传 OSS
    6. state_<tag>.json 记录 processed_index，崩溃/停止后 --resume 从断点继续

与旧模块 fetch_all 的区别:
    - 旧模块翻页拉全量（cursor 推进）；本模块按 nmID 单条查询（filter.search）
    - 旧模块 SHARD_SIZE=100 / CALL_INTERVAL=1.2；本模块 2000 / 1.5（限流 60s40 次）
    - 本模块数据目录 nmid_data/<store>/<taskId>/（扁平，无分类层），与旧模块隔离

变体规则（与 oss_input.plan_round 一致）:
    - treatment 只跑编号 001，每轮重复；control 每卖家只跑最新的一份，跑完记台账不再重复
    - 同 taskId 下 treatment / control 共用一个目录，靠文件名区分互不覆盖：
      输入 CSV 保留 OSS 原名、分片名带变体标记、断点/汇总带变体后缀
      （state_<tag>.json / summary_<tag>.json，tag=treatment|control）
"""
import argparse
import json
import os
import sys
import threading
import time

import config
from script import PAGE_URL, create_driver
from wb_to_oss import convert_and_upload_shard, oss_target

# 复用旧模块的通用能力（选店/捕获/调接口/清理/锁），不复制代码
from fetch_all import (
    RATE_LIMIT_PAUSE, SERVER_FAIL_LIMIT, CAPTURE_TIMEOUT,
    acquire_single_instance, release_single_instance,
    pre_launch_cleanup, wait_first_request, build_fetch_headers, call_api,
    ensure_selected_store, verify_selected_store,
)
from nmid_fetch import oss_input

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# 本模块专用常量（限流 60 秒 40 次 → 请求发起间隔下限 1.6 秒，对红线留余量）
CALL_INTERVAL = 1.6     # 间隔下限 floor：平稳时按此跑，最快
SHARD_SIZE = 2000       # 每 2000 次查询写一个分片

# 闭环退避：撞 429 → base ×BACKOFF_UP（封顶 BACKOFF_CAP）；连续 BACKOFF_CLEAN_STREAK
# 条干净 → base ×BACKOFF_DOWN（回落到 CALL_INTERVAL 为止）。退避快、回收慢。
BACKOFF_CAP = 2.4           # 间隔上限 cap：硬限流时最慢跑这个
BACKOFF_UP = 1.5            # 撞 429 的放大系数（1.6×1.5=2.4，一次即到顶）
BACKOFF_DOWN = 0.95         # 每满一个干净窗口的回收系数
BACKOFF_CLEAN_STREAK = 50   # 触发一次回收所需的连续干净条数

# 数据根目录：nmid_data/<store_id>/<task_id>/（扁平，无分类层）
#   treatment_001 每轮全量重跑；control 每卖家只抓最新一份、台账去重。
#   两类落在同一 taskId 目录下，靠文件名区分：输入用 OSS 原名、分片带变体标记、
#   断点/汇总带变体后缀（state_<tag>.json / summary_<tag>.json），互不覆盖。
DATA_ROOT = os.path.join(config.BASE_DIR, "nmid_data")

# control 台账：记住每个店已跑完的 control 输入文件（每店多条），跑过的不再下载不再跑
CONTROL_LEDGER = os.path.join(DATA_ROOT, "control_done.json")


def _store_dir(store_id):
    return os.path.join(DATA_ROOT, store_id)


def _lock_file(store_id):
    # 锁文件写在店目录顶层；先确保目录存在，否则 acquire_single_instance 写入会因
    # 父目录缺失而失败（锁静默失效，无法拦新旧模块抢同一店 profile）。
    d = _store_dir(store_id)
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "fetch_by_nmid.lock")


def _run_dir(store_id, task_id, variant=None, index=None):
    """一次抓取的工作目录。

    treatment/legacy：nmid_data/<store>/<task_id>/（位置不变，每轮重跑的输入落这里）；
    control：单独存放 nmid_data/<store>/control/<task_id>/，与每轮重跑的 treatment
    输入隔离，跑过一次的靠台账跳过、不再下载。目录内产物靠文件名区分：输入 CSV 保留
    OSS 原名、分片名带变体标记、断点/汇总带变体后缀（state_<tag>.json /
    summary_<tag>.json），互不覆盖。
    """
    if variant == "control":
        return os.path.join(_store_dir(store_id), "control", task_id)
    return os.path.join(_store_dir(store_id), task_id)


def store_id_for_seller(seller_id):
    """CSV 文件名（WB 数字卖家 ID）→ config 店铺 id（store1/store2）；找不到返回 None。"""
    for s in config.STORES:
        if config.get_seller_id(s["id"]) == str(seller_id):
            return s["id"]
    return None


# ---------------------------------------------------------------- 断点状态
def _state_path(run_dir, tag=None):
    """断点文件路径：扁平布局下同 taskId 的 treatment/control 共用目录，故按变体
    区分文件名（state_treatment.json / state_control.json），tag 缺省退回 state.json。"""
    return os.path.join(run_dir, f"state_{tag}.json" if tag else "state.json")


def _summary_path(run_dir, tag=None):
    """汇总文件路径：同 state，按变体区分避免 treatment/control 互相覆盖。"""
    return os.path.join(run_dir, f"summary_{tag}.json" if tag else "summary.json")


def save_state(run_dir, run_ts, shard_index, query_count, matched_count,
               processed_index, finished=False, tag=None):
    state = {
        "run_ts": run_ts,
        "shard_index": shard_index,
        "query_count": query_count,
        "matched_count": matched_count,
        "processed_index": processed_index,   # 已处理的 nmID 个数（下次从此继续）
        "finished": finished,
    }
    os.makedirs(run_dir, exist_ok=True)
    with open(_state_path(run_dir, tag), "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_state(run_dir, tag=None):
    p = _state_path(run_dir, tag)
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def reset_state_for_new_round(run_dir, tag=None):
    """一轮跑完后重置断点，让下一轮从头查询（每次全量处理）。"""
    st = load_state(run_dir, tag)
    if st is None:
        return
    st["finished"] = False
    st["processed_index"] = 0
    st["query_count"] = 0
    st["matched_count"] = 0
    st["shard_index"] = 1
    with open(_state_path(run_dir, tag), "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------- control 台账
def load_control_ledger():
    """读 control 台账：{store_id: {oss_key: {key, name, last_modified, ...}}}。

    兼容旧版单条台账（{store_id: {key, name, ...}}，每店只记最后跑的一份）：
    读到旧结构时自动转成多条结构，已跑过的那份不会重跑。
    """
    if not os.path.exists(CONTROL_LEDGER):
        return {}
    try:
        with open(CONTROL_LEDGER, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict):
        return {}
    ledger = {}
    for sid, v in data.items():
        if not isinstance(v, dict):
            continue
        if v.get("key") and v.get("finished_at"):
            ledger[sid] = {v["key"]: v}          # 旧版：每店单条
        else:
            ledger[sid] = {k: e for k, e in v.items() if isinstance(e, dict)}
    return ledger


def save_control_ledger_entry(store_id, row, extra=None):
    """control 跑完后按 OSS key 记一条；先写临时文件再 replace，避免半截 JSON。"""
    ledger = load_control_ledger()
    entry = {
        "key": row.get("key"),
        "name": row.get("name"),
        "task_id": row.get("task_id"),
        "seller_id": row.get("seller_id"),
        "last_modified": row.get("last_modified"),
        "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    if extra:
        entry.update(extra)
    ledger.setdefault(store_id, {})[row.get("key")] = entry
    os.makedirs(os.path.dirname(CONTROL_LEDGER), exist_ok=True)
    tmp = CONTROL_LEDGER + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ledger, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CONTROL_LEDGER)
    return entry


def control_already_done(store_id, row):
    """这份 control 是否已跑过（台账里同 OSS key 有条目且 mtime 一致即视为跑过）。

    OSS 上覆盖同名文件后 key 不变但 mtime 变：视为新的一份，会再跑一次，
    跑完台账刷新该 key 条目的 mtime。
    """
    entry = load_control_ledger().get(store_id, {}).get(row.get("key"))
    if not entry:
        return False
    return entry.get("last_modified") == row.get("last_modified")


def mark_control_done(store_id, row, result):
    """control 跑完（finished）后写台账。"""
    return save_control_ledger_entry(store_id, row, extra={
        "query_count": result.get("query_count", 0),
        "matched_count": result.get("matched_count", 0),
        "run_dir": result.get("out_dir"),
    })


# ---------------------------------------------------------------- 分片与上传
def flush_shard(buffer, shard_index, run_ts, task_id, seller_id, out_dir,
                variant=None, index=None):
    """写分片文件，文件名 {taskId}_{sellerId}[_{variant}]_shard_NNN_{run_ts}.json。

    变体标记带全：treatment 带完整编号（_treatment_001，与输入文件名对齐）、control
    带 _control，OSS 上靠文件名即可区分 001 与 control；上传路径不变。legacy/未指定
    变体不带标记。
    """
    tag = oss_input.variant_tag(variant, index) if variant else None
    if variant == "treatment":
        mid = f"_treatment_{index:03d}" if index is not None else "_treatment"
    elif tag:
        mid = f"_{tag}"
    else:
        mid = ""
    name = f"{task_id}_{seller_id}{mid}_shard_{shard_index:03d}_{run_ts}.json"
    path = os.path.join(out_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(buffer, f, ensure_ascii=False)
    print(f"[SHARD] 第 {shard_index} 个分片已写入: {path}（{len(buffer)} 条响应）")
    return shard_index + 1, path


def _shard_worker(run_dir, path, bucket, prefix):
    """异步线程体：转换分片并上传；成功删除本地，失败保留待补传。"""
    try:
        err = convert_and_upload_shard(run_dir, path, bucket, prefix)
        if err:
            print(f"[OSS-ASYNC] {err}")
            return
        if bucket is None or prefix is None:
            return
        csv_path = os.path.join(
            run_dir, "csv", os.path.splitext(os.path.basename(path))[0] + ".csv")
        for f in (path, csv_path):
            try:
                os.remove(f)
            except OSError as e:
                print(f"[OSS-ASYNC] 删除失败 {os.path.basename(f)}: {e}")
        print(f"[OSS-ASYNC] 已上传并清理本地: {os.path.basename(path)}")
    except Exception as e:
        print(f"[OSS-ASYNC] 线程异常: {type(e).__name__}: {e}")


# ---------------------------------------------------------------- 主流程
def run_nmid_fetch(store, task_id, seller_id, nmids, resume=True,
                   stop_event=None, progress=None, variant=None, index=None):
    """对单个 (store, task_id, seller_id, nmids) 执行按 nmID 查询。

    store: 已解析店铺 dict；nmids: nmID 字符串列表。
    variant/index: 输入文件变体（treatment/control 与编号）。用于推导分片名与断点/
                   汇总文件的变体后缀（tag），不再决定目录层级（布局扁平）。
    返回 dict: {finished, query_count, matched_count, out_dir, variant, error}
    """
    store_id = store["id"]
    tag = oss_input.variant_tag(variant, index) if variant else None
    run_dir = _run_dir(store_id, task_id, variant, index)
    os.makedirs(run_dir, exist_ok=True)
    result = {"finished": False, "query_count": 0, "matched_count": 0,
              "out_dir": run_dir, "variant": variant, "error": None}

    def _prog(**kw):
        if progress is None:
            return
        prev = progress.get("snap") or {}
        merged = dict(prev)
        merged.update(kw)
        merged["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        progress["snap"] = merged

    def _stopped():
        return stop_event is not None and stop_event.is_set()

    # 断点续传
    run_ts = time.strftime("%Y%m%d_%H%M%S")
    processed_index = 0
    shard_index = 1
    query_count = 0
    matched_count = 0
    st = load_state(run_dir, tag) if resume else None
    if st and not st.get("finished"):
        run_ts = st.get("run_ts", run_ts)
        processed_index = st.get("processed_index", 0)
        shard_index = st.get("shard_index", 1)
        query_count = st.get("query_count", 0)
        matched_count = st.get("matched_count", 0)
        print(f"[RESUME] 从断点继续: processed_index={processed_index}, "
              f"shard={shard_index}, query={query_count}")
    elif st and st.get("finished"):
        print("[RESUME] 该任务已完成，跳过")
        result["finished"] = True
        return result

    # OSS 上传目标
    bucket, prefix, oss_err = oss_target(oss_segment=store["oss_segment"])
    if oss_err:
        print(f"[OSS] 上传不可用，仅本地落盘: {oss_err}")
    upload_threads = []

    def on_shard_written(shard_path):
        t = threading.Thread(target=_shard_worker,
                             args=(run_dir, shard_path, bucket, prefix), daemon=True)
        upload_threads.append(t)
        t.start()

    # 启动浏览器
    pre_launch_cleanup(store["profile_dir"])
    _prog(status="launching_browser", store_id=store_id, task_id=task_id)
    driver = create_driver(headless=False, profile_dir=store["profile_dir"])
    driver.set_script_timeout(120)
    driver.get(PAGE_URL)

    try:
        # 自动选店
        expected_id = config.get_seller_id(store_id)
        switched = False
        if expected_id is not None:
            status, cur_id, rows = ensure_selected_store(driver, expected_id)
            if status == "switched":
                switched = True
                time.sleep(3)
                try:
                    driver.get_log("performance")
                except Exception:
                    pass
                driver.get(PAGE_URL)
            elif status != "already":
                result["error"] = f"store_select_{status}"
                return result
        else:
            print(f"[{store_id}] config.json 未配置卖家 ID，跳过自动选店")

        # 捕获首条请求
        first = wait_first_request(driver, CAPTURE_TIMEOUT)
        if first is None:
            result["error"] = "capture_timeout"
            return result
        headers = build_fetch_headers(first["headers"])

        # 切换后复核
        if expected_id is not None and switched:
            selected_id, _ = verify_selected_store(driver)
            if selected_id != expected_id:
                result["error"] = "store_mismatch"
                return result

        # 请求体模板
        body_template = None
        if first["postData"]:
            try:
                body_template = json.loads(first["postData"])
            except json.JSONDecodeError:
                body_template = None

        buffer = []
        consecutive_fail = 0
        server_fail = 0
        base_interval = CALL_INTERVAL   # 闭环退避动态间隔，起始 = floor
        clean_streak = 0                # 连续干净（非 429）计数，用于回收 base
        _prog(status="running", task_id=task_id, processed_index=processed_index,
              total=len(nmids), last_error=None)

        idx = processed_index
        while idx < len(nmids):
            if _stopped():
                print("[STOP] 收到停止信号，保存断点后优雅退出...")
                break
            nmid = nmids[idx]
            # 构造请求体：替换 filter.search
            if isinstance(body_template, dict):
                body = json.loads(json.dumps(body_template))  # 深拷贝
                filt = body.setdefault("filter", {})
                if isinstance(filt, dict):
                    filt["search"] = nmid
            else:
                body = {"sort": [{"columnID": 11, "order": "desc"}],
                        "filter": {"search": nmid, "paidOptions": {}},
                        "cursor": {"n": 20}}
            post_data = json.dumps(body, ensure_ascii=False)

            query_count += 1
            t_req = time.perf_counter()
            try:
                status, text = call_api(driver, first["url"], first["method"], headers, post_data)
            except Exception as e:
                consecutive_fail += 1
                if consecutive_fail >= 3:
                    print(f"[ERROR] 连续 3 次异常，保存断点退出: {e}")
                    break
                time.sleep(10)
                query_count -= 1
                continue
            dt_req = time.perf_counter() - t_req
            if status == 429:
                base_interval = min(base_interval * BACKOFF_UP, BACKOFF_CAP)
                clean_streak = 0
                print(f"[RATE-LIMIT] 429 限流，间隔退避→{base_interval:.2f}s，暂停 {RATE_LIMIT_PAUSE} 秒重试...")
                time.sleep(RATE_LIMIT_PAUSE)
                query_count -= 1
                continue
            if status != 200:
                if 500 <= status < 600:
                    server_fail += 1
                    if server_fail >= SERVER_FAIL_LIMIT:
                        print(f"[ERROR] 服务端连续错误 {SERVER_FAIL_LIMIT} 次，保存断点退出")
                        break
                    pause = min(30 * 2 ** (server_fail - 1), 300)
                    time.sleep(pause)
                    continue
                consecutive_fail += 1
                if consecutive_fail >= 5:
                    print("[ERROR] 连续失败 5 次，停止")
                    break
                time.sleep(5)
                continue
            consecutive_fail = 0
            server_fail = 0
            # 闭环退避回收：每满 BACKOFF_CLEAN_STREAK 条干净就把 base 往下收一点（到 floor 为止）
            clean_streak += 1
            if clean_streak >= BACKOFF_CLEAN_STREAK:
                clean_streak = 0
                if base_interval > CALL_INTERVAL:
                    base_interval = max(base_interval * BACKOFF_DOWN, CALL_INTERVAL)

            try:
                resp = json.loads(text)
            except json.JSONDecodeError:
                print(f"[WARN] 返回非 JSON，跳过该 nmID: {text[:120]}")
                idx += 1
                processed_index = idx
                continue

            cards = (resp.get("data") or {}).get("cards") or []
            if cards:
                matched_count += 1
                buffer.append({"data": {"cards": cards}})
            # 未命中：直接丢弃，不加入 buffer

            idx += 1
            processed_index = idx
            if idx % 50 == 0 or idx == len(nmids):
                print(f"[PROGRESS] {idx}/{len(nmids)} 已查询, 命中 {matched_count}, 间隔 {base_interval:.2f}s")
                _prog(status="running", processed_index=idx, total=len(nmids),
                      matched=matched_count, interval=round(base_interval, 2), last_error=None)

            # 满 SHARD_SIZE 写分片
            if len(buffer) >= SHARD_SIZE:
                shard_index, spath = flush_shard(
                    buffer, shard_index, run_ts, task_id, seller_id, run_dir,
                    variant, index)
                buffer = []
                on_shard_written(spath)
                save_state(run_dir, run_ts, shard_index, query_count,
                           matched_count, processed_index, tag=tag)

            # 自适应睡眠：把“请求发起间隔”钉在 base_interval（闭环退避动态值，floor=CALL_INTERVAL）
            # ——慢请求少睡/不睡，快请求补足余量；撞 429 时 base 抬高把 surge 摁住
            time.sleep(max(0.0, base_interval - dt_req))

        finished = idx >= len(nmids) and not _stopped()
        result["finished"] = finished
        result["query_count"] = query_count
        result["matched_count"] = matched_count

    finally:
        # 剩余 buffer 写入分片（即使不满 SHARD_SIZE）
        if buffer:
            shard_index, spath = flush_shard(
                buffer, shard_index, run_ts, task_id, seller_id, run_dir,
                variant, index)
            on_shard_written(spath)
        save_state(run_dir, run_ts, shard_index, query_count, matched_count,
                   processed_index, finished=result["finished"], tag=tag)
        # 写汇总
        summary = {
            "task_id": task_id, "seller_id": seller_id, "store_id": store_id,
            "variant": variant, "index": index,
            "run_ts": run_ts, "total_nmids": len(nmids),
            "query_count": query_count, "matched_count": matched_count,
            "missed_count": query_count - matched_count,
            "finished": result["finished"],
        }
        with open(_summary_path(run_dir, tag), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        # 等待上传线程完成，避免暂停丢失
        for t in upload_threads:
            t.join(timeout=30)
        try:
            driver.quit()
        except BaseException:
            pass
        _prog(status="finished" if result["finished"] else "stopped",
              processed_index=processed_index, matched=matched_count)
    return result


def download_input(store, task_id, seller_id, variant=None, index=None, oss_key=None, row=None):
    """下载输入 CSV 到工作目录（control 落 control/ 子目录），保留 OSS 原文件名同名覆盖。

    返回 (local_path, row, None)；失败返回 (None, row, 错误码)。
    oss_key 缺省时按 (卖家, 变体) 在该 taskId 下自己找；旧命名兜底仅在未指定变体时。
    """
    store_id = store["id"]
    tag = oss_input.variant_tag(variant, index) if variant else "input"
    run_dir = _run_dir(store_id, task_id, variant, index)
    target_key = oss_key
    if target_key is None:
        rows = oss_input.list_input_csvs(task_id)
        for r in rows:
            if r["seller_id"] != str(seller_id):
                continue
            if variant is None or (r["variant"] == variant and
                                   (index is None or r["index"] == index)):
                target_key, row = r["key"], row or r
                break
        if target_key is None and variant is None:
            for r in rows:
                if r["seller_id"] == str(seller_id) and r["variant"] == "legacy":
                    target_key, row = r["key"], r
                    break
    if target_key is None:
        print(f"[ERROR] taskId={task_id} 下找不到 sellerId={seller_id} 的 {tag} CSV")
        return None, row, "csv_not_found"
    os.makedirs(run_dir, exist_ok=True)
    fname = (row or {}).get("name") or os.path.basename(target_key) \
        or f"{seller_id}_{tag}.csv"
    local_csv = os.path.join(run_dir, fname)
    ok, err = oss_input.download_csv(target_key, local_csv)
    if not ok:
        print(f"[ERROR] 下载 CSV 失败: {err}")
        return None, row, f"download_failed: {err}"
    return local_csv, row, None


def process_task(store, task_id, seller_id, resume=True, stop_event=None, progress=None,
                 variant=None, index=None, oss_key=None, row=None, local_csv=None):
    """解析 nmID → 执行查询；control 跑完写台账。返回 run_nmid_fetch 的结果。

    local_csv: 调用方已提前下载好的输入路径（宿主轮初批量下载后传入）；缺省时本函数
    自行下载（CLI 单次跑路径）。row: plan_round 给出的输入行，用于 control 台账记录。
    """
    store_id = store["id"]
    tag = oss_input.variant_tag(variant, index) if variant else "input"
    run_dir = _run_dir(store_id, task_id, variant, index)
    if local_csv is None:
        local_csv, row, err = download_input(store, task_id, seller_id, variant=variant,
                                             index=index, oss_key=oss_key, row=row)
        if not local_csv:
            return {"finished": False, "variant": variant, "error": err}
    nmids = oss_input.parse_nmids(local_csv)
    print(f"[INPUT] taskId={task_id} sellerId={seller_id} {tag} 共 {len(nmids)} 个 nmID")
    if not nmids:
        # 空文件也算跑完：control 记账，避免每轮重复下载
        if variant == "control":
            mark_control_done(store_id, row or {"key": target_key, "task_id": task_id,
                                                "seller_id": str(seller_id)},
                              {"query_count": 0, "matched_count": 0, "run_dir": run_dir})
        return {"finished": True, "variant": variant, "out_dir": run_dir,
                "query_count": 0, "matched_count": 0, "error": "empty_csv"}
    result = run_nmid_fetch(store, task_id, seller_id, nmids,
                            resume=resume, stop_event=stop_event, progress=progress,
                            variant=variant, index=index)
    # control 只跑一次：完整跑完才记账，中途中断下次接着跑
    if variant == "control" and result.get("finished"):
        entry = mark_control_done(store_id, row or {"key": target_key, "task_id": task_id,
                                                   "seller_id": str(seller_id)}, result)
        print(f"[CONTROL] 已记入台账，后续只跑比它更新的输入: {entry['key']} "
              f"mtime={entry.get('last_modified')}")
    return result


def main():
    ap = argparse.ArgumentParser(description="按 nmID 集合抓取 WB 商品数据")
    ap.add_argument("--store", default=None, help="店铺 id（store1/store2）；缺省遍历全部")
    ap.add_argument("--task-id", default=None, help="只处理指定 taskId；缺省遍历全部")
    ap.add_argument("--resume", action="store_true", help="从断点续传")
    args = ap.parse_args()

    sellers = [config.get_seller_id(s["id"]) for s in config.STORES]
    sellers = [s for s in sellers if s]
    task_ids = [args.task_id] if args.task_id else None
    # 与常驻宿主走同一套规则：treatment_001 每轮跑 + 每卖家最新的一份 control
    jobs, unknown = oss_input.plan_round(task_ids=task_ids, sellers=sellers)
    if unknown:
        print(f"[WARN] 文件名不符合命名规则，已忽略: {sorted(unknown)}")
    if not jobs:
        print("[ERROR] content-opt-pool 下没有符合规则的输入文件")
        return
    for job in jobs:
        store_id = store_id_for_seller(job["seller_id"])
        if store_id is None:
            print(f"[WARN] sellerId={job['seller_id']} 未在 config.json 配置，跳过")
            continue
        if args.store and store_id != args.store:
            continue
        store = config.get_store(store_id)
        lock = _lock_file(store_id)
        if not acquire_single_instance(lock):
            print(f"[LOCK] {store_id} 已被其他进程占用，跳过")
            continue
        try:
            process_task(store, job["task_id"], job["seller_id"], resume=args.resume,
                         variant=job["variant"], index=job["index"],
                         oss_key=job["key"], row=job)
        finally:
            release_single_instance(lock)


if __name__ == "__main__":
    main()
