# -*- coding: utf-8 -*-
"""按 nmID 集合抓取 WB 商品数据（核心逻辑）。

流程:
    1. 从 OSS content-opt-pool/<taskId>/<sellerId>.csv 下载 nmID 列表
    2. 启动 Chrome（复用该店 profile）→ 选店 → 捕获首条 tableListv6 请求
    3. 对每个 nmID，把捕获请求体的 filter.search 替换为该 nmID 后重放
    4. 命中(有 cards)加入 buffer，未命中直接丢弃
    5. 每 SHARD_SIZE 次查询写一个分片，异步转 CSV 上传 OSS
    6. state.json 记录 processed_index，崩溃/停止后 --resume 从断点继续

与旧模块 fetch_all 的区别:
    - 旧模块翻页拉全量（cursor 推进）；本模块按 nmID 单条查询（filter.search）
    - 旧模块 SHARD_SIZE=100 / CALL_INTERVAL=1.2；本模块 2000 / 1.5（限流 60s40 次）
    - 本模块数据目录 nmid_data/<store>/<taskId>/，与旧模块隔离
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

# 本模块专用常量（限流 60 秒 40 次 → 间隔至少 1.5 秒）
CALL_INTERVAL = 1.5
SHARD_SIZE = 2000       # 每 2000 次查询写一个分片

# 数据根目录：nmid_data/<store_id>/<task_id>/
DATA_ROOT = os.path.join(config.BASE_DIR, "nmid_data")


def _store_dir(store_id):
    return os.path.join(DATA_ROOT, store_id)


def _lock_file(store_id):
    return os.path.join(_store_dir(store_id), "fetch_by_nmid.lock")


def _run_dir(store_id, task_id):
    return os.path.join(_store_dir(store_id), task_id)


def store_id_for_seller(seller_id):
    """CSV 文件名（WB 数字卖家 ID）→ config 店铺 id（store1/store2）；找不到返回 None。"""
    for s in config.STORES:
        if config.get_seller_id(s["id"]) == str(seller_id):
            return s["id"]
    return None


# ---------------------------------------------------------------- 断点状态
def save_state(run_dir, run_ts, shard_index, query_count, matched_count,
               processed_index, finished=False):
    state = {
        "run_ts": run_ts,
        "shard_index": shard_index,
        "query_count": query_count,
        "matched_count": matched_count,
        "processed_index": processed_index,   # 已处理的 nmID 个数（下次从此继续）
        "finished": finished,
    }
    os.makedirs(run_dir, exist_ok=True)
    with open(os.path.join(run_dir, "state.json"), "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_state(run_dir):
    p = os.path.join(run_dir, "state.json")
    if not os.path.exists(p):
        return None
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def reset_state_for_new_round(run_dir):
    """一轮跑完后重置断点，让下一轮从头查询（每次全量处理）。"""
    st = load_state(run_dir)
    if st is None:
        return
    st["finished"] = False
    st["processed_index"] = 0
    st["query_count"] = 0
    st["matched_count"] = 0
    st["shard_index"] = 1
    with open(os.path.join(run_dir, "state.json"), "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------- 分片与上传
def flush_shard(buffer, shard_index, run_ts, task_id, seller_id, out_dir):
    """写分片文件，文件名 {taskId}_{sellerId}_shard_NNN_{run_ts}.json。"""
    name = f"{task_id}_{seller_id}_shard_{shard_index:03d}_{run_ts}.json"
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
                   stop_event=None, progress=None):
    """对单个 (store, task_id, seller_id, nmids) 执行按 nmID 查询。

    store: 已解析店铺 dict；nmids: nmID 字符串列表。
    返回 dict: {finished, query_count, matched_count, out_dir, error}
    """
    store_id = store["id"]
    run_dir = _run_dir(store_id, task_id)
    os.makedirs(run_dir, exist_ok=True)
    result = {"finished": False, "query_count": 0, "matched_count": 0,
              "out_dir": run_dir, "error": None}

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
    st = load_state(run_dir) if resume else None
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
            if status == 429:
                print(f"[RATE-LIMIT] 429 限流，暂停 {RATE_LIMIT_PAUSE} 秒重试...")
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
                print(f"[PROGRESS] {idx}/{len(nmids)} 已查询, 命中 {matched_count}")
                _prog(status="running", processed_index=idx, total=len(nmids),
                      matched=matched_count, last_error=None)

            # 满 SHARD_SIZE 写分片
            if len(buffer) >= SHARD_SIZE:
                shard_index, spath = flush_shard(
                    buffer, shard_index, run_ts, task_id, seller_id, run_dir)
                buffer = []
                on_shard_written(spath)
                save_state(run_dir, run_ts, shard_index, query_count,
                           matched_count, processed_index)

            time.sleep(CALL_INTERVAL)

        finished = idx >= len(nmids) and not _stopped()
        result["finished"] = finished
        result["query_count"] = query_count
        result["matched_count"] = matched_count

    finally:
        # 剩余 buffer 写入分片（即使不满 SHARD_SIZE）
        if buffer:
            shard_index, spath = flush_shard(
                buffer, shard_index, run_ts, task_id, seller_id, run_dir)
            on_shard_written(spath)
        save_state(run_dir, run_ts, shard_index, query_count, matched_count,
                   processed_index, finished=result["finished"])
        # 写汇总
        summary = {
            "task_id": task_id, "seller_id": seller_id, "store_id": store_id,
            "run_ts": run_ts, "total_nmids": len(nmids),
            "query_count": query_count, "matched_count": matched_count,
            "missed_count": query_count - matched_count,
            "finished": result["finished"],
        }
        with open(os.path.join(run_dir, "summary.json"), "w", encoding="utf-8") as f:
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


def process_task(store, task_id, seller_id, resume=True, stop_event=None, progress=None):
    """下载 CSV → 解析 nmID → 执行查询。返回 run_nmid_fetch 的结果。"""
    store_id = store["id"]
    csvs = oss_input.list_store_csvs(task_id)
    target_key = None
    for key, sid in csvs:
        if sid == str(seller_id):
            target_key = key
            break
    if target_key is None:
        print(f"[ERROR] taskId={task_id} 下找不到 sellerId={seller_id} 的 CSV")
        return {"finished": False, "error": "csv_not_found"}
    local_csv = os.path.join(_run_dir(store_id, task_id), f"input_{seller_id}.csv")
    ok, err = oss_input.download_csv(target_key, local_csv)
    if not ok:
        print(f"[ERROR] 下载 CSV 失败: {err}")
        return {"finished": False, "error": f"download_failed: {err}"}
    nmids = oss_input.parse_nmids(local_csv)
    print(f"[INPUT] taskId={task_id} sellerId={seller_id} 共 {len(nmids)} 个 nmID")
    if not nmids:
        return {"finished": True, "error": "empty_csv"}
    return run_nmid_fetch(store, task_id, seller_id, nmids,
                          resume=resume, stop_event=stop_event, progress=progress)


def main():
    ap = argparse.ArgumentParser(description="按 nmID 集合抓取 WB 商品数据")
    ap.add_argument("--store", default=None, help="店铺 id（store1/store2）；缺省遍历全部")
    ap.add_argument("--task-id", default=None, help="只处理指定 taskId；缺省遍历全部")
    ap.add_argument("--resume", action="store_true", help="从断点续传")
    args = ap.parse_args()

    task_ids = [args.task_id] if args.task_id else oss_input.list_task_ids()
    if not task_ids:
        print("[ERROR] content-opt-pool 下无 taskId")
        return
    for tid in task_ids:
        for key, seller_id in oss_input.list_store_csvs(tid):
            store_id = store_id_for_seller(seller_id)
            if store_id is None:
                print(f"[WARN] sellerId={seller_id} 未在 config.json 配置，跳过")
                continue
            if args.store and store_id != args.store:
                continue
            store = config.get_store(store_id)
            lock = _lock_file(store_id)
            if not acquire_single_instance(lock):
                print(f"[LOCK] {store_id} 已被其他进程占用，跳过")
                continue
            try:
                process_task(store, tid, seller_id, resume=args.resume)
            finally:
                release_single_instance(lock)


if __name__ == "__main__":
    main()
