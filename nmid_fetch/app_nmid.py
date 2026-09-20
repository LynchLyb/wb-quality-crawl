# -*- coding: utf-8 -*-
"""nmid_fetch 常驻 Flask 宿主（端口 8081，与旧模块 8080 隔离）。

职责:
    - 后台 worker 线程循环处理 content-opt-pool/ 下所有 taskId 的所有店铺 CSV
    - 一轮跑完立即开始下一轮（不 sleep），每轮重新下载 CSV、从头查询
    - HTTP 接口供协调器/运维控制: /health /status /stop /resume

与旧模块 app.py 的区别:
    - 旧模块每店一个 supervisor + cron 巡检；本模块单 worker 串行遍历所有 (task, store)
    - 本模块不自动重启（由协调器控制 stop/resume），避免与旧模块抢 profile
"""
import threading
import time

from flask import Flask, jsonify, request
from waitress import serve

import config
from nmid_fetch import oss_input
from nmid_fetch.fetch_by_nmid import (
    _lock_file, _run_dir, process_task, reset_state_for_new_round,
    store_id_for_seller,
)
from fetch_all import acquire_single_instance, release_single_instance

HOST = "127.0.0.1"
PORT = 8081
IDLE_SLEEP = 60  # 无数据时的等待间隔（秒）

app = Flask(__name__)

# 全局运行状态
_stop_event = threading.Event()
_progress = {"snap": {}}
_worker_thread = None
_worker_lock = threading.Lock()


def _main_loop():
    """常驻循环：遍历所有 taskId 的所有店铺 CSV，一轮跑完立即重新开始。"""
    while not _stop_event.is_set():
        task_ids = oss_input.list_task_ids()
        if not task_ids:
            _progress["snap"] = {"status": "idle", "reason": "no_pending_tasks",
                                 "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
            # 无数据：等待，不空转
            for _ in range(IDLE_SLEEP):
                if _stop_event.is_set():
                    return
                time.sleep(1)
            continue

        round_done = True
        for tid in task_ids:
            if _stop_event.is_set():
                return
            for _key, seller_id in oss_input.list_store_csvs(tid):
                if _stop_event.is_set():
                    return
                store_id = store_id_for_seller(seller_id)
                if store_id is None:
                    print(f"[WARN] sellerId={seller_id} 未在 config.json 配置，跳过")
                    continue
                store = config.get_store(store_id)
                lock = _lock_file(store_id)
                if not acquire_single_instance(lock):
                    print(f"[LOCK] {store_id} 已被其他进程占用，跳过本轮")
                    round_done = False
                    continue
                try:
                    r = process_task(store, tid, seller_id, resume=True,
                                     stop_event=_stop_event, progress=_progress)
                    if not r.get("finished"):
                        round_done = False
                except Exception as e:
                    print(f"[ERROR] 处理 {tid}/{seller_id} 异常: {type(e).__name__}: {e}")
                    round_done = False
                finally:
                    release_single_instance(lock)

        if round_done and not _stop_event.is_set():
            # 一轮跑完：重置断点，立即开始下一轮（每次全量处理）
            print("[ROUND] 本轮全部完成，重置断点后立即开始下一轮")
            for tid in task_ids:
                for _key, seller_id in oss_input.list_store_csvs(tid):
                    sid = store_id_for_seller(seller_id)
                    if sid:
                        reset_state_for_new_round(_run_dir(sid, tid))
        # round_done=False 时（有任务未完成/被跳过）也立即重试下一轮


def _ensure_worker():
    """确保 worker 线程在跑（/resume 调用）。"""
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return False, "worker 已在运行"
        _stop_event.clear()
        _worker_thread = threading.Thread(target=_main_loop, daemon=True)
        _worker_thread.start()
        return True, "worker 已启动"


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "wb-nmid-fetch-host",
                    "worker_alive": _worker_thread is not None and _worker_thread.is_alive()})


@app.get("/status")
def status():
    snap = dict(_progress.get("snap") or {})
    snap["worker_alive"] = _worker_thread is not None and _worker_thread.is_alive()
    # 待处理任务数（供协调器判断是否需要切换）
    try:
        snap["pending_task_count"] = len(oss_input.list_task_ids())
    except Exception:
        snap["pending_task_count"] = 0
    return jsonify(snap)


@app.post("/stop")
def stop():
    _stop_event.set()
    return jsonify({"ok": True, "message": "已请求停止，worker 将在当前查询周期后退出"})


@app.post("/resume")
def resume():
    ok, msg = _ensure_worker()
    return jsonify({"ok": ok, "message": msg}), (200 if ok else 409)


@app.post("/start")
def start():
    return resume()


def main():
    print(f"[NMID-HOST] waitress 监听 http://{HOST}:{PORT}（/health /status /stop /resume）")
    serve(app, host=HOST, port=PORT, threads=4)


if __name__ == "__main__":
    main()
