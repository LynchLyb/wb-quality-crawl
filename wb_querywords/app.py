# -*- coding: utf-8 -*-
"""wb_querywords Flask 常驻宿主（127.0.0.1:8082）。

接口:
    GET  /health   存活探针
    GET  /status   当前快照（input 日期 / worker 存活 / 进度 / 本地待传分片 / 已传台账）
    POST /resume   拉起 worker（幂等）
    POST /start    等价 /resume
    POST /stop     请求 worker 优雅停止（当前行结束后落 state 退出）

部署（生产 Windows 机）:
    python -m wb_querywords.app
    或 pythonw 常驻 + 计划任务每 2 分钟看门狗（MultipleInstances=IgnoreNew）
"""
import os
import glob
import sys
import threading
import time

from flask import Flask, jsonify
from waitress import serve

from wb_querywords import config, state, oss_uploader
from wb_querywords.worker import DailyWorker

# 日志落盘：nohup/pythonw 场景下 print 进黑洞；统一追加 wb_querywords/app.log，
# 每次启动把上一份降级 .old（保留最近一代现场，崩溃可查）。
_LOG_PATH = os.path.join(config.MODULE_DIR, "app.log")
os.makedirs(config.MODULE_DIR, exist_ok=True)
if os.path.exists(_LOG_PATH):
    try:
        os.replace(_LOG_PATH, _LOG_PATH + ".old")
    except OSError:
        pass
_logf = open(_LOG_PATH, "a", encoding="utf-8", buffering=1)
if sys.stdout is None or sys.stderr is None:
    sys.stdout = sys.stderr = _logf
else:
    os.dup2(_logf.fileno(), 1)
    os.dup2(_logf.fileno(), 2)

HOST = "127.0.0.1"
PORT = 8082

app = Flask(__name__)

_worker_thread = None
_worker_lock = threading.Lock()
_stop_event = threading.Event()
_progress = {"snap": {}}


def _worker_main():
    """后台线程入口：拿到 PID 锁才能开跑；退出释放。"""
    if not state.acquire_lock():
        _progress["snap"] = {"status": "locked", "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        print("[WQ][LOCK] 已有其他进程持有 run.lock，本 worker 退出", flush=True)
        return
    try:
        worker = DailyWorker(progress=_progress)
        worker._stop_event = _stop_event   # 共享停止信号
        worker.run()
    finally:
        state.release_lock()


def _ensure_worker():
    global _worker_thread
    with _worker_lock:
        if _worker_thread is not None and _worker_thread.is_alive():
            return False, "worker 已在运行"
        _stop_event.clear()
        _worker_thread = threading.Thread(target=_worker_main, daemon=True)
        _worker_thread.start()
        return True, "worker 已启动"


@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "service": "wb-querywords-host",
        "worker_alive": _worker_thread is not None and _worker_thread.is_alive(),
    })


@app.get("/status")
def status():
    snap = dict(_progress.get("snap") or {})
    st = state.load_state()
    # 本地未上传（未合并/未传）的临时分片日期，按类型归集
    pending = {}
    for sp in glob.glob(os.path.join(config.OUTPUT_DIR, "*", "*_shard_*.csv")):
        date = os.path.basename(sp)[:8]
        recom = os.path.basename(os.path.dirname(sp))
        pending.setdefault(recom, set()).add(date)
    snap.update({
        "worker_alive": _worker_thread is not None and _worker_thread.is_alive(),
        "recom_types": list(config.RECOM_TYPES),
        "store_id": config.STORE_ID,
        "input_date": st.get("input_date"),
        "processed_index": st.get("processed_index"),
        "pending_local_dates": {k: sorted(v) for k, v in pending.items()},
        "uploaded": sorted(oss_uploader.load_manifest().keys()),
    })
    if st:
        snap["state"] = st
    return jsonify(snap)


@app.post("/stop")
def stop():
    _stop_event.set()
    return jsonify({"ok": True, "message": "已请求停止，worker 将在当前行结束后退出"})


@app.post("/resume")
def resume():
    ok, msg = _ensure_worker()
    return jsonify({"ok": ok, "message": msg}), (200 if ok else 409)


@app.post("/start")
def start():
    return resume()


def main():
    print(f"[WQ-HOST] waitress 监听 http://{HOST}:{PORT}"
          f"（store={config.STORE_ID} recom_types={config.RECOM_TYPES}）", flush=True)
    if config.AUTO_START:
        ok, msg = _ensure_worker()
        print(f"[WQ-HOST] AUTO_START 拉起 worker: ok={ok} {msg}", flush=True)
    serve(app, host=HOST, port=PORT, threads=4)


if __name__ == "__main__":
    main()
