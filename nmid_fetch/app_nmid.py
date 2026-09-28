# -*- coding: utf-8 -*-
"""nmid_fetch 常驻 Flask 宿主（端口 8081，与旧模块 8080 隔离）。

职责:
    - 后台 worker 线程按当前账号规则处理 content-opt-pool/ 下的输入 CSV：
        treatment_001 → 每轮重复跑；control → 每卖家只跑最新的一份（台账去重）
    - treatment 一轮跑完重置断点立即开始下一轮；本轮没干活（无数据/全部被跳过）则等待
    - HTTP 接口供协调器/运维控制: /health /status /stop /resume

与旧模块 app.py 的区别:
    - 旧模块每店一个 supervisor + cron 巡检；本模块单 worker 串行遍历待跑清单
    - 本模块启动时不自起 worker（由协调器 stop/resume 或人工 POST /resume 拉起），
      避免与旧模块抢 profile
"""
import os
import sys
import threading
import time

from flask import Flask, jsonify, request
from waitress import serve

import config
from nmid_fetch import oss_input
from nmid_fetch.singleton import single_instance_or_exit
from nmid_fetch.fetch_by_nmid import (
    _lock_file, _run_dir, control_already_done, load_control_ledger,
    process_task, reset_state_for_new_round, store_id_for_seller,
)
from fetch_all import acquire_single_instance, release_single_instance

# 日志落盘：pythonw 无控制台、print 会进黑洞，统一追加到 nmid_data/app_nmid.log。
# 启动时把上一份降级为 .old（只保留一代），避免无限增长；崩溃重启后上次运行现场仍在 .old 可查。
_LOG_PATH = os.path.join(config.BASE_DIR, "nmid_data", "app_nmid.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
if os.path.exists(_LOG_PATH):
    try:
        os.replace(_LOG_PATH, _LOG_PATH + ".old")
    except OSError:
        pass
_logf = open(_LOG_PATH, "a", encoding="utf-8", buffering=1)
if sys.stdout is None or sys.stderr is None:   # pythonw：无控制台
    sys.stdout = sys.stderr = _logf
else:                                          # 交互终端：fd 级重定向，子线程 print 也进文件
    os.dup2(_logf.fileno(), 1)
    os.dup2(_logf.fileno(), 2)

HOST = "127.0.0.1"
PORT = 8081
IDLE_SLEEP = 60  # 本轮没干活（无数据 / 全部被跳过）时的等待间隔（秒）

app = Flask(__name__)

# 全局运行状态
_stop_event = threading.Event()
_progress = {"snap": {}}
_worker_thread = None
_worker_lock = threading.Lock()


def _configured_sellers():
    """config.json 里已配置卖家 ID 的店铺（未配置的输入文件一律不跑）。"""
    out = []
    for s in config.STORES:
        sid = config.get_seller_id(s["id"])
        if sid:
            out.append(str(sid))
    return out


def _sleep_idle(seconds, reason, extra=None):
    """置 idle 快照并分秒睡（能被 /stop 立即打断）。"""
    snap = {"status": "idle", "reason": reason,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    if extra:
        snap.update(extra)
    _progress["snap"] = snap
    for _ in range(int(seconds)):
        if _stop_event.is_set():
            return False
        time.sleep(1)
    return not _stop_event.is_set()


def _plan_jobs():
    """拉一轮待跑清单：(jobs, 本轮跳过原因汇总)。只保留 config.json 里的两个店。"""
    sellers = _configured_sellers()
    jobs, unknown = oss_input.plan_round(sellers=sellers)
    skipped = []
    if unknown:
        skipped.append(f"命名不符 {len(unknown)} 个")
    kept = []
    unconfigured = {}
    for j in jobs:
        store_id = store_id_for_seller(j["seller_id"])
        if store_id is None:
            unconfigured[j["seller_id"]] = unconfigured.get(j["seller_id"], 0) + 1
            continue
        j["store_id"] = store_id
        kept.append(j)
    if unconfigured:
        detail = ", ".join(f"{k}×{v}" for k, v in sorted(unconfigured.items()))
        skipped.append(f"未配置的卖家: {detail}")
        print(f"[WARN] 以下卖家未在 config.json 配置，本轮共跳过 {sum(unconfigured.values())} "
              f"个输入文件: {detail}")
    return kept, skipped


def _main_loop():
    """常驻循环：按规则跑 treatment_001（每轮重复）+ control（只跑最新、台账去重）。"""
    while not _stop_event.is_set():
        jobs, skipped = _plan_jobs()
        if not jobs:
            reason = "no_matched_input"
            print(f"[IDLE] 没有符合规则的输入文件（{'; '.join(skipped) or '池为空'}），"
                  f"{IDLE_SLEEP}s 后重查")
            if not _sleep_idle(IDLE_SLEEP, reason, {"skipped": skipped}):
                return
            continue

        todo = []
        for j in jobs:
            # control 只跑一次：台账里同一份（key+mtime）就跳过，有更新的才重跑
            if j["variant"] == "control" and control_already_done(j["store_id"], j):
                continue
            todo.append(j)
        if not todo:
            ledger = load_control_ledger()
            done_desc = ", ".join(f"{k}:{v.get('name')}" for k, v in sorted(ledger.items()))
            print(f"[IDLE] treatment 本轮已完成、control 已记账（{done_desc}），"
                  f"{IDLE_SLEEP}s 后重查")
            if not _sleep_idle(IDLE_SLEEP, "all_done",
                               {"skipped": skipped, "control_done": ledger}):
                return
            continue

        print(f"[ROUND] 本轮待跑 {len(todo)} 个输入文件: "
              + ", ".join(f"{j['task_id']}/{j['name']}" for j in todo))
        round_done = True
        worked = False
        treatments = []
        for j in todo:
            if _stop_event.is_set():
                return
            store_id = j["store_id"]
            store = config.get_store(store_id)
            lock = _lock_file(store_id)
            if not acquire_single_instance(lock):
                print(f"[LOCK] {store_id} 已被其他进程占用，跳过本轮")
                round_done = False
                continue
            try:
                r = process_task(store, j["task_id"], j["seller_id"], resume=True,
                                 stop_event=_stop_event, progress=_progress,
                                 variant=j["variant"], index=j["index"],
                                 oss_key=j["key"], row=j)
                worked = True
                if not r.get("finished"):
                    round_done = False
                elif j["variant"] == "treatment":
                    treatments.append((store_id, j))
            except Exception as e:
                print(f"[ERROR] 处理 {j['task_id']}/{j['name']} 异常: {type(e).__name__}: {e}")
                round_done = False
            finally:
                release_single_instance(lock)

        if not worked and not _stop_event.is_set():
            # 整轮都被跳过（如锁被占）：等一会儿再试，不空转刷 OSS / 刷日志
            print(f"[IDLE] 本轮未实际执行任何任务，{IDLE_SLEEP}s 后重试")
            if not _sleep_idle(IDLE_SLEEP, "nothing_executed", {"skipped": skipped}):
                return
            continue

        if round_done and not _stop_event.is_set():
            # treatment 一轮跑完：重置断点，立即开始下一轮（每次全量重跑）
            # control 不重置：它只跑一次，靠台账去重，出现更新的文件时自然重跑
            print(f"[ROUND] 本轮全部完成，重置 {len(treatments)} 个 treatment 断点后立即开始下一轮")
            for store_id, j in treatments:
                reset_state_for_new_round(
                    _run_dir(store_id, j["task_id"], j["variant"], j["index"]),
                    oss_input.variant_tag(j["variant"], j["index"]))


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
    # 待处理任务数（供协调器判断是否需要切换）：按当前规则统计，不按池里目录数
    try:
        snap["pending_task_count"] = oss_input.pending_task_count(
            sellers=_configured_sellers())
    except Exception:
        snap["pending_task_count"] = 0
    snap["control_ledger"] = load_control_ledger()
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
    # 单实例守卫：计划任务的 2 分钟重复触发与人工 Start-ScheduledTask 可能同一瞬间各起
    # 一个实例（IgnoreNew 与 PID 锁文件在这种竞态下都拦不住），改用 OS 级独占绑定保证。
    with single_instance_or_exit("app_nmid", log=print):
        print(f"[NMID-HOST] waitress 监听 http://{HOST}:{PORT}（/health /status /stop /resume）")
        serve(app, host=HOST, port=PORT, threads=4)
    return 0


if __name__ == "__main__":
    sys.exit(main())
