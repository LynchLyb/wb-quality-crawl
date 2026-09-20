# -*- coding: utf-8 -*-
"""新旧模块时间片轮转协调器（方案 A）。

背景: Chrome profile 独占，新旧模块共用同一 profile 必须串行运行。
本协调器通过调用两模块已有的 HTTP 接口（/stop /resume）实现时间片轮转，
不改动任何旧代码。

调度规则:
    - 新模块无待处理数据 → 旧模块 100% 跑
    - 新模块有数据 → 每天 00:00 优先启动新模块，之后按 NEW_RATIO 时间片轮转
    - 新模块当天全部完成 → 剩余时间旧模块 100% 跑
    - 新模块跨天未完成 → 第二天继续轮转

守护:
    - 切换失败重试 3 次；停止超时则放弃本次切换（避免 profile 冲突）
    - 主循环 try/except 包裹，单次异常不致进程猝死
    - 心跳文件供 check_health.py 判断存活

用法:
    python -m nmid_fetch.coordinator
"""
import json
import os
import time
import urllib.request
from datetime import datetime

OLD_MODULE = "http://127.0.0.1:8080"   # 旧模块 app.py
NEW_MODULE = "http://127.0.0.1:8081"   # 新模块 app_nmid.py

# 心跳文件：协调器每循环写一次，供 check_health.py 判断协调器存活
HEARTBEAT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "nmid_data", "coordinator.heartbeat")


def _write_heartbeat():
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_FILE), exist_ok=True)
        with open(HEARTBEAT_FILE, "w", encoding="utf-8") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
    except OSError:
        pass


def _http_get_json(url, timeout=5):
    """标准库 GET 返回 JSON；异常返回 None。"""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def _http_post(url, timeout=10):
    """标准库 POST；异常返回 False。"""
    try:
        req = urllib.request.Request(url, data=b"", method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status < 400
    except Exception:
        return False

NEW_RATIO = 0.2            # 新模块时间占比（新 20% / 旧 80%；新数据量小，旧模块保生产）
CYCLE_MINUTES = 60         # 轮转周期（分钟）
NEW_START_HOUR = 0         # 每天新模块优先启动的小时
STOP_WAIT_TIMEOUT = 30     # 等待模块完全停止的最长时间（秒）
SWITCH_RETRY = 3           # 切换失败重试次数
SWITCH_RETRY_INTERVAL = 5  # 切换重试间隔（秒）


def _worker_alive(base_url):
    """读取 /status 判断 worker 是否在跑；异常视为未运行。"""
    data = _http_get_json(f"{base_url}/status")
    if isinstance(data, dict):
        # 旧模块返回 {store_id: snap} 或单店 snap；新模块返回单 snap
        if "worker_alive" in data:
            return bool(data.get("worker_alive"))
        return any(bool(s.get("worker_alive")) for s in data.values()
                   if isinstance(s, dict))
    return False


def wait_worker_stopped(base_url, timeout=STOP_WAIT_TIMEOUT):
    """轮询 /status 直到 worker_alive=false，最多等 timeout 秒。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _worker_alive(base_url):
            return True
        time.sleep(2)
    return not _worker_alive(base_url)


def _pending_new_tasks():
    """新模块是否有待处理数据（pending_task_count > 0）。"""
    data = _http_get_json(f"{NEW_MODULE}/status")
    if isinstance(data, dict):
        return (data.get("pending_task_count") or 0) > 0
    return False


def switch_to_new():
    """停旧 → 起新；任一步失败返回 False（停止超时不强行 resume）。"""
    print("[协调器] 停止旧模块 → 启动新模块")
    if not _http_post(f"{OLD_MODULE}/stop?store=all"):
        print("[协调器] 旧模块 /stop 调用失败")
        return False
    if not wait_worker_stopped(OLD_MODULE):
        print("[协调器] 旧模块超时未停止，放弃本次切换（避免 profile 冲突）")
        return False
    if not _http_post(f"{NEW_MODULE}/resume"):
        print("[协调器] 新模块 /resume 调用失败")
        return False
    return True


def switch_to_old():
    """停新 → 起旧；任一步失败返回 False（停止超时不强行 resume）。"""
    print("[协调器] 停止新模块 → 启动旧模块")
    if not _http_post(f"{NEW_MODULE}/stop"):
        print("[协调器] 新模块 /stop 调用失败")
        return False
    if not wait_worker_stopped(NEW_MODULE):
        print("[协调器] 新模块超时未停止，放弃本次切换（避免 profile 冲突）")
        return False
    if not _http_post(f"{OLD_MODULE}/resume?store=all"):
        print("[协调器] 旧模块 /resume 调用失败")
        return False
    return True


def _switch_with_retry(fn, label):
    """切换失败重试；重试用尽交还主循环下轮处理，不强行操作。"""
    for i in range(1, SWITCH_RETRY + 1):
        if fn():
            return True
        print(f"[协调器] {label}失败（第 {i}/{SWITCH_RETRY} 次），{SWITCH_RETRY_INTERVAL}s 后重试")
        time.sleep(SWITCH_RETRY_INTERVAL)
    print(f"[协调器] {label}重试 {SWITCH_RETRY} 次仍失败，等待主循环下轮重试")
    return False


def _new_all_finished():
    """新模块当前是否无待处理/已全部完成（worker 空闲且无 pending）。"""
    return not _pending_new_tasks()


def main():
    new_minutes = int(CYCLE_MINUTES * NEW_RATIO)
    old_minutes = CYCLE_MINUTES - new_minutes
    last_new_day = None

    # 启动时默认旧模块跑（新模块数据未准备好时不抢）
    print(f"[协调器] 启动：新模块占比 {NEW_RATIO:.0%}（{new_minutes}min / {old_minutes}min 每周期）")

    while True:
        try:
            _write_heartbeat()
            now = datetime.now()
            today = now.date()

            if not _pending_new_tasks():
                # 新模块无数据：旧模块 100% 跑
                if _worker_alive(NEW_MODULE):
                    _switch_with_retry(switch_to_old, "切旧模块")
                time.sleep(60)
                continue

            # 每天首次检测到新数据：优先启动新模块
            if last_new_day != today and now.hour >= NEW_START_HOUR:
                print(f"[协调器] {today} 检测到新数据，优先启动新模块")
                if _switch_with_retry(switch_to_new, "切新模块"):
                    last_new_day = today   # 成功才记；失败下轮循环重试

            # 新模块时间片
            if not _worker_alive(NEW_MODULE):
                _switch_with_retry(switch_to_new, "切新模块")
            for _ in range(new_minutes):
                time.sleep(60)
                if _new_all_finished():
                    print("[协调器] 新模块已完成全部任务，切回旧模块")
                    _switch_with_retry(switch_to_old, "切旧模块")
                    # 旧模块跑到当天结束
                    while datetime.now().date() == today:
                        time.sleep(60)
                    break
            else:
                # 新模块时间片用完（未完成）：切旧模块时间片
                _switch_with_retry(switch_to_old, "切旧模块")
                for _ in range(old_minutes):
                    time.sleep(60)
        except Exception as e:
            # 守护：单次循环异常不杀死协调器进程
            print(f"[协调器] 主循环异常: {type(e).__name__}: {e}，60s 后继续")
            time.sleep(60)


if __name__ == "__main__":
    main()
