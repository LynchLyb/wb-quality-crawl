# -*- coding: utf-8 -*-
"""新旧模块时间片轮转协调器（方案 A）。

背景: Chrome profile 独占，新旧模块共用同一 profile 必须串行运行。
本协调器通过调用两模块已有的 HTTP 接口（/stop /resume）实现时间片轮转，
不改动任何旧代码。

调度规则:
    - 新模块无待处理数据 → 旧模块 100% 跑
    - 新模块有数据 → 天级轮转：新模块每天跑 NEW_START_HOUR 起的 NEW_HOURS 小时（默认 00:00-12:00）
    - 新模块窗口外 → 旧模块跑（断点续传）
    - 新模块窗口内未跑完 → 第二天窗口从断点继续

守护:
    - 切换失败重试 3 次；停止超时则放弃本次切换（避免 profile 冲突）
    - 停止等待超时 180s（覆盖旧模块首捕 120s + 限流暂停 30s 的最坏优雅停止耗时）
    - 新模块窗口内每分钟补发旧模块 /stop，防旧宿主 AUTO_START/崩溃重启复活 worker
    - 主循环 try/except 包裹，单次异常不致进程猝死
    - 心跳文件供 check_health.py 判断存活

用法:
    python -m nmid_fetch.coordinator
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime

OLD_MODULE = "http://127.0.0.1:8080"   # 旧模块 app.py
NEW_MODULE = "http://127.0.0.1:8081"   # 新模块 app_nmid.py

# 心跳文件：协调器每循环写一次，供 check_health.py 判断协调器存活
HEARTBEAT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "nmid_data", "coordinator.heartbeat")

# 新老交替历史：每次切换追加一行带时间戳的记录，长期留档（供人工/脚本查切换时间）
SWITCH_HISTORY_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "nmid_data", "switch_history.log")

# 日志落盘：协调器同样以 pythonw 无控制台拉起、print 会进黑洞，统一追加到 nmid_data/coordinator.log。
# 启动时把上一份降级为 .old（只保留一代）；_log 自带每行时间戳。
_LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "nmid_data", "coordinator.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
if os.path.exists(_LOG_PATH):
    try:
        os.replace(_LOG_PATH, _LOG_PATH + ".old")
    except OSError:
        pass
_logf = open(_LOG_PATH, "a", encoding="utf-8", buffering=1)
if sys.stdout is None or sys.stderr is None:   # pythonw：无控制台
    sys.stdout = sys.stderr = _logf
else:                                          # 交互终端：fd 级重定向
    os.dup2(_logf.fileno(), 1)
    os.dup2(_logf.fileno(), 2)


def _write_heartbeat():
    try:
        os.makedirs(os.path.dirname(HEARTBEAT_FILE), exist_ok=True)
        with open(HEARTBEAT_FILE, "w", encoding="utf-8") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
    except OSError:
        pass


def _log(msg):
    """带时间戳打印，coordinator.log 每行都含时间，便于核对切换时刻。"""
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def _record_switch(direction, ok, note=""):
    """把一次新老交替(时间戳/方向/成败/备注)追加到 switch_history.log。"""
    try:
        os.makedirs(os.path.dirname(SWITCH_HISTORY_FILE), exist_ok=True)
        with open(SWITCH_HISTORY_FILE, "a", encoding="utf-8") as f:
            f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {direction} | "
                    f"{'OK' if ok else 'FAILED'}"
                    f"{(' | ' + note) if note else ''}\n")
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

NEW_START_HOUR = 0         # 新模块每天窗口开始小时（可调）
NEW_HOURS = 24             # 新模块每天运行小时数（可调；24=全量跑新，旧模块仅在无待处理 nmID 任务时兜底）
                           # 约束: NEW_START_HOUR + NEW_HOURS <= 24（窗口不跨天）
STOP_WAIT_TIMEOUT = 180    # 等待模块完全停止的最长时间（秒）；旧模块优雅停止最坏
                           # ≈ 首捕 CAPTURE_TIMEOUT 120s + 限流 RATE_LIMIT_PAUSE 30s + 周期余量
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
    _log("[协调器] 停止旧模块 → 启动新模块")
    if not _http_post(f"{OLD_MODULE}/stop?store=all"):
        _log("[协调器] 旧模块 /stop 调用失败")
        _record_switch("OLD->NEW", False, "old /stop failed")
        return False
    if not wait_worker_stopped(OLD_MODULE):
        _log("[协调器] 旧模块超时未停止，放弃本次切换（避免 profile 冲突）")
        _record_switch("OLD->NEW", False, "old stop timeout")
        return False
    if not _http_post(f"{NEW_MODULE}/resume"):
        _log("[协调器] 新模块 /resume 调用失败")
        _record_switch("OLD->NEW", False, "new /resume failed")
        return False
    _record_switch("OLD->NEW", True, "new window 00:00-12:00")
    return True


def switch_to_old():
    """停新 → 起旧；任一步失败返回 False（停止超时不强行 resume）。"""
    _log("[协调器] 停止新模块 → 启动旧模块")
    if not _http_post(f"{NEW_MODULE}/stop"):
        _log("[协调器] 新模块 /stop 调用失败")
        _record_switch("NEW->OLD", False, "new /stop failed")
        return False
    if not wait_worker_stopped(NEW_MODULE):
        _log("[协调器] 新模块超时未停止，放弃本次切换（避免 profile 冲突）")
        _record_switch("NEW->OLD", False, "new stop timeout")
        return False
    if not _http_post(f"{OLD_MODULE}/resume?store=all"):
        _log("[协调器] 旧模块 /resume 调用失败")
        _record_switch("NEW->OLD", False, "old /resume failed")
        return False
    _record_switch("NEW->OLD", True, "old window 12:00-24:00")
    return True


def _switch_with_retry(fn, label):
    """切换失败重试；重试用尽交还主循环下轮处理，不强行操作。"""
    for i in range(1, SWITCH_RETRY + 1):
        if fn():
            return True
        _log(f"[协调器] {label}失败（第 {i}/{SWITCH_RETRY} 次），{SWITCH_RETRY_INTERVAL}s 后重试")
        time.sleep(SWITCH_RETRY_INTERVAL)
    _log(f"[协调器] {label}重试 {SWITCH_RETRY} 次仍失败，等待主循环下轮重试")
    return False


def _in_new_window(now):
    """当前是否处于新模块每天运行窗口（窗口不跨天）。"""
    return NEW_START_HOUR <= now.hour < NEW_START_HOUR + NEW_HOURS


def main():
    _log(f"[协调器] 启动：天级轮转，新模块每天 {NEW_START_HOUR:02d}:00-"
         f"{NEW_START_HOUR + NEW_HOURS:02d}:00（{NEW_HOURS}h），"
         f"旧模块其余 {24 - NEW_HOURS}h")

    while True:
        try:
            _write_heartbeat()
            now = datetime.now()

            if not _pending_new_tasks():
                # 新模块无数据：旧模块 100% 跑
                if _worker_alive(NEW_MODULE):
                    _switch_with_retry(switch_to_old, "切旧模块")
                time.sleep(60)
                continue

            if _in_new_window(now):
                # 新模块窗口：新模块跑（断点续传）
                if _worker_alive(OLD_MODULE):
                    # 持续压制：防旧宿主 AUTO_START/崩溃重启复活 worker 抢 profile
                    _http_post(f"{OLD_MODULE}/stop?store=all")
                if not _worker_alive(NEW_MODULE):
                    _switch_with_retry(switch_to_new, "切新模块")
            else:
                # 旧模块窗口：旧模块跑
                if _worker_alive(NEW_MODULE):
                    _switch_with_retry(switch_to_old, "切旧模块")
                elif not _worker_alive(OLD_MODULE):
                    if not _http_post(f"{OLD_MODULE}/resume?store=all"):
                        _log("[协调器] 旧模块 /resume 调用失败，下轮重试")
            time.sleep(60)
        except Exception as e:
            # 守护：单次循环异常不杀死协调器进程
            _log(f"[协调器] 主循环异常: {type(e).__name__}: {e}，60s 后继续")
            time.sleep(60)


if __name__ == "__main__":
    main()
