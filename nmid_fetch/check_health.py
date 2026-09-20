# -*- coding: utf-8 -*-
"""一键健康检测：检查旧模块 / 新模块 / 协调器 三者运行状态。

用法:
    python -m nmid_fetch.check_health

输出: 表格化状态 + 每项正常/异常判断 + 总体结论。
"""
import json
import os
import urllib.request
from datetime import datetime

OLD_MODULE = "http://127.0.0.1:8080"
NEW_MODULE = "http://127.0.0.1:8081"
HEARTBEAT_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "nmid_data", "coordinator.heartbeat")
HEARTBEAT_MAX_AGE = 180  # 心跳超过 3 分钟视为协调器已死


def _get_json(url, timeout=5):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        return {"_error": f"{type(e).__name__}"}


def _worker_alive_old(status):
    """旧模块 /status 返回 {store_id: snap}；任一店 worker_alive 即认为在跑。"""
    if not isinstance(status, dict) or "_error" in status:
        return None
    snaps = [s for s in status.values() if isinstance(s, dict)]
    if not snaps:
        return None
    return any(bool(s.get("worker_alive")) for s in snaps)


def _fmt(val):
    if val is None:
        return "-"
    if val is True:
        return "运行中"
    if val is False:
        return "已停止"
    return str(val)


def main():
    print("=" * 70)
    print(f"WB 抓取系统健康检测  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print("=" * 70)

    rows = []

    # 旧模块
    old_health = _get_json(f"{OLD_MODULE}/health")
    old_status = _get_json(f"{OLD_MODULE}/status")
    old_up = "_error" not in old_health
    old_worker = _worker_alive_old(old_status)
    rows.append(("旧模块 (8080)", "存活" if old_up else f"不可达({old_health.get('_error')})",
                 _fmt(old_worker), "全量抓取"))

    # 新模块
    new_health = _get_json(f"{NEW_MODULE}/health")
    new_status = _get_json(f"{NEW_MODULE}/status")
    new_up = "_error" not in new_health
    new_worker = new_status.get("worker_alive") if isinstance(new_status, dict) else None
    pending = new_status.get("pending_task_count") if isinstance(new_status, dict) else None
    rows.append(("新模块 (8081)", "存活" if new_up else f"不可达({new_health.get('_error')})",
                 _fmt(new_worker), f"按nmID (待处理task={pending})"))

    # 协调器（心跳文件）
    coord_state = "未运行"
    if os.path.exists(HEARTBEAT_FILE):
        try:
            with open(HEARTBEAT_FILE, encoding="utf-8") as f:
                hb = f.read().strip()
            hb_time = datetime.strptime(hb, "%Y-%m-%d %H:%M:%S")
            age = (datetime.now() - hb_time).total_seconds()
            coord_state = f"运行中 (心跳{int(age)}s前)" if age < HEARTBEAT_MAX_AGE \
                else f"疑似已死 (心跳{int(age)}s前)"
        except (OSError, ValueError):
            coord_state = "心跳异常"
    rows.append(("协调器", coord_state, "-", "80:20→99:1 轮转"))

    # 打印表格
    print(f"{'组件':<16}{'HTTP/进程':<24}{'Worker':<10}{'说明'}")
    print("-" * 70)
    for name, up, worker, note in rows:
        print(f"{name:<16}{up:<24}{worker:<10}{note}")

    # 总体判断
    print("-" * 70)
    problems = []
    if not old_up:
        problems.append("旧模块不可达（未启动或端口错）")
    if not new_up:
        problems.append("新模块不可达（未启动或端口错）")
    if coord_state.startswith("疑似已死") or coord_state == "心跳异常":
        problems.append("协调器心跳过期（可能已崩溃）")
    # 互斥检查：两者 worker 不应同时运行（共用 profile）
    if old_worker is True and new_worker is True:
        problems.append("警告: 新旧 worker 同时在跑（profile 冲突风险!）")

    if problems:
        print("结论: 发现问题")
        for p in problems:
            print(f"  [!] {p}")
    else:
        print("结论: 一切正常")
        if old_worker is True and new_worker is not True:
            print("  当前: 旧模块在跑（新模块停止/无数据）")
        elif new_worker is True and old_worker is not True:
            print("  当前: 新模块在跑（旧模块停止）")
        elif old_worker is not True and new_worker is not True:
            print("  当前: 两者都停止（等待协调器调度或手动启动）")
    print("=" * 70)


if __name__ == "__main__":
    main()
