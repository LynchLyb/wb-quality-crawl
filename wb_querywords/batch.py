# -*- coding: utf-8 -*-
"""wb_querywords 批处理入口（供每日计划任务链调用，非常驻宿主）。

语义:
    - resume=True：从 state.json 断点续跑当天 input（词不丢）。
    - one_shot=True：跑完当天 input 全部行、或命中每日对比推荐配额上限（limitExceeded）、
      或 OSS input 无可用日期，三者之一即退出进程（不空转等更新 input）。
    - 拿到单实例锁才开跑，避免与常驻宿主 / 另一个批处理抢同一 store1 profile。

用法（生产 Windows 机，项目根目录）:
    .venv\\Scripts\\python.exe -u -m wb_querywords.batch

退出码:
    0  正常收尾（跑完 / 配额用尽 / 无 input）——计划任务链据此继续跑 fetch_all
    2  没拿到单实例锁（已有实例在跑）——本次直接跳过
"""
import sys

from wb_querywords import config, state
from wb_querywords.worker import DailyWorker


def main():
    if not state.acquire_lock():
        print("[BATCH][LOCK] 已有 wb_querywords 实例持有 run.lock，本次批处理跳过", flush=True)
        return 2

    progress = {"snap": {}}
    worker = DailyWorker(store_id=config.STORE_ID, resume=True,
                         progress=progress, one_shot=True)
    try:
        worker.run()
    finally:
        state.release_lock()

    snap = progress.get("snap", {})
    print(f"[BATCH] 结束 status={snap.get('status')} "
          f"processed={snap.get('processed_index')}/{snap.get('total_rows')} "
          f"input_date={snap.get('input_date')}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
