# -*- coding: utf-8 -*-
"""FetchSupervisor：把 fetch_all 装成常驻后台 worker 线程 + APScheduler cron 巡检守护。

线程与锁的正确性（重点）:
    - driver 线程亲和：worker 线程内独占 create_driver→使用→quit，绝不跨线程发 Selenium 命令
    - threading.Lock（非阻塞获取）做进程内单-worker 守卫；跨进程由 fetch_all 的 PID 锁负责
    - 巡检只在 scheduler 线程读 worker.is_alive()/progress，需重启才 spawn 新线程，快进快出
"""
import threading
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import fetch_all


class FetchSupervisor:
    """管理单个店铺的 fetch_all worker 线程生命周期与定时巡检自愈（每店一个实例）。"""

    def __init__(self, store, cron_expr="*/2 * * * *", auto_start=True):
        self.store = store                      # 已解析店铺 dict（config.resolve_store 的产物）
        self.store_id = store["id"]
        self.cron_expr = cron_expr
        self.auto_start = auto_start
        self._worker_lock = threading.Lock()    # 进程内单-worker 守卫（非阻塞获取）
        self._worker = None                     # 当前 worker 线程
        self._stop_event = threading.Event()    # 通知 worker 优雅停止
        self._progress = {}                     # {"snap": {...}} 原子进度快照
        self._restart_count = 0                 # 巡检自动重启次数
        self._started_at = None                 # 当前 worker 启动时间
        self._last_inspect = None               # 上次巡检时间
        self._user_stopped = False              # 用户显式 /stop 后不自动重启
        self._ever_started = False              # 本店 worker 是否曾被启动过；False=休眠店(auto_start=False 且未 /start)，巡检不自动拉起
        self._scheduler = BackgroundScheduler()

    # ---------------- worker 生命周期 ----------------
    def start_worker(self, resume=True):
        """启动 worker 线程。已在运行则跳过（非阻塞锁）。返回 (ok, message)。"""
        if not self._worker_lock.acquire(blocking=False):
            return False, f"[{self.store_id}] worker 已在运行"
        # 拿到锁：本店自此纳入巡检自愈（曾启动过）；为本次 worker 重置停止信号与进度，并清除"用户已停止"标记
        self._ever_started = True
        self._stop_event = threading.Event()
        self._progress = {}
        self._user_stopped = False
        self._started_at = time.strftime("%Y-%m-%d %H:%M:%S")
        stop_event = self._stop_event    # 捕获本次 worker 专属引用，避免被下次重置串号
        progress = self._progress

        def _run():
            try:
                fetch_all.run_fetch(resume=resume, stop_event=stop_event, progress=progress,
                                    store=self.store)
            except Exception as e:
                # run_fetch 内部已兜底返回，这里是双保险，确保异常也能被巡检感知
                snap = dict(progress.get("snap") or {})
                snap.update(status="crashed", last_error=f"{type(e).__name__}: {e}",
                            store_id=self.store_id, updated_at=time.strftime("%Y-%m-%d %H:%M:%S"))
                progress["snap"] = snap
            finally:
                self._worker_lock.release()

        self._worker = threading.Thread(target=_run, name=f"fetch-worker-{self.store_id}", daemon=True)
        self._worker.start()
        return True, f"[{self.store_id}] worker 已启动（resume={resume}）"

    def stop_worker(self):
        """请求优雅停止：标记用户停止 + 置位 stop_event。

        worker 最多滞后一个调用周期退出并保存断点；置 _user_stopped 后巡检不再自动重启。
        """
        self._user_stopped = True
        self._stop_event.set()
        return True, "已发送停止信号，worker 将在当前调用周期结束后优雅退出并保存断点"

    def is_running(self):
        return self._worker is not None and self._worker.is_alive()

    # ---------------- cron 巡检守护 ----------------
    def inspect(self):
        """巡检 worker 健康：挂了且非用户主动停、未拉完、非终态 → 自动 --resume 重启。"""
        self._last_inspect = time.strftime("%Y-%m-%d %H:%M:%S")
        if self.is_running():
            return
        if not self._ever_started:
            return  # 从未启动（auto_start=False 且未显式 /start）：保持休眠，不自动拉起（防未登录新店空转重启）
        if self._user_stopped:
            return  # 用户显式停止，尊重意图不自动重启
        snap = self._progress.get("snap") or {}
        if snap.get("finished"):
            return  # 已全部拉完，无需重启
        if snap.get("error") in ("no_resume_state", "locked", "store_mismatch",
                                 "store_verify_failed", "store_not_available"):
            return  # 终态：无可续传 / 他进程持锁 / 自动选店或校验不通过（需人工确认店铺），重启无意义
        ok, _msg = self.start_worker(resume=True)
        if ok:
            self._restart_count += 1
            print(f"[{self.store_id}][INSPECT] worker 未运行且未拉完 → 自动重启（第 {self._restart_count} 次）")

    # ---------------- 宿主生命周期 ----------------
    def start(self):
        """注册 cron 巡检任务并启动 scheduler；auto_start 时立即拉起 worker。"""
        self._scheduler.add_job(
            self.inspect,
            CronTrigger.from_crontab(self.cron_expr),
            id="inspect", max_instances=1, coalesce=True)
        self._scheduler.start()
        print(f"[{self.store_id}][SUPERVISOR] 巡检守护已启动，cron={self.cron_expr}")
        if self.auto_start:
            _ok, msg = self.start_worker(resume=True)
            print(f"[{self.store_id}][SUPERVISOR] AUTO_START: {msg}")

    def shutdown(self):
        """停止 scheduler 并请求 worker 优雅退出（宿主进程关闭时调用）。"""
        try:
            self._scheduler.shutdown(wait=False)
        except Exception:
            pass
        self.stop_worker()

    def get_status(self):
        """返回给 /status 的快照（worker 存活、重启计数、巡检信息、进度）。"""
        snap = self._progress.get("snap") or {}
        return {
            "store_id": self.store_id,
            "worker_alive": self.is_running(),
            "restart_count": self._restart_count,
            "worker_started_at": self._started_at,
            "last_inspect": self._last_inspect,
            "cron_expr": self.cron_expr,
            "auto_start": self.auto_start,
            "ever_started": self._ever_started,
            "user_stopped": self._user_stopped,
            "progress": snap,
        }
