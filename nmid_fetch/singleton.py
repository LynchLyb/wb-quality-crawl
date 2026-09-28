# -*- coding: utf-8 -*-
"""常驻进程单实例守卫。

为什么不用 PID 锁文件：`fetch_all.acquire_single_instance` 是"读旧 PID → 判活 → 写自己"
三步非原子操作。计划任务的 2 分钟重复触发与人工 `Start-ScheduledTask` 可能在同一瞬间
各拉起一个实例（`MultipleInstancesPolicy=IgnoreNew` 在这种竞态下也拦不住），两个进程
同时读到"无锁"便双双放行——现场就出现过 2× app_nmid + 2× coordinator。

这里改用回环端口独占绑定：`bind()` 由操作系统保证原子互斥，进程消失（含被 kill）时
端口自动释放，不存在陈旧锁需要接管。用法：

    with single_instance_or_exit("app_nmid", 48081):
        serve(...)
"""
import contextlib
import socket

# 各常驻进程的守卫端口（仅绑定 127.0.0.1，不监听、不收发数据）
GUARD_PORTS = {
    "app_nmid": 48081,
    "coordinator": 48080,
}

GUARD_HOST = "127.0.0.1"


def try_acquire(name, port=None):
    """尝试占用单实例标识。

    返回 (socket, None) 表示拿到；(None, 持有者提示) 表示已有同类进程在跑。
    拿到的 socket 必须一直持有（进程退出即释放），不要 close。
    """
    port = port or GUARD_PORTS.get(name)
    if not port:
        raise ValueError(f"未登记的守卫端口: {name}")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((GUARD_HOST, port))
    except OSError as e:
        s.close()
        # 10048 = WSAEADDRINUSE（Windows），98 = EADDRINUSE（Linux）
        if getattr(e, "winerror", None) == 10048 or e.errno == 98:
            return None, f"{GUARD_HOST}:{port} 已被占用（同类进程在跑）"
        raise
    return s, None


@contextlib.contextmanager
def single_instance_or_exit(name, port=None, log=print):
    """拿不到单实例标识就抛 SystemExit(0)。

    退出码必须是 0：这是预期行为，返回非 0 会触发计划任务 RestartOnFailure 反复重启。
    """
    sock, err = try_acquire(name, port)
    if sock is None:
        log(f"[SINGLETON] {name} 已有实例在跑，本实例退出: {err}")
        raise SystemExit(0)
    try:
        yield sock
    finally:
        try:
            sock.close()
        except OSError:
            pass
