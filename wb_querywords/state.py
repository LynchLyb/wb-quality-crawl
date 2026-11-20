# -*- coding: utf-8 -*-
"""wb_querywords 断点状态与单实例锁。

状态文件 state.json:
    {
      "input_date": "20261117",       # 当前正在跑的 input 日期（一对一决定 output 命名）
      "processed_index": 1234,         # 当前 input 已处理到的行号（0-based）
      "current_recom_index": 0,        # 当前行内已完成的 recomType 数（B 双路用）
      "store_index": 0,                 # 当前跑的回退链店铺下标（STORE_IDS[store_index]）
      "shards": {"<recom>": {"shard_index": 1, "calls_in_shard": 37}},  # 当前店各类型分片进度
      "updated_at": "2026-11-19 03:12:00"
    }

单实例锁：复用 fetch_all 的 PID 锁（同机只有一个 worker 抢浏览器）。

day_done.json（每日链/每小时预检用的收尾台账，与 state.json 分开）:
    {
      "input_date": "20261120",     # 收尾对应的 input 日期
      "status": "done",             # done=全部词跑完；quota=当日全部店配额用尽。两者都表示“这份跑过了”，check_due 见此即丢弃、不再回来，只认比它更新的日期
      "calendar": "20261120",       # 收尾当天的 BJT 日期（仅供观测；现行策略不再据它跨天续跑）
      "updated_at": "2026-11-20 17:03:10"
    }
"""
import json
import os
import time

from . import config
from fetch_all import acquire_single_instance, release_single_instance


def _atomic_write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_state():
    if not os.path.exists(config.STATE_FILE):
        return {}
    try:
        with open(config.STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(input_date, processed_index, current_recom_index, shards=None, store_index=0):
    """写断点状态（input_date 为当前 input 日期；shards 为当前店各类型分片进度；
    store_index 为当前正在跑的回退链店铺下标，崩溃/切日后可恢复到同一店续跑）。"""
    _atomic_write_json(config.STATE_FILE, {
        "input_date": input_date,
        "processed_index": int(processed_index),
        "current_recom_index": int(current_recom_index),
        "store_index": int(store_index),
        "shards": shards or {},
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })


def reset_state_for_new_input(input_date):
    """切到新的 input 日期时归零进度与分片，并回到回退链首店。"""
    save_state(input_date, 0, 0, shards={}, store_index=0)


def load_day_done():
    """读收尾台账；无文件/损坏返回 {}。"""
    if not os.path.exists(config.DAY_DONE_FILE):
        return {}
    try:
        with open(config.DAY_DONE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def mark_day_done(input_date, status):
    """记录某 input 日期收尾：status='done'（全部词跑完）或 'quota'（当日全部店配额用尽）。
    无论哪种都代表“这份跑过了”：此后 check_due 只要看到 day_done.input_date==上次日期就丢弃不续，
    只有 OSS 出现比它更新的日期才会再跑（每日新文件模）。calendar 仅供观测。"""
    _atomic_write_json(config.DAY_DONE_FILE, {
        "input_date": input_date,
        "status": status,
        "calendar": config.today_bjt_str(),
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })


def acquire_lock():
    return acquire_single_instance(config.LOCK_FILE)


def release_lock():
    release_single_instance(config.LOCK_FILE)


def _set_snap(progress, **kw):
    if progress is None:
        return
    progress["snap"] = {**progress.get("snap", {}), **kw,
                        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
