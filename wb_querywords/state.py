# -*- coding: utf-8 -*-
"""wb_querywords 断点状态与单实例锁。

状态文件 state.json:
    {
      "input_date": "20261117",       # 当前正在跑的 input 日期（一对一决定 output 命名）
      "processed_index": 1234,         # 当前 input 已处理到的行号（0-based）
      "current_recom_index": 0,        # 当前行内已完成的 recomType 数（B 双路用）
      "shards": {"<recom>": {"shard_index": 1, "calls_in_shard": 37}},  # 各类型分片进度
      "updated_at": "2026-11-19 03:12:00"
    }

单实例锁：复用 fetch_all 的 PID 锁（同机只有一个 worker 抢浏览器）。
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


def save_state(input_date, processed_index, current_recom_index, shards=None):
    """写断点状态（input_date 为当前 input 日期；shards 为各类型分片进度）。"""
    _atomic_write_json(config.STATE_FILE, {
        "input_date": input_date,
        "processed_index": int(processed_index),
        "current_recom_index": int(current_recom_index),
        "shards": shards or {},
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    })


def reset_state_for_new_input(input_date):
    """切到新的 input 日期时归零进度与分片。"""
    save_state(input_date, 0, 0, shards={})


def acquire_lock():
    return acquire_single_instance(config.LOCK_FILE)


def release_lock():
    release_single_instance(config.LOCK_FILE)


def _set_snap(progress, **kw):
    if progress is None:
        return
    progress["snap"] = {**progress.get("snap", {}), **kw,
                        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
