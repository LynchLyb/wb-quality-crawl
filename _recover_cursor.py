# -*- coding: utf-8 -*-
"""一次性恢复脚本：state.json 的 last_cursor 被覆盖成 null 后，
从 OSS 上的最后一个分片（shard_340）找回游标并写回 state.json。"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from wb_to_oss import oss_key_for, oss_target

RUN_TS = "20260901_191657"
RUN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), f"tableListv6_{RUN_TS}")
SHARD_NAME = "tableListv6_shard_340_20260901_191657.json"
OSS_DAY = None   # 该分片上传当天(北京时间)的日期目录，如 "2026-09-08"；None=今天

print("1) 构造 OSS 目标...")
bucket, prefix, err = oss_target()
if err:
    print(f"[ERROR] oss_target 失败: {err}")
    sys.exit(1)
key = oss_key_for(prefix, SHARD_NAME, day=OSS_DAY)
print(f"   key = {key}")

local_path = os.path.join(RUN_DIR, SHARD_NAME)
if os.path.exists(local_path):
    print("   本地已存在该分片，跳过下载")
else:
    print("2) 从 OSS 下载分片...")
    bucket.get_object_to_file(key, local_path)
    print(f"   已下载到 {local_path}")

print("3) 解析最后一条响应的游标...")
with open(local_path, encoding="utf-8") as f:
    bodies = json.load(f)
cursor = (bodies[-1].get("data") or {}).get("cursor")
print(f"   分片含 {len(bodies)} 条响应, 最后游标 = {cursor}")
if not cursor:
    print("[ERROR] 游标为空，停止")
    sys.exit(1)

print("4) 写回 state.json...")
state_path = os.path.join(RUN_DIR, "state.json")
with open(state_path, encoding="utf-8") as f:
    st = json.load(f)
st["last_cursor"] = cursor
with open(state_path, "w", encoding="utf-8") as f:
    json.dump(st, f, ensure_ascii=False, indent=2)
print("   完成:", json.dumps(st, ensure_ascii=False))
