# -*- coding: utf-8 -*-
"""诊断 fetch_all 异步上传线程失败原因：手动执行一次转换+上传并打印完整 traceback。"""
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

RUN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tableListv6_20260901_191657")
SHARD = os.path.join(RUN_DIR, "tableListv6_shard_231_20260901_191657.json")

print("1) 测试 oss_target ...")
from wb_to_oss import oss_target, convert_and_upload_shard
bucket, base_key, err = oss_target("20260901_191657")
print(f"   oss_err={err}")
print(f"   bucket={bucket}")
print(f"   base_key={base_key}")
if err:
    sys.exit(1)

print("2) 测试 convert_and_upload_shard ...")
try:
    result = convert_and_upload_shard(RUN_DIR, SHARD, bucket, base_key)
    print(f"   返回值: {result!r}  (None=成功)")
except Exception:
    traceback.print_exc()

print("3) 完成")
