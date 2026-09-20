# -*- coding: utf-8 -*-
"""OSS 输入文件扫描 / 下载 / 解析。

输入源约定（bucket 根目录下）:
    content-opt-pool/
    ├── <taskId>/
    │   ├── <storeId>.csv     ← 文件名 = WB 数字卖家 ID
    │   └── ...
    └── ...

CSV 只有一列 nm_id，每行一个 nmID。

本模块只负责"读"，不做任何写 OSS 操作；写 OSS 复用 wb_to_oss.convert_and_upload_shard。
"""
import csv
import os
import sys

# 复用 wb_to_oss 的 bucket 构造与配置读取，保证凭证/endpoint 单一来源
from wb_to_oss import DEFAULT_CONFIG, oss_target

# 输入根目录（bucket 根下，与输出 prefix 无关）
INPUT_PREFIX = "content-opt-pool/"


def get_bucket(config_path=DEFAULT_CONFIG):
    """读取 oss_config.json 构造 bucket。返回 (bucket, 错误信息)。"""
    bucket, _, err = oss_target(config_path=config_path, oss_segment=None)
    return bucket, err


def list_task_ids(bucket=None):
    """列出 content-opt-pool/ 下所有 taskId 目录名（不含尾斜杠）。

    用 delimiter='/' 只列一级子目录，避免递归扫全量对象。
    """
    if bucket is None:
        bucket, err = get_bucket()
        if err:
            print(f"[OSS-INPUT] 获取 bucket 失败: {err}")
            return []
    import oss2
    task_ids = []
    for obj in oss2.ObjectIterator(bucket, prefix=INPUT_PREFIX, delimiter="/"):
        if obj.is_prefix():
            # obj.key 形如 content-opt-pool/task_001/，取中间段
            rel = obj.key[len(INPUT_PREFIX):].strip("/")
            if rel:
                task_ids.append(rel)
    return sorted(task_ids)


def list_store_csvs(task_id, bucket=None):
    """列出某 taskId 下所有 *.csv 的 OSS key。返回 [(oss_key, store_id), ...]。

    store_id = 文件名去掉 .csv 后缀（WB 数字卖家 ID）。
    """
    if bucket is None:
        bucket, err = get_bucket()
        if err:
            print(f"[OSS-INPUT] 获取 bucket 失败: {err}")
            return []
    import oss2
    prefix = f"{INPUT_PREFIX}{task_id}/"
    results = []
    for obj in oss2.ObjectIterator(bucket, prefix=prefix):
        if obj.is_prefix():
            continue
        name = os.path.basename(obj.key)
        if name.endswith(".csv"):
            store_id = name[: -len(".csv")]
            results.append((obj.key, store_id))
    return sorted(results)


def download_csv(oss_key, local_path, bucket=None):
    """下载 CSV 到本地路径。返回 (True, None) 或 (False, 错误信息)。"""
    if bucket is None:
        bucket, err = get_bucket()
        if err:
            return False, err
    try:
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        bucket.get_object_to_file(oss_key, local_path)
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def parse_nmids(csv_path):
    """解析 CSV 的 nm_id 列，返回去重后的 nmID 字符串列表（保持原顺序）。

    兼容：首行表头 nm_id / 无表头纯数字 / 前后空白 / 空行。
    """
    nmids = []
    seen = set()
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            val = row[0].strip()
            if not val:
                continue
            # 跳过表头
            if val.lower() in ("nm_id", "nmid", "nm id"):
                continue
            if val not in seen:
                seen.add(val)
                nmids.append(val)
    return nmids


def has_pending_tasks(bucket=None):
    """检查 content-opt-pool/ 下是否有待处理数据（供协调器 / /status 调用）。

    只要存在任意 taskId 目录即认为有数据。
    """
    return len(list_task_ids(bucket)) > 0


if __name__ == "__main__":
    # 冒烟测试：python -m nmid_fetch.oss_input
    b, e = get_bucket()
    if e:
        print(f"[OSS-INPUT] bucket 错误: {e}")
        sys.exit(1)
    tids = list_task_ids(b)
    print(f"[OSS-INPUT] taskId 列表: {tids}")
    for tid in tids[:3]:
        csvs = list_store_csvs(tid, b)
        print(f"[OSS-INPUT]   {tid}: {[(k, s) for k, s in csvs]}")
