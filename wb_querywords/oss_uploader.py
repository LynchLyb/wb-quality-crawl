# -*- coding: utf-8 -*-
"""OSS 输入下载 + 输出合并文件上传（复用 wb_to_oss 的凭证与上传实现）。

约定:
    - 输入源在 OSS：列举 wb_querywords/input/ 下的 YYYYMMDD.csv，取最大日期下载到本地 input/。
    - 输出在 OSS：某类型某 input 日期的临时分片合并成单文件，上传到
      wb_querywords/output/<recomType>/<inputDate>.csv（类型优先分目录，文件名=input日期）。
    - 台账 uploaded.json 记 <recomType>/<date>.csv -> {key,size,...}，防重启重传。
    - 每次上传的 key 由「类型 + input 日期」实时决定（一对一），不落固定日期目录。
"""
import json
import os
import re

import oss2
from wb_to_oss import make_bucket, upload_one

from . import config

_DATE_RE = re.compile(r"(\d{8})\.csv$")


def _load_cfg():
    if not os.path.exists(config.OSS_CONFIG_FILE):
        raise FileNotFoundError(
            f"缺少 OSS 配置文件 {config.OSS_CONFIG_FILE}（复制 oss_config.json.example 为 oss_config.json）")
    with open(config.OSS_CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def get_bucket():
    """按 oss_config.json 建 bucket 句柄（调用方持有复用，勿每片重建）。"""
    return make_bucket(_load_cfg())


# ------------------------------------------------------------ 输入：列举 + 下载
def list_input_dates(bucket):
    """列举 OSS input 前缀下所有 YYYYMMDD.csv 的日期，升序返回；无则空列表。"""
    dates = set()
    for obj in oss2.ObjectIterator(bucket, prefix=config.OSS_INPUT_PREFIX):
        if obj.is_prefix():
            continue
        m = _DATE_RE.search(obj.key)
        if m:
            dates.add(m.group(1))
    return sorted(dates)


def max_input_date(bucket):
    """当前 OSS input 里最大日期字符串（YYYYMMDD）；无文件返回 None。"""
    dates = list_input_dates(bucket)
    return dates[-1] if dates else None


def download_input(bucket, date_str):
    """下载 wb_querywords/input/<date>.csv 到本地 input/<date>.csv，返回本地路径。"""
    key = f"{config.OSS_INPUT_PREFIX}{date_str}.csv"
    local = config.local_input_path(date_str)
    os.makedirs(os.path.dirname(local), exist_ok=True)
    bucket.get_object_to_file(key, local)
    return local


# ------------------------------------------------------------ 输出：合并文件上传 + 台账
def load_manifest():
    if os.path.exists(config.UPLOAD_MANIFEST):
        try:
            with open(config.UPLOAD_MANIFEST, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}


def _save_manifest(m):
    tmp = config.UPLOAD_MANIFEST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(m, f, ensure_ascii=False, indent=2)
    os.replace(tmp, config.UPLOAD_MANIFEST)


def _ledger_name(recom_type, date_str):
    """台账键：类型/日期.csv（跨类型不撞名）。"""
    return f"{recom_type}/{config.merged_filename(date_str)}"


def is_uploaded(name, size):
    prev = load_manifest().get(name)
    return bool(prev) and prev.get("size") == size


def upload_merged(bucket, recom_type, date_str, local_merged_path):
    """上传合并文件到 wb_querywords/output/<recomType>/<date>.csv。

    返回 (ok, info)：已传同尺寸则 skip；失败不记台账（下次重试）。
    """
    if not local_merged_path or not os.path.exists(local_merged_path):
        return False, "merged_missing"
    name = _ledger_name(recom_type, date_str)
    size = os.path.getsize(local_merged_path)
    if is_uploaded(name, size):
        return True, f"skip:{config.oss_output_key(recom_type, date_str)}"
    key = config.oss_output_key(recom_type, date_str)
    ok = upload_one(bucket, key, local_merged_path)
    if ok:
        m = load_manifest()
        m[name] = {"key": key, "size": size, "date": date_str}
        _save_manifest(m)
        return True, key
    return False, f"upload_failed:{key}"
