# -*- coding: utf-8 -*-
"""一次性运维脚本：手动把 wb_querywords 本地分片合并并上传 OSS。

用途：批处理被限流中断、未触发「延迟统一上传」时，手动补传当天 input 日期的分片。
只做模块本就有的 merge_shards + upload_merged（不改模块代码），成功后删临时分片、保留合并文件。
"""
import glob
import json
import os

from wb_querywords import config, csv_io, oss_uploader


def _state_date():
    try:
        with open(config.STATE_FILE, encoding="utf-8") as f:
            return (json.load(f) or {}).get("input_date")
    except Exception:
        return None


def main():
    date = _state_date()
    if not date:
        print("[X] state.json 无 input_date，无法确定要上传的日期")
        return 1
    print(f"[INFO] input_date={date}  recom_types={config.RECOM_TYPES}")

    bucket = oss_uploader.get_bucket()
    print("[OSS] bucket 就绪")

    any_fail = False
    for recom in config.RECOM_TYPES:
        shard_files = sorted(glob.glob(csv_io.shard_glob(recom, date)))
        merged = csv_io.merge_shards(recom, date)
        if not merged:
            print(f"[SKIP] {recom}/{date}: 无分片可合并")
            continue
        size = os.path.getsize(merged)
        with open(merged, encoding="utf-8-sig") as f:
            rows = max(0, sum(1 for _ in f) - 1)   # 去表头
        ok, info = oss_uploader.upload_merged(bucket, recom, date, merged)
        tag = "OK" if ok else "X"
        print(f"[{tag}] {recom}/{date}.csv  shards={len(shard_files)} rows={rows} bytes={size} -> {info}")
        if ok:
            for sp in shard_files:                  # 与 worker._delete_shards 一致：只删临时分片，保留合并文件
                try:
                    os.remove(sp)
                except OSError:
                    pass
            print(f"      已删除 {len(shard_files)} 个临时分片，保留合并文件 {merged}")
        else:
            any_fail = True
    return 2 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
