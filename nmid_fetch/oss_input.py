# -*- coding: utf-8 -*-
"""OSS 输入文件扫描 / 下载 / 解析。

输入源约定（bucket 根目录下）:
    content-opt-pool/
    ├── <taskId>/
    │   ├── <sellerId>_treatment_<NNN>.csv   ← 实验组，编号 001/002/...
    │   ├── <sellerId>_control.csv           ← 对照组，每轮只跑最新的一份
    │   └── ...
    └── ...

CSV 只有一列 nm_id，每行一个 nmID。

本模块只负责"读"，不做任何写 OSS 操作；写 OSS 复用 wb_to_oss.convert_and_upload_shard。

当前账号规则（由 plan_round 统一编排，宿主只按它跑）:
    - treatment 只取编号 001（treatment_001.csv），每轮重复跑；
    - control 每个卖家只取 last_modified 最新的一份，跑过记账不再重复；
    - 其余（treatment_002+、旧命名 <sellerId>.csv）一律不跑。
"""
import csv
import os
import re
import sys

# 复用 wb_to_oss 的 bucket 构造与配置读取，保证凭证/endpoint 单一来源
from wb_to_oss import DEFAULT_CONFIG, oss_target

# 输入根目录（bucket 根下，与输出 prefix 无关）
INPUT_PREFIX = "content-opt-pool/"

# 只跑这个实验组编号；其余 treatment_NNN 忽略
TREATMENT_INDEX = 1

# 文件名解析（已去 .csv 后缀）：<sellerId>_treatment_<NNN> / <sellerId>_control / 旧命名 <sellerId>
_RE_TREATMENT = re.compile(r"^(\d+)_treatment_(\d+)$")
_RE_CONTROL = re.compile(r"^(\d+)_control$")
_RE_LEGACY = re.compile(r"^(\d+)$")


def variant_tag(variant, index=None):
    """变体 → 目录/文件名标记: treatment 编号 1 保持原样(兼容历史产物)，其余带上编号。"""
    if variant == "treatment":
        return "treatment" if index in (None, TREATMENT_INDEX) else f"treatment_{index:03d}"
    return variant or "unknown"


def parse_csv_name(name):
    """解析输入 CSV 文件名。

    返回 (seller_id, variant, index)；variant ∈ {"treatment", "control", "legacy"}，
    index 仅 treatment 有意义，其余为 None。非 CSV 返回 None。
    """
    if not name.endswith(".csv"):
        return None
    stem = name[: -len(".csv")]
    m = _RE_TREATMENT.match(stem)
    if m:
        return m.group(1), "treatment", int(m.group(2))
    m = _RE_CONTROL.match(stem)
    if m:
        return m.group(1), "control", None
    m = _RE_LEGACY.match(stem)
    if m:
        return m.group(1), "legacy", None
    return stem, "unknown", None


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


def list_input_csvs(task_id, bucket=None):
    """列出某 taskId 下所有输入 CSV 的结构化信息。

    返回 [{key, name, seller_id, variant, index, size, last_modified}, ...]，按 key 排序。
    last_modified 为 OSS 对象的 Unix 秒（int），用于挑"最新"的 control。
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
        parsed = parse_csv_name(name)
        if parsed is None:
            continue
        seller_id, variant, index = parsed
        results.append({
            "key": obj.key,
            "name": name,
            "seller_id": seller_id,
            "variant": variant,
            "index": index,
            "size": getattr(obj, "size", 0) or 0,
            "last_modified": int(getattr(obj, "last_modified", 0) or 0),
        })
    results.sort(key=lambda r: r["key"])
    return results


def plan_round(task_ids=None, bucket=None, sellers=None):
    """按当前账号规则编排一轮要跑的输入文件。

    sellers: 只编排这些卖家 ID（str）；缺省 None = 不限制，由调用方自行过滤未配置的店。

    返回 (jobs, unknown_names):
        jobs = [{task_id, key, name, seller_id, variant, index, last_modified}, ...]
               treatment_001 每个 taskId 一份（每轮重复跑）；
               control 按卖家各取 last_modified 最新的一份（跑过由调用方查台账跳过）。
        unknown_names = 文件名不符合任何已知命名的 CSV 名字集合（供一次性告警）。
    """
    if bucket is None:
        bucket, err = get_bucket()
        if err:
            print(f"[OSS-INPUT] 获取 bucket 失败: {err}")
            return [], set()
    if task_ids is None:
        task_ids = list_task_ids(bucket)
    want = None if sellers is None else {str(s) for s in sellers}
    treatments, controls, unknown = [], [], set()
    for tid in task_ids:
        for r in list_input_csvs(tid, bucket):
            if r["variant"] == "unknown":
                unknown.add(r["name"])
                continue
            if want is not None and r["seller_id"] not in want:
                continue
            if r["variant"] == "treatment" and r["index"] == TREATMENT_INDEX:
                treatments.append(r)
            elif r["variant"] == "control":
                controls.append(r)
    jobs = [dict(r, task_id=tid_of_key(r["key"])) for r in treatments]
    for latest in latest_control_per_seller(controls).values():
        jobs.append(dict(latest, task_id=tid_of_key(latest["key"])))
    return jobs, unknown


def tid_of_key(oss_key):
    """从 OSS key 反推 taskId：content-opt-pool/<taskId>/<name>.csv。"""
    parts = oss_key.split("/")
    return parts[-2] if len(parts) >= 2 else ""


def latest_control_per_seller(controls=None, task_ids=None, bucket=None):
    """每个卖家各挑出 last_modified 最新的一份 control。返回 {seller_id: row}。"""
    if controls is None:
        if bucket is None:
            bucket, err = get_bucket()
            if err:
                print(f"[OSS-INPUT] 获取 bucket 失败: {err}")
                return {}
        if task_ids is None:
            task_ids = list_task_ids(bucket)
        controls = [r for tid in task_ids for r in list_input_csvs(tid, bucket)
                    if r["variant"] == "control"]
    latest = {}
    for r in controls:
        cur = latest.get(r["seller_id"])
        if cur is None or (r["last_modified"], r["key"]) > (cur["last_modified"], cur["key"]):
            latest[r["seller_id"]] = r
    return latest


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


def pending_task_count(task_ids=None, sellers=None, bucket=None):
    """按当前规则统计待处理 taskId 数（供 /status 与协调器判断是否切换模块）。"""
    jobs, _ = plan_round(task_ids=task_ids, bucket=bucket, sellers=sellers)
    return len({j["task_id"] for j in jobs})


def has_pending_tasks(task_ids=None, sellers=None, bucket=None):
    """检查按当前规则是否还有待处理数据（供协调器 / /status 调用）。"""
    return pending_task_count(task_ids=task_ids, sellers=sellers, bucket=bucket) > 0


if __name__ == "__main__":
    # 冒烟测试：python -m nmid_fetch.oss_input
    b, e = get_bucket()
    if e:
        print(f"[OSS-INPUT] bucket 错误: {e}")
        sys.exit(1)
    tids = list_task_ids(b)
    print(f"[OSS-INPUT] taskId 列表: {tids}")
    for tid in tids:
        rows = list_input_csvs(tid, b)
        print(f"[OSS-INPUT]   {tid}: {len(rows)} 个输入文件")
        for r in rows:
            print(f"[OSS-INPUT]     {r['name']:<40} variant={r['variant']:<10} "
                  f"index={r['index']} mtime={r['last_modified']} size={r['size']}")
    _jobs, _unknown = plan_round(bucket=b)
    print(f"[OSS-INPUT] 本轮计划跑 {len(_jobs)} 个文件（未按 config.json 过滤店铺）:")
    for j in _jobs:
        print(f"[OSS-INPUT]   {j['task_id']} / {j['name']} ({j['variant']})")
    if _unknown:
        print(f"[OSS-INPUT] 无法解析的文件名: {sorted(_unknown)}")
