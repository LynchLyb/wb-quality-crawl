# -*- coding: utf-8 -*-
"""
tableListv6 分片 JSON -> CSV 转换 + 上传阿里云 OSS。

约定:
    - 每个 tableListv6_shard_NNN_*.json 转成同名 .csv（csv/ 子目录），与分片一一对应
    - 每个商品(nmID)一行，32 列，表头与对齐模板
      (Downloads/tableListv6_shard_001_20260901_191657.xlsx) 完全一致
    - CSV 用 UTF-8-BOM 编码，Excel/WPS 直接打开不乱码
    - OSS 路径: {prefix}{json|csv}/{上传当天北京时间日期}/{文件名}
      JSON 与 CSV 分开两个目录，类型目录在日期之前；
      日期按每次上传时的北京时间(UTC+8)当天实时计算（跨天自动切换），不再取 run_ts

用法:
    python wb_to_oss.py                                   # 最新运行目录: 转换 + 上传
    python wb_to_oss.py --dir tableListv6_20260901_191657
    python wb_to_oss.py --convert-only / --upload-only
    python wb_to_oss.py --limit 3                         # 只处理前 N 个分片（冒烟测试）
"""
import argparse
import csv
import glob
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CONFIG = os.path.join(BASE_DIR, "oss_config.json")
BJT = timezone(timedelta(hours=8))
MULTIPART_THRESHOLD = 8 * 1024 * 1024   # 超过 8MB 的文件用分片上传
EXPORT_STATE = "export_state.json"

# export_state.json 的读改写锁：fetch_all 的异步线程与本脚本（do_convert/do_upload）共用
# 写入侧统一在 save_export_state 内部加锁；用 RLock 允许外层 RMW 块重入
_STATE_LOCK = threading.RLock()

# 拍平后的 32 列，顺序与模板 xlsx 一致，不得增删改
CANONICAL_COLS = [
    "nmID", "imtID", "vendorCode", "title", "brand", "brandRaw", "subject", "colors",
    "updateAt", "stocks", "discount", "discountSource", "feedbackRating", "feedbacks",
    "externalBan", "sizes_count", "sizes_detail", "mediaFiles_count", "tags",
    "meta_needKiz", "meta_needUIN", "rating", "isCardRated",
    "err_characteristics", "err_title", "err_description", "err_image", "err_brand",
    "hasPaidOptions_hasPhotoTags", "hasPaidOptions_hasRichContent",
    "hasPaidOptions_hasAutoplayVideo", "hasPaidOptions_hasClientTryOn",
]

ERR_COLS = ("err_characteristics", "err_title", "err_description", "err_image", "err_brand")


def fmt(v):
    """CSV 单元格格式化: None -> 空，bool/数字 -> Python str 形式（True/False、8.3）。"""
    if v is None:
        return ""
    return str(v)


def flatten_card(card):
    """一条商品卡片 -> 一行 dict（32 列）。"""
    row = {}
    row["nmID"] = card.get("nmID")
    row["imtID"] = card.get("imtID")
    row["vendorCode"] = card.get("vendorCode")
    row["title"] = card.get("title")
    row["brand"] = card.get("brand")
    row["brandRaw"] = card.get("brandRaw")
    row["subject"] = card.get("subject")
    row["colors"] = ", ".join(str(c) for c in card.get("colors") or [])
    row["updateAt"] = card.get("updateAt")
    row["stocks"] = card.get("stocks")
    row["discount"] = card.get("discount")
    row["discountSource"] = card.get("discountSource")
    row["feedbackRating"] = card.get("feedbackRating")
    if card.get("feedbacks"):
        row["feedbacks"] = json.dumps(card["feedbacks"], ensure_ascii=False)
    else:
        row["feedbacks"] = ""
    if card.get("externalBan"):
        row["externalBan"] = json.dumps(card["externalBan"], ensure_ascii=False)
    else:
        row["externalBan"] = ""

    sizes = card.get("sizes") or []
    row["sizes_count"] = len(sizes)
    parts = []
    for s in sizes:
        tech = str(s.get("techSize") or s.get("wbSize") or "")
        price = s.get("currentPrice")
        skus = ",".join(str(x) for x in s.get("skus") or [])
        parts.append(f"{tech}(price={'' if price is None else price},skus={skus})")
    row["sizes_detail"] = "; ".join(parts)

    row["mediaFiles_count"] = len(card.get("mediaFiles") or {})
    row["tags"] = ", ".join(str(t) for t in card.get("tags") or [])

    meta = card.get("meta") or {}
    row["meta_needKiz"] = meta.get("needKiz")
    row["meta_needUIN"] = meta.get("needUIN")
    rd = meta.get("ratingData") or {}
    row["rating"] = rd.get("rating")
    row["isCardRated"] = rd.get("isCardRated")
    for e in rd.get("errors") or []:
        col = f"err_{e.get('field') or ''}"
        if col in ERR_COLS:
            row[col] = ", ".join(str(d) for d in e.get("details") or [])
    for col in ERR_COLS:
        row.setdefault(col, "")

    hp = card.get("hasPaidOptions") or {}
    for k in ("hasPhotoTags", "hasRichContent", "hasAutoplayVideo", "hasClientTryOn"):
        row[f"hasPaidOptions_{k}"] = hp.get(k)
    return row


def load_export_state(run_dir):
    path = os.path.join(run_dir, EXPORT_STATE)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_export_state(run_dir, state):
    """加锁 + 临时文件原子替换写盘，避免并发写或进程被杀留下半截 JSON。"""
    path = os.path.join(run_dir, EXPORT_STATE)
    tmp = path + ".tmp"
    with _STATE_LOCK:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)


# ---------------------------------------------------------------- 转换

def convert_shard(json_path, csv_path):
    """一个分片 JSON -> 一个 CSV。返回 (行数, nmID 数, 去重数, 错误响应数)。"""
    with open(json_path, encoding="utf-8") as f:
        responses = json.load(f)

    rows, err_responses = [], 0
    for resp in responses:
        payload = resp.get("data") if isinstance(resp, dict) else None
        if not isinstance(payload, dict):
            err_responses += 1
            continue
        cards = payload.get("cards") or payload.get("list") or []
        for card in cards:
            rows.append(flatten_card(card))

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CANONICAL_COLS)
        for row in rows:
            writer.writerow([fmt(row.get(c)) for c in CANONICAL_COLS])

    nmids = [r.get("nmID") for r in rows if r.get("nmID") is not None]
    return len(rows), len(nmids), len(set(nmids)), err_responses


def do_convert(run_dir, limit=None):
    shards = sorted(glob.glob(os.path.join(run_dir, "tableListv6_shard_*.json")))
    if not shards:
        print(f"[ERROR] {run_dir} 下没有分片文件")
        return False
    if limit:
        shards = shards[:limit]

    csv_dir = os.path.join(run_dir, "csv")
    os.makedirs(csv_dir, exist_ok=True)
    state = load_export_state(run_dir)
    converted = state.setdefault("converted", {})

    t0 = time.time()
    total_rows = total_dedup = 0
    for i, json_path in enumerate(shards, 1):
        name = os.path.basename(json_path)
        csv_path = os.path.join(csv_dir, os.path.splitext(name)[0] + ".csv")
        src_stat = os.stat(json_path)
        prev = converted.get(name)
        if prev and prev.get("source_size") == src_stat.st_size and os.path.exists(csv_path):
            print(f"[{i}/{len(shards)}] 跳过（已转换且源文件未变）: {name}")
            total_rows += prev.get("rows", 0)
            total_dedup += prev.get("dedup", 0)
            continue
        rows, nmid, dedup, err_n = convert_shard(json_path, csv_path)
        converted[name] = {
            "csv": os.path.basename(csv_path), "rows": rows, "nmid": nmid,
            "dedup": dedup, "error_responses": err_n, "source_size": src_stat.st_size,
        }
        total_rows += rows
        total_dedup += dedup
        print(f"[{i}/{len(shards)}] {name} -> {os.path.basename(csv_path)}: "
              f"{rows} 行 (nmID 去重 {dedup}{f'，错误响应 {err_n}' if err_n else ''})")
        if i % 20 == 0:
            save_export_state(run_dir, state)

    save_export_state(run_dir, state)
    print(f"\n[CONVERT] 完成 {len(shards)} 个分片，共 {total_rows} 行，"
          f"nmID 去重合计 {total_dedup}，耗时 {time.time() - t0:.0f}s")
    return True


# ---------------------------------------------------------------- 上传

def oss_today():
    """上传当天（北京时间 UTC+8）的日期目录名，如 '2026-09-08'。

    每次上传实时计算：跨天长跑时新分片自动落到新一天的目录，实现"按上传日归档"。
    历史对象留在其原日期目录，不迁移。
    """
    return datetime.now(BJT).strftime("%Y-%m-%d")


def make_bucket(cfg):
    import oss2
    if cfg.get("sts_token"):
        auth = oss2.StsAuth(cfg["access_key_id"], cfg["access_key_secret"], cfg["sts_token"])
    else:
        auth = oss2.Auth(cfg["access_key_id"], cfg["access_key_secret"])
    endpoint = cfg["endpoint"] if "://" in cfg["endpoint"] else "https://" + cfg["endpoint"]
    return oss2.Bucket(auth, endpoint.rstrip("/"), cfg["bucket"])


def progress_cb(fname):
    last = {"pct": -1}

    def cb(consumed, total):
        pct = int(consumed * 100 / total) if total else 100
        if pct >= last["pct"] + 20 or pct == 100:
            last["pct"] = pct
            print(f"        {fname}: {pct}%")
    return cb


def upload_one(bucket, key, path):
    import oss2
    size = os.path.getsize(path)
    for attempt in range(1, 4):
        try:
            if size >= MULTIPART_THRESHOLD:
                oss2.resumable_upload(bucket, key, path, multipart_threshold=MULTIPART_THRESHOLD,
                                      part_size=8 * 1024 * 1024, num_threads=3,
                                      progress_callback=progress_cb(os.path.basename(path)))
            else:
                bucket.put_object_from_file(key, path)
            return True
        except Exception as e:
            print(f"[WARN] {os.path.basename(path)} 第 {attempt} 次上传失败: "
                  f"{type(e).__name__}: {e}")
            time.sleep(10 * attempt)
    return False


# （读改写锁 _STATE_LOCK 已移至文件头部常量区，写入侧统一收口在 save_export_state 内）


def _full_prefix(base_prefix, oss_segment=None):
    """base prefix 规整为以 / 结尾；oss_segment 非空则追加店铺段（多店区分文件）。"""
    prefix = base_prefix or ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    if oss_segment:
        prefix += oss_segment.strip("/") + "/"
    return prefix


def oss_target(config_path=DEFAULT_CONFIG, oss_segment=None):
    """读取配置，构造 bucket 与 OSS prefix。返回 (bucket, prefix, 错误信息)。

    日期目录不再由 run_ts 决定，而是在 oss_key_for 内按"上传当天(北京时间)"实时计算；
    最终 key = {prefix}{json|csv}/{当天BJT日期}/{文件名}。
    oss_segment 非空时插入店铺段：prefix = base_prefix + oss_segment + "/"。
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = json.load(f)
        for k in ("access_key_id", "access_key_secret", "endpoint", "bucket"):
            if not cfg.get(k):
                return None, None, f"oss_config.json 缺少 {k}"
        bucket = make_bucket(cfg)
        prefix = _full_prefix(cfg.get("prefix") or "", oss_segment)
        return bucket, prefix, None
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"


def oss_key_for(prefix, filename, day=None):
    """JSON 与 CSV 各自落到独立目录，类型目录在日期之前，日期=上传当天(北京时间)：
    {prefix}{json|csv}/{当天BJT日期}/{文件名}

    day 可显式指定日期目录（补传/恢复到某历史天）；缺省用 oss_today() 实时计算。
    """
    subdir = "json" if filename.endswith(".json") else "csv"
    return f"{prefix}{subdir}/{day or oss_today()}/{filename}"


def convert_and_upload_shard(run_dir, json_path, bucket=None, prefix=None):
    """转换单个分片并上传 JSON+CSV，成功项记录进 export_state.json。

    fetch_all 归档分片后开异步线程调用（bucket/prefix 为 None 时只转换不上传）；
    补传走 --upload-only 时通过 manifest 跳过已传文件，二者不会重复上传。
    日期目录按上传当天(北京时间)实时算；已在 manifest 的文件不因跨天换目录而重传。
    返回 None 表示成功，否则返回错误描述。
    """
    name = os.path.basename(json_path)
    csv_name = os.path.splitext(name)[0] + ".csv"
    csv_path = os.path.join(run_dir, "csv", csv_name)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    src_size = os.path.getsize(json_path)
    with _STATE_LOCK:
        state = load_export_state(run_dir)
    prev = (state.get("converted") or {}).get(name)
    if not (prev and prev.get("source_size") == src_size and os.path.exists(csv_path)):
        try:
            rows, nmid, dedup, err_n = convert_shard(json_path, csv_path)
        except Exception as e:
            return f"转换失败: {name}: {type(e).__name__}: {e}"
        with _STATE_LOCK:
            state = load_export_state(run_dir)
            state.setdefault("converted", {})[name] = {
                "csv": csv_name, "rows": rows, "nmid": nmid, "dedup": dedup,
                "error_responses": err_n, "source_size": src_size,
            }
            save_export_state(run_dir, state)
        print(f"[OSS] {name} -> {csv_name}: {rows} 行")

    if bucket is None or prefix is None:
        return None
    day = oss_today()   # 本分片 json+csv 用同一天目录，避免跨零点分裂
    for path in (json_path, csv_path):
        fname = os.path.basename(path)
        key = oss_key_for(prefix, fname, day)
        size = os.path.getsize(path)
        with _STATE_LOCK:
            state = load_export_state(run_dir)
        prev_up = (state.get("uploaded") or {}).get(fname)
        if prev_up and prev_up.get("size") == size:
            continue   # 已上传过（可能落在更早日期目录），不重复上传
        try:
            ok = upload_one(bucket, key, path)
        except Exception as e:
            print(f"[OSS] 上传异常 {fname}: {type(e).__name__}: {e}")
            ok = False
        if ok:
            with _STATE_LOCK:
                state = load_export_state(run_dir)
                state.setdefault("uploaded", {})[fname] = {
                    "key": key, "size": size,
                    "uploaded_at": datetime.now(BJT).isoformat(timespec="seconds"),
                }
                save_export_state(run_dir, state)
            print(f"[OSS] 已上传: {key}")
        else:
            return f"上传失败: {fname}（补传脚本可重试）"
    return None


def do_upload(run_dir, config_path, limit=None, oss_segment=None):
    state = load_export_state(run_dir)
    run_ts = state.get("run_ts") or os.path.basename(run_dir).replace("tableListv6_", "")
    day = oss_today()   # 整批用同一天目录（本次上传当天，北京时间）
    state["run_ts"], state["oss_date"] = run_ts, day

    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    for k in ("access_key_id", "access_key_secret", "endpoint", "bucket"):
        if not cfg.get(k):
            print(f"[ERROR] oss_config.json 缺少 {k}")
            return False
    prefix = _full_prefix(cfg.get("prefix") or "", oss_segment)

    bucket = make_bucket(cfg)
    pairs = []
    for p in sorted(glob.glob(os.path.join(run_dir, "tableListv6_shard_*.json"))):
        pairs.append((p, oss_key_for(prefix, os.path.basename(p), day)))
    csv_dir = os.path.join(run_dir, "csv")
    for p in sorted(glob.glob(os.path.join(csv_dir, "*.csv"))):
        pairs.append((p, oss_key_for(prefix, os.path.basename(p), day)))
    if not pairs:
        print("[ERROR] 没有可上传的文件（JSON 分片或 CSV 均为空）")
        return False
    if limit:
        pairs = pairs[:limit]

    uploaded = state.setdefault("uploaded", {})
    print(f"[UPLOAD] 目标: bucket={cfg['bucket']}  "
          f"key: {prefix}{{json|csv}}/{day}/文件名")
    print(f"[UPLOAD] 待上传 {len(pairs)} 个文件，共 "
          f"{sum(os.path.getsize(p) for p, _ in pairs) / 1024 / 1024:.1f} MB\n")

    t0 = time.time()
    ok = fail = skipped = 0
    for i, (path, key) in enumerate(pairs, 1):
        name = os.path.basename(path)
        size = os.path.getsize(path)
        prev = uploaded.get(name)
        if prev and prev.get("size") == size:
            skipped += 1
            continue   # 已上传过（可能在更早日期目录），不重复上传
        print(f"[{i}/{len(pairs)}] {name} ({size / 1024 / 1024:.1f} MB) -> {key}")
        if upload_one(bucket, key, path):
            uploaded[name] = {"key": key, "size": size,
                              "uploaded_at": datetime.now(BJT).isoformat(timespec="seconds")}
            ok += 1
            if i % 5 == 0 or i == len(pairs):
                save_export_state(run_dir, state)
        else:
            fail += 1
            save_export_state(run_dir, state)

    save_export_state(run_dir, state)
    print(f"\n[UPLOAD] 成功 {ok}，跳过(已传) {skipped}，失败 {fail}，耗时 {time.time() - t0:.0f}s")
    if fail:
        print("[提示] 失败文件未写入 manifest，重跑本脚本即可只重传失败项")
        return False
    return True


# ---------------------------------------------------------------- 主流程

def find_latest_dir(base=BASE_DIR):
    if not os.path.isdir(base):
        return None
    dirs = sorted(
        (d for d in os.listdir(base)
         if d.startswith("tableListv6_2") and os.path.isdir(os.path.join(base, d))),
        reverse=True,
    )
    return os.path.join(base, dirs[0]) if dirs else None


def main():
    ap = argparse.ArgumentParser(description="tableListv6 分片转 CSV 并上传 OSS")
    ap.add_argument("--dir", help="运行目录（默认取该店 data_dir 下最新的 tableListv6_*）")
    ap.add_argument("--store", help="店铺 id（从 config.STORES 解析 data_dir 与 oss_segment）")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="OSS 配置文件路径")
    ap.add_argument("--convert-only", action="store_true", help="只转换不上传")
    ap.add_argument("--upload-only", action="store_true", help="只上传不转换")
    ap.add_argument("--limit", type=int, help="只处理前 N 个分片（测试用）")
    args = ap.parse_args()

    base, oss_segment = BASE_DIR, None
    if args.store:
        import config
        st = config.get_store(args.store)
        if st is None:
            print(f"[ERROR] 未知店铺 id: {args.store}（可选: {', '.join(s['id'] for s in config.STORES)}）")
            return 2
        base, oss_segment = st["data_dir"], st["oss_segment"]

    run_dir = args.dir and os.path.abspath(args.dir) or find_latest_dir(base)
    if not run_dir or not os.path.isdir(run_dir):
        print("[ERROR] 未找到运行目录")
        return 2
    print(f"运行目录: {run_dir}")

    if not args.upload_only:
        if not do_convert(run_dir, args.limit):
            return 1
    if not args.convert_only:
        if not do_upload(run_dir, args.config, args.limit, oss_segment):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
