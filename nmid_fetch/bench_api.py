"""bench_api — tableListv6 vs GetCardInfo 单条查询耗时基准测试。

用途:
    对比两个接口按 nmID 查单卡的原始请求耗时（不含 CALL_INTERVAL 睡眠），
    验证生产实测 3.3s/条 中真正的网络耗时占比，评估 GetCardInfo 是否更快。

运行（仓库根目录，Windows 生产机，需已登录）:
    python -m nmid_fetch.bench_api --store store1 --count 20
    python -m nmid_fetch.bench_api --store store1 --nmids 1641296819,123456789
    python -m nmid_fetch.bench_api --store store1 --nmid-file ids.txt --interval 2.0

nmID 来源（三选一，优先级从高到低）:
    --nmids 逗号分隔 / --nmid-file 文本文件（每行一个）/ 自动取本店最新 input_*.csv

安全设计:
    - 复用 fetch_by_nmid.lock：生产 worker 在跑时拒绝启动，避免互相干扰计时与触发限流
    - 只读查询，不落分片、不上传 OSS、不写 state.json
    - 429 时按 RATE_LIMIT_PAUSE 暂停后重试，该次不计入耗时统计
"""
import argparse
import glob
import json
import os
import sys
import time

import config
from script import PAGE_URL, create_driver

from fetch_all import (
    RATE_LIMIT_PAUSE, CAPTURE_TIMEOUT,
    acquire_single_instance, release_single_instance,
    pre_launch_cleanup, wait_first_request, build_fetch_headers, call_api,
    ensure_selected_store, verify_selected_store,
)
from nmid_fetch import oss_input

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

DATA_ROOT = os.path.join(config.BASE_DIR, "nmid_data")
MAX_429_RETRY = 3


# ---------------------------------------------------------------- nmID 来源
def collect_nmids(args, store_id):
    if args.nmids:
        return [x.strip() for x in args.nmids.split(",") if x.strip()]
    if args.nmid_file:
        with open(args.nmid_file, encoding="utf-8-sig") as f:
            return [line.strip() for line in f if line.strip()]
    # 自动：本店 nmid_data/<store>/*/input_*.csv 中最新的一个
    pattern = os.path.join(DATA_ROOT, store_id, "*", "input_*.csv")
    candidates = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
    if not candidates:
        return []
    print(f"[BENCH] 自动选用输入文件: {candidates[0]}")
    return oss_input.parse_nmids(candidates[0])


# ---------------------------------------------------------------- 统计
def _pct(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    idx = min(int(q * len(sorted_vals)), len(sorted_vals) - 1)
    return sorted_vals[idx]


def summarize(name, samples):
    """samples: [(dt, ok), ...]（已剔除 429 重试与 warmup）。"""
    dts = sorted(dt for dt, ok in samples)
    ok_n = sum(1 for _, ok in samples if ok)
    if not dts:
        return {"接口": name, "n": 0, "成功": 0, "失败": len(samples)}
    return {
        "接口": name, "n": len(dts), "成功": ok_n, "失败": len(samples) - ok_n,
        "min": f"{dts[0]:.3f}s", "avg": f"{sum(dts) / len(dts):.3f}s",
        "p50": f"{_pct(dts, 0.5):.3f}s", "p95": f"{_pct(dts, 0.95):.3f}s",
        "max": f"{dts[-1]:.3f}s",
    }


# ---------------------------------------------------------------- 单次调用
def timed_call(driver, url, method, headers, post_data, label):
    """执行一次请求并计时。429 时暂停重试（不计入）。返回 (dt, status, ok, text)。"""
    retry_429 = 0
    while True:
        t0 = time.perf_counter()
        try:
            status, text = call_api(driver, url, method, headers, post_data)
        except Exception as e:
            return time.perf_counter() - t0, -1, False, f"EXCEPTION: {type(e).__name__}: {e}"
        dt = time.perf_counter() - t0
        if status == 429 and retry_429 < MAX_429_RETRY:
            retry_429 += 1
            print(f"[BENCH] {label} 429 限流，暂停 {RATE_LIMIT_PAUSE}s 后重试（第 {retry_429} 次）")
            time.sleep(RATE_LIMIT_PAUSE)
            continue
        ok = False
        if status == 200:
            try:
                payload = json.loads(text)
                data = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(data, dict):
                    # tableListv6: data.cards 非空数组；GetCardInfo: data.id 存在
                    ok = bool(data.get("cards")) or data.get("id") is not None
            except (json.JSONDecodeError, AttributeError):
                ok = False
        return dt, status, ok, text


# ---------------------------------------------------------------- 主流程
def main():
    ap = argparse.ArgumentParser(description="tableListv6 vs GetCardInfo 耗时基准")
    ap.add_argument("--store", required=True, help="店铺 id（config.STORES 里的 store1/store2）")
    ap.add_argument("--nmids", help="逗号分隔的 nmID 列表")
    ap.add_argument("--nmid-file", help="nmID 文本文件（每行一个）")
    ap.add_argument("--count", type=int, default=20, help="参与测试的 nmID 数（默认 20）")
    ap.add_argument("--interval", type=float, default=1.5,
                    help="相邻请求间隔秒数（默认 1.5，与生产 CALL_INTERVAL 一致）")
    ap.add_argument("--warmup", type=int, default=2,
                    help="预热 nmID 数，不计入统计（默认 2）")
    ap.add_argument("--out", help="可选：明细结果 JSON 输出路径")
    args = ap.parse_args()

    store = next((s for s in config.STORES if s["id"] == args.store), None)
    if store is None:
        print(f"[BENCH] 未知店铺: {args.store}（可选: {[s['id'] for s in config.STORES]}）")
        return 2
    store_id = store["id"]

    nmids = collect_nmids(args, store_id)
    if not nmids:
        print("[BENCH] 无可用 nmID：请用 --nmids / --nmid-file，或确认本店有 input_*.csv")
        return 2
    total = args.warmup + args.count
    if len(nmids) < total:
        print(f"[BENCH] nmID 不足：需要 {total}（warmup {args.warmup} + count {args.count}），"
              f"实际 {len(nmids)}，按实际数量执行")
        total = len(nmids)
    nmids = nmids[:total]

    # 与生产 worker 互斥：占用同一把锁，防止并发打接口干扰计时
    lock_file = os.path.join(DATA_ROOT, store_id, "fetch_by_nmid.lock")
    os.makedirs(os.path.dirname(lock_file), exist_ok=True)
    if not acquire_single_instance(lock_file):
        print(f"[BENCH] 本店抓取 worker 正在运行（锁被占用: {lock_file}），"
              "请在协调器窗口外或 worker 停止后再测")
        return 2

    driver = None
    results = []
    try:
        pre_launch_cleanup(store["profile_dir"])
        driver = create_driver(headless=False, profile_dir=store["profile_dir"])
        driver.set_script_timeout(120)
        driver.get(PAGE_URL)

        expected_id = config.get_seller_id(store_id)
        switched = False
        if expected_id is not None:
            status, cur_id, rows = ensure_selected_store(driver, expected_id)
            if status == "switched":
                switched = True
                time.sleep(3)
                try:
                    driver.get_log("performance")
                except Exception:
                    pass
                driver.get(PAGE_URL)
            elif status != "already":
                print(f"[BENCH] 自动选店失败: store_select_{status}")
                return 3
        first = wait_first_request(driver, CAPTURE_TIMEOUT)
        if first is None:
            print("[BENCH] capture_timeout：未捕获到 tableListv6 请求（登录态可能失效）")
            return 3
        headers = build_fetch_headers(first["headers"])
        if expected_id is not None and switched:
            selected_id, _ = verify_selected_store(driver)
            if selected_id != expected_id:
                print("[BENCH] store_mismatch：切换后复核不符")
                return 3

        body_template = None
        if first["postData"]:
            try:
                body_template = json.loads(first["postData"])
            except json.JSONDecodeError:
                body_template = None
        getcardinfo_base = first["url"].rsplit("/", 1)[0] + "/GetCardInfo?nmID="

        print(f"[BENCH] 开始：{total} 个 nmID × 2 接口，间隔 {args.interval}s，"
              f"预热 {args.warmup} 个不计入统计")
        for i, nmid in enumerate(nmids):
            is_warmup = i < args.warmup

            # ① tableListv6：与生产完全一致的构造方式（filter.search 替换）
            if isinstance(body_template, dict):
                body = json.loads(json.dumps(body_template))
                filt = body.setdefault("filter", {})
                if isinstance(filt, dict):
                    filt["search"] = nmid
            else:
                body = {"sort": [{"columnID": 11, "order": "desc"}],
                        "filter": {"search": nmid, "paidOptions": {}},
                        "cursor": {"n": 20}}
            post_data = json.dumps(body, ensure_ascii=False)
            dt1, st1, ok1, _ = timed_call(driver, first["url"], first["method"],
                                          headers, post_data, "tableListv6")
            time.sleep(args.interval)

            # ② GetCardInfo：GET，鉴权头一致
            dt2, st2, ok2, _ = timed_call(driver, getcardinfo_base + nmid,
                                          "GET", headers, None, "GetCardInfo")
            time.sleep(args.interval)

            results.append({"nmid": nmid, "warmup": is_warmup,
                            "tableListv6": {"dt": round(dt1, 3), "status": st1, "ok": bool(ok1)},
                            "GetCardInfo": {"dt": round(dt2, 3), "status": st2, "ok": bool(ok2)}})
            tag = " [warmup]" if is_warmup else ""
            print(f"[BENCH] {i + 1}/{total} nmID={nmid}{tag}  "
                  f"tableListv6={dt1:.3f}s(status={st1},ok={ok1})  "
                  f"GetCardInfo={dt2:.3f}s(status={st2},ok={ok2})")

        # 汇总
        counted = [r for r in results if not r["warmup"]]
        s1 = summarize("tableListv6", [(r["tableListv6"]["dt"], r["tableListv6"]["ok"])
                                       for r in counted])
        s2 = summarize("GetCardInfo", [(r["GetCardInfo"]["dt"], r["GetCardInfo"]["ok"])
                                       for r in counted])
        print("\n========== 基准结果（不含 interval 睡眠，不含 warmup） ==========")
        for row in (s1, s2):
            print("  ".join(f"{k}={v}" for k, v in row.items()))
        if s1.get("n") and s2.get("n"):
            a1 = sum(r["tableListv6"]["dt"] for r in counted) / len(counted)
            a2 = sum(r["GetCardInfo"]["dt"] for r in counted) / len(counted)
            print(f"\n平均耗时差: tableListv6 - GetCardInfo = {a1 - a2:+.3f}s "
                  f"(GetCardInfo 为 tableListv6 的 {a2 / a1:.1%})")
            print(f"换算整链路（+interval {args.interval}s）: "
                  f"tableListv6 ≈ {a1 + args.interval:.2f}s/条, "
                  f"GetCardInfo ≈ {a2 + args.interval:.2f}s/条")
        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump({"interval": args.interval, "results": results,
                           "summary": {"tableListv6": s1, "GetCardInfo": s2}},
                          f, ensure_ascii=False, indent=2)
            print(f"[BENCH] 明细已写入: {args.out}")
        return 0
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        release_single_instance(lock_file)


if __name__ == "__main__":
    sys.exit(main())
