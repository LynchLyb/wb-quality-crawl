# -*- coding: utf-8 -*-
"""最终重放验证：切到 250149024 -> 捕获一条 analytics 请求头 -> 归一化（单一 content-type，
去掉大小写重复头）-> POST competitor-comparison/recommendations（86 字节体）-> 期望 200。
无人工交互。用法: python _probe_reco3.py [store4] [250149024]"""
import json
import sys
import time

import config
from script import PAGE_URL, create_driver
from fetch_all import (pre_launch_cleanup, build_fetch_headers, call_api,
                       ensure_selected_store)

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except Exception:
        pass

ANALYTICS_HINT = "analytics-api"
COMPARE_URL = "https://seller.wildberries.ru/platform-analytics/cards-comparison"
RECO_URL = ("https://seller-content.wildberries.ru/ns/analytics-api/"
            "content-analytics/api/v2/competitor-comparison/recommendations")
BODY = {"recomType": "searchOrders", "searchText": "куртка зимняя женская"}
LISTEN_SECONDS = 40


def run(store_id, switch_to):
    store = config.get_store(store_id)
    pdir = store["profile_dir"]
    driver = None
    try:
        pre_launch_cleanup(pdir)
        driver = create_driver(headless=False, profile_dir=pdir)
        driver.set_script_timeout(120)
        driver.get(PAGE_URL)
        time.sleep(3)
        st_sel, cur_sel, _rows = ensure_selected_store(driver, switch_to)
        print(f"[SWITCH] -> {st_sel} (current={cur_sel})", flush=True)
        time.sleep(2)
        driver.get(COMPARE_URL)
        time.sleep(3)
        print(f"[NAV] {driver.current_url}", flush=True)

        first = None
        deadline = time.time() + LISTEN_SECONDS
        while time.time() < deadline and first is None:
            for entry in driver.get_log("performance"):
                try:
                    msg = json.loads(entry["message"])["message"]
                except (KeyError, json.JSONDecodeError):
                    continue
                if msg.get("method") != "Network.requestWillBeSent":
                    continue
                req = msg.get("params", {}).get("request", {})
                if ANALYTICS_HINT in req.get("url", "") and req.get("method", "").upper() != "OPTIONS":
                    first = req
                    print(f"[CAP] {req.get('method')} {req.get('url')}", flush=True)
            time.sleep(0.5)
        if first is None:
            print("[WARN] 未捕获到 analytics 头", flush=True)
            return

        headers = build_fetch_headers(first["headers"])
        # 归一化：只保留一个 content-type，避免大小写重复头导致 400 invalidRequestBody
        headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
        headers["content-type"] = "application/json"
        print(f"[HDR] normalized keys={sorted(headers.keys())}", flush=True)

        post = json.dumps(BODY, ensure_ascii=False)
        print(f"[BODY] bytes={len(post.encode('utf-8'))} {post}", flush=True)
        st, tx = call_api(driver, RECO_URL, "POST", headers, post)
        print(f"\n[REPLAY] HTTP {st}", flush=True)
        print(f"[REPLAY] BODY(前2000)={tx[:2000]}", flush=True)
    except Exception as e:
        print(f"[{store_id}] ERROR {type(e).__name__}: {e}", flush=True)
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass
        time.sleep(2)


if __name__ == "__main__":
    run(sys.argv[1] if len(sys.argv) > 1 else "store4",
        sys.argv[2] if len(sys.argv) > 2 else "250149024")
    print("[DONE]", flush=True)
