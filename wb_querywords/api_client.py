# -*- coding: utf-8 -*-
"""wb_querywords 推荐词 API 客户端。

流程（照抄 _probe_reco3.py 的成功骨架）:
    1. 起 Chrome（独立 profile，DPAPI 已登录态）
    2. 打开 PAGE_URL → ensure_selected_store 切到目标店（防爬错账号）
    3. 打开 COMPARE_URL 触发一次 analytics-api 真实请求
    4. 从 performance log 里捕获首条 analytics-api 请求头
    5. build_fetch_headers + 归一化 content-type → 得到 headers
    6. 反复 call_api 即可（同一浏览器会话内 headers 可复用；AuthorizeV3/Wb-Seller-Lk 会过期，worker 会检测 401/403 重开浏览器）
"""
import json
import time

import config as _root_config
from fetch_all import (
    build_fetch_headers, call_api, ensure_selected_store, pre_launch_cleanup,
)
from script import PAGE_URL, create_driver

from wb_querywords import config


class RecoClient:
    """一次浏览器会话 = 一个 RecoClient；挂了由 worker 重建。"""

    def __init__(self, store_id=None):
        self.store_id = store_id or config.STORE_ID
        self.driver = None
        self.headers = None

    # ------------------------------------------------------------ 生命周期
    def start(self):
        """起浏览器 + 捕获 analytics-api 请求头。异常直接抛给 worker。"""
        store = _root_config.get_store(self.store_id)
        if store is None:
            raise RuntimeError(f"store not found: {self.store_id}")

        pre_launch_cleanup(store["profile_dir"])
        self.driver = create_driver(headless=False, profile_dir=store["profile_dir"])
        self.driver.set_script_timeout(120)
        self.driver.get(PAGE_URL)
        time.sleep(3)

        # 切到本店（DPAPI 会话已登录，下拉框确保 checked == 本店卖家 ID）
        expected_seller_id = _root_config.get_seller_id(self.store_id)
        if expected_seller_id:
            st_sel, cur_sel, _rows = ensure_selected_store(self.driver, expected_seller_id)
            print(f"[WQ][SWITCH] store={self.store_id} status={st_sel} current={cur_sel}", flush=True)
            time.sleep(2)
            # 切换后需重载页面才能让 provider context 更新到最新店
            if st_sel == "switched":
                self.driver.get_log("performance")   # 清旧日志
                self.driver.get(PAGE_URL)
                time.sleep(2)

        # 打开对比页触发 analytics-api
        self.driver.get(config.COMPARE_URL)
        time.sleep(3)

        first = self._wait_analytics_request()
        if first is None:
            raise RuntimeError("未捕获到 analytics-api 请求头（登录态或页面异常）")

        headers = build_fetch_headers(first["headers"])
        # 归一化：只保留一个小写 content-type，避免大小写重复头触发 400 invalidRequestBody
        headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}
        headers["content-type"] = "application/json"
        self.headers = headers
        print(f"[WQ][HDR] captured keys={sorted(headers.keys())}", flush=True)

    def stop(self):
        if self.driver is not None:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None
        self.headers = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False

    # ------------------------------------------------------------ 首捕
    def _wait_analytics_request(self, timeout=None):
        """从 performance log 里抓第一条 analytics-api 非 OPTIONS 请求；返回 request dict 或 None。"""
        timeout = timeout or config.CAPTURE_TIMEOUT
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                entries = self.driver.get_log("performance")
            except Exception:
                entries = []
            for entry in entries:
                try:
                    msg = json.loads(entry["message"])["message"]
                except (KeyError, json.JSONDecodeError, TypeError):
                    continue
                if msg.get("method") != "Network.requestWillBeSent":
                    continue
                req = msg.get("params", {}).get("request", {})
                url = req.get("url", "")
                method = (req.get("method") or "").upper()
                if config.ANALYTICS_HINT in url and method != "OPTIONS":
                    print(f"[WQ][CAP] {method} {url}", flush=True)
                    return req
            time.sleep(0.5)
        return None

    # ------------------------------------------------------------ 调用
    def query(self, search_text, recom_type):
        """单次推荐词查询。返回 (status_int, parsed_list_or_None, raw_text, error_text)。

        响应结构（实测）: {"error": false, "errorText": "", "data": [{nmID,title,mainPhoto,...}, ...]}
        - parsed_list 即 data 数组（每条推荐一个 dict）；失败/非 200/error=true 时为 None。
        - error_text：2xx 且 error=true 时为响应里的 errorText（如 content.analytics.limitExceeded，
          表示每日对比推荐配额已用尽）；其余情况为空串 ""。调用方据此区分"配额限流/业务错误/真空结果"。
        """
        if self.driver is None or self.headers is None:
            raise RuntimeError("RecoClient 未启动")
        body = {"recomType": recom_type, "searchText": search_text}
        post = json.dumps(body, ensure_ascii=False)
        status, text = call_api(self.driver, config.RECO_URL, "POST", self.headers, post)
        recs = None
        error_text = ""
        if 200 <= status < 300:
            try:
                j = json.loads(text)
            except (json.JSONDecodeError, TypeError):
                j = None
            if isinstance(j, dict):
                if j.get("error"):
                    # 业务错误：透出 errorText 交调用方分类（配额/其它）
                    error_text = str(j.get("errorText") or "")
                else:
                    data = j.get("data")
                    if isinstance(data, list):
                        recs = data
                    elif isinstance(data, dict) and isinstance(data.get("recommendations"), list):
                        recs = data["recommendations"]
                    elif isinstance(j.get("recommendations"), list):
                        recs = j["recommendations"]
        return status, recs, text, error_text

