# -*- coding: utf-8 -*-
"""
打开 Wildberries 卖家后台商品列表页，监听指定接口 (tableListv6) 的请求返回。

原理:
    1. 开启 Chrome performance 日志（含 Network 域事件）
    2. 轮询日志，匹配目标 URI 的 responseReceived / loadingFinished 事件
    3. 请求加载完成后，通过 CDP 命令 Network.getResponseBody 获取响应体

用法:
    python monitor_wb_tablelist.py                 # 正常窗口模式（方便登录/手动操作触发请求）
    python monitor_wb_tablelist.py --headless      # 无头模式
"""
import base64
import json
import os
import sys
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

# 本地缓存的 chromedriver（如不存在则回退到 Selenium Manager 自动下载）
CHROMEDRIVER_PATH = os.path.expanduser(
    r"~\PycharmProjects\WelcomeScreen\chromedriver.exe"
)

PAGE_URL = "https://seller.wildberries.ru/card-main/all-goods"
# 目标接口：用路径片段匹配，兼容带 query 参数的情况
TARGET_URI = "seller-content.wildberries.ru/ns/viewer/content-card/viewer/tableListv6"
# 表格滚动容器：滚到底部会触发下一页 tableListv6 请求（无限滚动）
SCROLL_CONTAINER_CSS = '[class*="Table__container__"]'
AUTO_SCROLL = True      # 是否自动滚动表格加载下一页（一直滚到底为止，无时长上限）
SCROLL_INTERVAL = 0.5   # 两次滚动之间的间隔（秒）
# 使用独立的自动化资料目录（已从日常 Chrome 复制，带原有 cookie/登录状态）
# 注：Chrome 136+ 禁止对默认资料目录开启远程调试，故不能直接指向日常 User Data
PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "chrome_profile")


def create_driver(headless: bool, profile_dir: str = PROFILE_DIR) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    # 使用指定的独立资料目录（多店时每店一个 profile，cookie 互不干扰）：
    # 已复制日常 Chrome 的登录状态，免登录且可与日常 Chrome 同时运行
    options.add_argument(f"--user-data-dir={profile_dir}")
    # 开启 performance 日志并捕获 Network 事件
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    options.add_experimental_option("perfLoggingPrefs", {
        "enableNetwork": True,
        "enablePage": False,
    })

    if os.path.exists(CHROMEDRIVER_PATH):
        return webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=options)
    return webdriver.Chrome(options=options)


def drain_performance_logs(driver):
    """读取并解析 performance 日志，返回 (已响应的请求, 已完成加载的请求)。

    返回:
        responded: {requestId: {url, status, mimeType}}
        finished:  已完成加载（可取响应体）的 requestId 集合
    """
    responded, finished = {}, set()
    for entry in driver.get_log("performance"):
        try:
            msg = json.loads(entry["message"])["message"]
        except (KeyError, json.JSONDecodeError):
            continue

        method = msg.get("method", "")
        params = msg.get("params", {})

        if method == "Network.responseReceived":
            url = params.get("response", {}).get("url", "")
            if TARGET_URI in url:
                responded[params["requestId"]] = {
                    "url": url,
                    "status": params["response"].get("status"),
                    "mimeType": params["response"].get("mimeType"),
                }
        elif method == "Network.loadingFinished":
            finished.add(params.get("requestId"))
    return responded, finished


def get_response_body(driver, request_id: str):
    """通过 CDP 获取响应体，尝试解析为 JSON（保证结果可被 json.dump 序列化）。"""
    result = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
    body = result.get("body", "")
    if result.get("base64Encoded"):
        data = base64.b64decode(body)
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(data).decode("ascii")  # 二进制内容保留 base64
    try:
        return json.loads(body)
    except (json.JSONDecodeError, TypeError):
        return body


def save_collected(collected, output_file="tableListv6_response.json"):
    """将已捕获的响应写入文件（每捕获一条就立即落盘，避免中途丢失）。"""
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(collected, f, ensure_ascii=False, indent=2, default=str)


def get_scroll_info(driver):
    """返回滚动容器状态 (是否存在, scrollTop, scrollHeight, clientHeight)。"""
    return driver.execute_script("""
        const c = document.querySelector(arguments[0]);
        if (!c) return null;
        return [c.scrollTop, c.scrollHeight, c.clientHeight];
    """, SCROLL_CONTAINER_CSS)


def scroll_container(driver, target: str):
    """将滚动容器滚动到指定位置（'bottom' 或像素值），返回滚动后的 scrollTop。"""
    return driver.execute_script("""
        const c = document.querySelector(arguments[0]);
        if (!c) return -1;
        c.scrollTop = (arguments[1] === 'bottom')
            ? c.scrollHeight
            : Math.min(Number(arguments[1]), c.scrollHeight);
        return c.scrollTop;
    """, SCROLL_CONTAINER_CSS, target)


def auto_scroll_step(driver, state):
    """自动滚动一步，驱动无限加载。返回更新后的 state；加载完毕时 state['done']=True。

    逻辑：
        1. 有请求在途（pending）：等待，不重复触发；
        2. 刚收到新数据：等待一小段时间让新行渲染；
        3. 分步滚动到底部触发下一页；若已在底部且无新数据，判定加载完毕（记录 done 时间）。
    """
    now = time.time()
    if state["pending"] or now < state["cool_down_until"]:
        return state

    info = get_scroll_info(driver)
    if info is None:
        return state  # 容器还没渲染出来（可能在登录页）
    scroll_top, scroll_height, client_height = info
    max_top = scroll_height - client_height

    if max_top <= 0:
        return state  # 内容没有溢出，无需滚动（列表很短）
    if scroll_top >= max_top - 5:
        # 已在底部：先等待足够久，避免把服务端响应间隙误判为加载完毕
        if now - state["last_hit_at"] < 15:
            return state
        if state["retry_phase"] != "kicked":
            # 底部长时间无响应：上滑一小段再滚回底部，重触发一次加载哨兵
            scroll_container(driver, max_top - 500)
            state["retry_phase"] = "kicked"
            state["cool_down_until"] = now + SCROLL_INTERVAL
            return state
        state["done"] = True
        state["done_at"] = now
        print("[DONE] 已滚动到底且无新数据（含一次重触发尝试），判定全部加载完毕")
        return state

    # 分步往下滚，模拟人工滚轮，避免一次性跳到底部触发异常
    step = min(client_height * 0.9, max_top - scroll_top)
    scroll_container(driver, scroll_top + step)
    state["cool_down_until"] = now + SCROLL_INTERVAL
    return state


def main():
    headless = "--headless" in sys.argv
    driver = create_driver(headless)
    # 结果文件名带脚本启动时间戳，避免多次运行互相覆盖
    output_file = f"tableListv6_response_{time.strftime('%Y%m%d_%H%M%S')}.json"
    print(f"打开页面: {PAGE_URL}")
    print(f"监听接口: {TARGET_URI}")
    print(f"结果文件: {output_file}")
    print("无时长上限，自动滚动直到列表加载完毕（可随时 Ctrl+C 停止）...\n")

    driver.get(PAGE_URL)

    responded = {}        # 匹配到响应头的请求（在途）
    collected = []        # 已成功取到响应体的结果
    scroll_state = {
        "pending": False,          # 是否有 tableListv6 请求在途（含首次页面加载）
        "cool_down_until": time.time() + 8,   # 启动后先等首屏加载完
        "last_hit_at": time.time(),           # 上次捕获到响应的时间（供渲染缓冲）
        "retry_phase": None,       # 到底后的重触发标记（'kicked' 表示已尝试过一次）
        "done": False,
        "done_at": 0.0,            # 判定加载完毕的时刻，之后收尾监听几秒再退出
    }

    try:
        while True:
            new_responded, finished = drain_performance_logs(driver)
            if new_responded:
                scroll_state["pending"] = True
            responded.update(new_responded)

            # 对已完成加载的请求获取响应体
            for rid, info in list(responded.items()):
                if rid not in finished:
                    continue
                del responded[rid]
                try:
                    body = get_response_body(driver, rid)
                except Exception as e:
                    print(f"[WARN] 获取响应体失败 (requestId={rid}): {e}")
                    continue
                collected.append({"info": info, "body": body})
                save_collected(collected, output_file)
                scroll_state["pending"] = False
                scroll_state["last_hit_at"] = time.time()
                scroll_state["cool_down_until"] = time.time() + 1.0  # 等新行渲染
                print(f"[HIT] 捕获到请求: {info['url']}")
                print(f"      HTTP 状态: {info['status']}, 类型: {info['mimeType']}，已累计 {len(collected)} 页")
                scroll_state["retry_phase"] = None  # 有新数据，重置到底重触发标记

            # 自动滚动触发下一页加载（可在页面加载后任意时刻开启，不抢首屏）
            if AUTO_SCROLL and not scroll_state["done"]:
                scroll_state = auto_scroll_step(driver, scroll_state)

            # 已判定加载完毕：再收尾监听几秒（等迟到的响应），然后提前结束
            if scroll_state["done"] and time.time() - scroll_state["done_at"] > 5:
                break

            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n手动停止监听")
    finally:
        # 保存捕获结果（中途已逐条落盘，此处确保最终状态完整）
        save_collected(collected, output_file)
        print(f"\n共捕获 {len(collected)} 个响应，已保存到 {output_file}")
        if not scroll_state["done"]:
            print("[提示] 列表尚未加载完毕即退出（手动停止），已捕获的数据不受影响")
        driver.quit()


if __name__ == "__main__":
    main()