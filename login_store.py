# -*- coding: utf-8 -*-
"""一次性人工登录：把某个店铺的 WB 卖家后台登录态写入其独立 Chrome profile。

用法:
    python login_store.py store2      # 登录 store2
    python login_store.py             # 缺省 config.DEFAULT_STORE

为什么需要它:
    WB 的 cookie 受 Windows DPAPI 保护、且绑定 profile，无法自动登录。每个新店必须
    人工登录一次，cookie 落到该店独立 profile（config.STORES[i].profile_dir）；
    之后 fetch_all / Flask 宿主抓取会自动复用该 profile，免登录。

登录后:
    把该店 config 的 auto_start 置 True（或 POST /start?store=<id>），即可随宿主并行抓取。
"""
import sys

import config
from script import PAGE_URL, create_driver


def main():
    store_id = sys.argv[1] if len(sys.argv) > 1 else None
    store = config.get_store(store_id)
    if store is None:
        ids = ", ".join(s["id"] for s in config.STORES)
        print(f"[ERROR] 未知店铺 id: {store_id}（可选: {ids}）")
        return 2

    print(f"[LOGIN] 店铺 {store['id']}，profile={store['profile_dir']}")
    print(f"[LOGIN] 即将打开 {PAGE_URL}")
    print("[LOGIN] 请在弹出的浏览器里完成【该店】WB 卖家后台登录（确认右上角是目标店铺）...")
    driver = create_driver(headless=False, profile_dir=store["profile_dir"])
    try:
        driver.get(PAGE_URL)
        input(">>> 登录并看到商品列表页后，回到此窗口按回车关闭浏览器（cookie 会保存到该店 profile）...")
    except (KeyboardInterrupt, EOFError):
        print("\n[LOGIN] 已中断")
    finally:
        try:
            driver.quit()
        except BaseException:
            pass
    print(f"[LOGIN] 完成。下一步：把 config.STORES 里 {store['id']} 的 auto_start 置 True，"
          f"或跑 python app.py 后 POST /start?store={store['id']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
