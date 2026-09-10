# -*- coding: utf-8 -*-
"""Flask 宿主运行配置（集中可调）。

所有部署相关的可调项都放这里，改完重启 app.py 即可生效。
"""
import json
import os

# 项目根目录：STORES 里的相对路径都按此解析为绝对路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# HTTP 监听地址：默认仅本机可访问（安全默认）。需局域网访问再改 0.0.0.0。
HOST = "127.0.0.1"
PORT = 8080

# 巡检 cron 表达式（APScheduler CronTrigger.from_crontab 语法，5 段：分 时 日 月 周）。
# 默认每 2 分钟巡检一次 worker 健康状态：挂了且未拉完就自动 --resume 重启。
CRON_EXPR = "*/2 * * * *"

# 宿主启动即拉起 worker（--resume 断点续传），实现"常驻子线程"效果。
# 设为 False 则宿主只提供接口，需手动 POST /start 才开跑。
AUTO_START = True

# ---------------------------------------------------------------- 多店铺注册表
# 每店一个条目，各店完全隔离（独立 profile / 运行目录 / 锁 / OSS prefix 段）。
# 相对路径按 BASE_DIR 解析为绝对路径。字段:
#   id          店铺唯一标识；同时用作 HTTP ?store= 参数、锁文件名、日志前缀
#   profile_dir 该店独立 Chrome user-data-dir（cookie 隔离，避免多店撞登录态）
#   data_dir    该店运行目录（tableListv6_* 分片与 fetch_all.lock 落这里）
#   oss_segment OSS prefix 追加段（缺省=id）；最终 prefix = base_prefix + oss_segment + "/"
#   auto_start  可选，缺省=全局 AUTO_START；新店登录前置 False，避免未登录空转重启
# 上线前把 store2/store3 的 id/oss_segment 换成真实 WB 卖家 id 或店铺代号即可，代码不用动。
STORES = [
    # store1 现有店：已登录，随宿主 AUTO_START 自动续拉
    {"id": "store1", "profile_dir": "chrome_profile",  "data_dir": ".",           "oss_segment": "store1"},
    # store2 已登录并续拉中：auto_start=True，随宿主 AUTO_START 一起自动 --resume 续传（与 store1 一致）
    {"id": "store2", "profile_dir": "profiles/store2", "data_dir": "data/store2", "oss_segment": "store2", "auto_start": True},
    # store3 已按需求暂时移除（暂不抓取该店数据）；日后需要时照 store2 再加一行即可
]
DEFAULT_STORE = "store1"


def resolve_store(spec):
    """把 STORES 里的一个条目解析成运行时 dict：相对路径转绝对、oss_segment 缺省=id。"""
    sid = spec["id"]

    def _abs(p):
        p = p or "."
        return p if os.path.isabs(p) else os.path.normpath(os.path.join(BASE_DIR, p))

    return {
        "id": sid,
        "profile_dir": _abs(spec.get("profile_dir")),
        "data_dir": _abs(spec.get("data_dir", ".")),
        "oss_segment": (spec.get("oss_segment") or sid).strip("/"),
        "auto_start": spec.get("auto_start", AUTO_START),
    }


def get_store(store_id=None):
    """按 id 取已解析店铺配置。store_id=None → DEFAULT_STORE；未知 id → None（交调用方处理）。"""
    target = store_id or DEFAULT_STORE
    for s in STORES:
        if s["id"] == target:
            return resolve_store(s)
    return None


def all_stores():
    """全部店铺的已解析配置列表（顺序同 STORES）。"""
    return [resolve_store(s) for s in STORES]


# ---------------------------------------------------------------- 店铺校验用卖家 ID
# config.json: {"store1": 250132124, "store2": 250149024}
# 抓取前用它校验页面下拉框选中的店铺，防止登录错账号、爬错店数据。
SELLER_ID_FILE = os.path.join(BASE_DIR, "config.json")


def _load_seller_ids():
    try:
        with open(SELLER_ID_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def get_seller_id(store_id):
    """该店期望的 WB 卖家 ID（字符串）；config.json 未配置则返回 None。"""
    v = _load_seller_ids().get(store_id)
    return str(v) if v is not None else None
