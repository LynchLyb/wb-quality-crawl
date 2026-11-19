# -*- coding: utf-8 -*-
"""wb_querywords 常量与 BJT 日期助手。

设计原则:
    - 所有可调项集中在此，生产改参数不动其他模块
    - BJT 日期不依赖系统时区，显式用 UTC+8 计算
    - A/B 分支共用同一份代码，仅 RECOM_TYPES 一处差异
"""
import os
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------- 项目根
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(MODULE_DIR, "input")
OUTPUT_DIR = os.path.join(MODULE_DIR, "output")

# ---------------------------------------------------------------- 店铺与登录
# 用哪个店 profile 起浏览器（该店 profile 需已在生产机登录过 WB，DPAPI 才能解 cookie）。
# 用 store2（seller 250149024）的登录态跑。
STORE_ID = "store2"

# ---------------------------------------------------------------- 运行参数
# A 分支: 只跑 searchViewItem 一路
# B 分支: ["searchOrders", "searchAddToCart"]
RECOM_TYPES = ["searchViewItem"]

# 纯 input 驱动：无固定时间窗口，靠每小时扫 OSS input 取最大日期文件推进
INPUT_SCAN_INTERVAL = 3600     # 每隔 1 小时重新扫描 input 取最大日期（秒）

# 宿主启动即自动拉起 worker（配合计划任务看门狗实现端到端自愈）；
# 关掉则需手动 POST /resume 才起 worker。user_stopped 是内存态，宿主被看门狗重启后
# AUTO_START 会重新拉起 worker，长期停用请 Disable-ScheduledTask WB_QuerywordsHost。
AUTO_START = True

# ---------------------------------------------------------------- 接口
RECO_URL = ("https://seller-content.wildberries.ru/ns/analytics-api/"
            "content-analytics/api/v2/competitor-comparison/recommendations")
COMPARE_URL = "https://seller.wildberries.ru/platform-analytics/cards-comparison"
ANALYTICS_HINT = "analytics-api"       # 首捕识别 URL 子串
CAPTURE_TIMEOUT = 40                   # 等 analytics-api 首捕秒数

# ---------------------------------------------------------------- 限流与退避
CALL_INTERVAL = 1.2                    # 两次调用间隔（秒），沿用 fetch_all 的稳妥值
RATE_LIMIT_PAUSE = 30                  # 429 后基础暂停
BACKOFF_CAP = 300                      # 429 连撞暂停上限
CLEAN_STREAK = 50                      # 连续成功 N 次把 interval 缩回基线
MAX_RETRY_PER_ROW = 3                  # 单行连续非 2xx 失败上限（超过跳过该行）

# ---------------------------------------------------------------- 输出
# 每行 = 一条推荐结果；searchText/recomType 为入参上下文，nmID/title/mainPhoto 来自响应 data[]
CSV_HEADER = ["searchText", "nmID", "title", "mainPhoto", "recomType"]

# ---------------------------------------------------------------- 分片与 OSS
SHARD_SIZE = 100          # 每 100 次调用（100 个搜索词）本地刷一个临时分片 CSV（崩溃不丢）
# input 源在 OSS：list OSS_INPUT_PREFIX 取最大日期的 YYYYMMDD.csv 下载解析
OSS_INPUT_PREFIX = "wb_querywords/input/"
# output 在 OSS：类型优先分目录，文件名 = input 日期.csv（每类型每日期合并成一个文件上传）
#   key = wb_querywords/output/<recomType>/<inputDate>.csv
OSS_OUTPUT_PREFIX = "wb_querywords/output/"
OSS_CONFIG_FILE = os.path.join(BASE_DIR, "oss_config.json")
UPLOAD_MANIFEST = os.path.join(MODULE_DIR, "uploaded.json")   # 已上传台账（防重启重传）

# ---------------------------------------------------------------- 输入列识别
# 输入 CSV 至少含两列：搜索词 + 搜索量（按搜索量降序抓取）。
# 有表头 → 用小写规范化后的候选名匹配；无表头 → 按位置 index 回退。
# 实际输入表头：search_request(词) / search_volume(量) / avg_daily_search_volume / top_order_category / ...
# 词列优先精确匹配 search_request；量列优先精确匹配 search_volume（靠候选顺序+列位置先后，避免误取 avg_daily_search_volume）。
TEXT_COL_CANDIDATES = ("search_request", "searchtext", "search_text", "query", "keyword", "搜索词", "词", "text")
VOL_COL_CANDIDATES = ("search_volume", "volume", "searchvolume", "cnt", "count",
                      "num", "pv", "uv", "搜索量", "热度")
TEXT_COL_INDEX_FALLBACK = 0   # 无表头时视为搜索词的列号
VOL_COL_INDEX_FALLBACK = 1    # 无表头时视为搜索量的列号

# ---------------------------------------------------------------- 状态文件
STATE_FILE = os.path.join(MODULE_DIR, "state.json")
DAY_DONE_FILE = os.path.join(MODULE_DIR, "day_done.json")
LOCK_FILE = os.path.join(MODULE_DIR, "run.lock")

# ---------------------------------------------------------------- BJT 时间助手
BJT = timezone(timedelta(hours=8))


def now_bjt():
    """当前 BJT 时间（ tz-aware datetime）。"""
    return datetime.now(BJT)


def today_bjt_str():
    """BJT 今日日期字符串 YYYYMMDD。"""
    return now_bjt().strftime("%Y%m%d")


def current_bjt_dt():
    """当前 BJT datetime（仅用于状态展示时间戳）。"""
    return now_bjt()


def local_input_path(date_str):
    """某 input 日期文件下载到本地的绝对路径：input/<YYYYMMDD>.csv。"""
    return os.path.join(INPUT_DIR, f"{date_str}.csv")


def type_output_dir(recom_type):
    """某 recomType 的本地输出目录（临时分片 + 合并产物按类型分目录，A/B 各自隔离）。"""
    return os.path.join(OUTPUT_DIR, recom_type)


def shard_filename(recom_type, shard_index, date_str):
    """本地临时分片文件名：<inputDate>_<recomType>_shard_NNN.csv。"""
    return f"{date_str}_{recom_type}_shard_{shard_index:03d}.csv"


def shard_path(recom_type, shard_index, date_str):
    """本地临时分片绝对路径（在类型子目录下）。"""
    return os.path.join(type_output_dir(recom_type), shard_filename(recom_type, shard_index, date_str))


def merged_filename(date_str):
    """合并后的最终文件名 = input 日期.csv。"""
    return f"{date_str}.csv"


def merged_path(recom_type, date_str):
    """合并后本地文件绝对路径：output/<recomType>/<date>.csv。"""
    return os.path.join(type_output_dir(recom_type), merged_filename(date_str))


def oss_output_key(recom_type, date_str):
    """OSS 上传 key：wb_querywords/output/<recomType>/<date>.csv。"""
    return f"{OSS_OUTPUT_PREFIX}{recom_type}/{merged_filename(date_str)}"
