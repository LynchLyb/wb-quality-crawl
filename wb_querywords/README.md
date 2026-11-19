# wb_querywords — 搜索词推荐词抓取方案（纯 input 驱动）

> A 分支（nmid-fetch）实现 `recomType=searchViewItem` 单路；B 分支（nmid-fetch-two）实现 `recomType=searchOrders` 与 `searchAddToCart` 双路。
> 本目录是**独立抓取器**，与 `nmid_fetch/`（按 nmID 抓商品详情）、根 `fetch_all.py`（全量商品翻页）互不重叠；生产不再同时开：`原来的停掉，单独跑这个`。

---

## 1. 业务目标

**纯 input 驱动**（不再有固定 00:00–23:59 窗口）：始终跑 OSS input 里**日期最大**的那份文件，每小时重扫一次；出现更大日期的 input 才切换。

1. 输入：OSS `wb_querywords/input/YYYYMMDD.csv`（每小时列举取最大日期，下载到本地 `input/` 解析）。
   - 至少含 **搜索词列 `search_request`** + **搜索量列 `search_volume`**；上游一般已按搜索量降序，本模块再稳定降序兜底。
2. 输出：按 **recomType 分目录**，每个 input 日期每种类型最终合并成**一个文件**，文件名 = input 日期：
   - A：`wb_querywords/output/searchViewItem/<inputDate>.csv`
   - B：`wb_querywords/output/searchOrders/<inputDate>.csv` 与 `wb_querywords/output/searchAddToCart/<inputDate>.csv`
   - **output 日期与 input 日期一对一**（用 input 文件名里的日期，不依赖运行时的 BJT 日期）。
3. 每一次调 `POST https://seller-content.wildberries.ru/ns/analytics-api/content-analytics/api/v2/competitor-comparison/recommendations`，请求体 `{"recomType": <type>, "searchText": <词>}`。
4. **延迟统一上传**：跑的时候每 100 次调用在本地 `output/<类型>/` 刷一个临时分片（即时落盘、崩溃不丢），**不即时上传**；当扫到比当前更大的 input 日期时，把旧日期的分片合并成 `<旧日期>.csv` 再**统一上传** OSS。
5. 切换即丢弃：出现更新 input 时，旧日期没跑完的词不再补（能跑多少是多少），统一上传已跑部分后切到新日期。
6. 中途崩溃 / 手动重启 → 从 `state.json` 断点续跑当前日期（不重跑已完成行）；本地未上传的旧日期分片由启动补传逻辑统一上传。

## 2. 接口与响应字段

响应结构（实测）：`{"error": false, "errorText": "", "data": [{nmID, title, mainPhoto, ...}, ...]}`
——注意 `data` 直接是数组（不是 `data.recommendations`）；`error=true` 视为业务失败不解析。
每条推荐取字段：
```
{ "nmID": 265843232, "title": "Куртка оверсайз", "mainPhoto": "https://basket-17.wbbasket.ru/..." }
```
输出 CSV 固定 5 列（每行 = 一条推荐；searchText/recomType 为入参上下文，空结果写占位行）：
```
searchText, nmID, title, mainPhoto, recomType
```
B 双路 → 两个 `recomType` 各写各的临时分片、各合并到各自的 `<inputDate>.csv`，同一 `searchText` 会分别出现在两个类型文件里。

## 3. 目录布局

```
wb_querywords/
  __init__.py
  config.py         # 常量 + 路径/OSS key 助手
  state.py          # 断点（input_date/processed_index/分片进度）+ 单实例锁
  api_client.py     # 切店 → 捕获 analytics-api 头 → 归一化 → POST 循环（复用 _probe_reco3.py 骨架）
  csv_io.py         # 输入读取（search_request+search_volume，按量降序）/ ShardWriter 临时分片 / merge_shards 合并
  oss_uploader.py   # OSS input 列举+下载 / 合并文件上传（output/<类型>/<日期>.csv）/ 上传台账判重
  worker.py         # 纯 input 驱动主循环 + 每小时重扫 + 切换统一上传 + 429 闭环退避
  app.py            # Flask 宿主 127.0.0.1:8082  /health /status /stop /resume /start
  README.md         # 本方案
  input/            # 从 OSS 下载的 input 副本（gitignore）
  output/           # 按类型分子目录的临时分片 + 合并产物（gitignore，上传源+崩溃恢复）
    searchViewItem/     # A
    searchOrders/       # B
    searchAddToCart/    # B
  state.json        # 断点/分片进度（gitignore）
  uploaded.json     # 已上传台账（gitignore）
  run.lock          # PID 锁（gitignore）
  *.log             # 日志（gitignore）
```

## 4. 关键实现要点

### 4.1 纯 input 驱动（取代 BJT 日窗）
- 不再有 `DAY_*` 窗口与 deadline。worker 主循环：
  1. `_ensure_current`：`max_date = oss.max_input_date(bucket)`（列举 `wb_querywords/input/` 下 `YYYYMMDD.csv`）。
     - 本地有不属于 `cur` 的旧分片 → 先 `_backfill` 统一上传。
     - `cur` 为空 → 设 `cur=max_date`、下载 input、归零断点。
     - `cur < max_date` → 统一上传 `cur` → 切 `max_date`。
  2. `_process_current`：逐行跑 `cur`；每 `INPUT_SCAN_INTERVAL=3600s` 调 `_rescan_due`，发现更新 input → 统一上传 `cur` → break 回主循环切新日期。
  3. `cur` 全部跑完 → **不上传**，空转等被更新 input 取代（最新日期的数据要等下一份 input 到达才上传，这是"延迟统一上传"的既定语义）。

### 4.2 单实例锁
`state.py` 复用根 `fetch_all.acquire_single_instance / release_single_instance`，锁文件 `wb_querywords/run.lock`。宿主重启、手动 CLI 并发运行都会被 `[LOCK]` 挡下。

### 4.3 断点状态
`state.json`：
```
{"input_date": "20261117", "processed_index": N, "current_recom_index": 0|1,
 "shards": {"<recomType>": {"shard_index": i, "calls_in_shard": c}}}
```
- `processed_index`：当前 input 已完成行号；下次从 N 续跑。
- `current_recom_index`（B 用）：当前行已跑到第几个 recomType（0 起；跑满 len(RECOM_TYPES) 才前进 processed_index）。
- `shards`：各类型临时分片进度（分片序号从 1 起 + 片内已写调用数）。
- 切到新 input 日期 → `reset_state_for_new_input` 归零 processed/recom/shards。

### 4.4 API 会话生命周期（单浏览器多次调用）
- 起一个 Chrome：`pre_launch_cleanup` → `create_driver(headless=False, profile_dir)` → `driver.get(PAGE_URL)` → 可选 `ensure_selected_store` 切目标店 → `driver.get(COMPARE_URL)` 触发 `analytics-api` → 40s 轮询 performance log 首捕请求头 → `build_fetch_headers` + 归一化 `content-type: application/json`。
- 后续每行只需 `call_api(driver, RECO_URL, "POST", headers, body)`，无需重开浏览器。
- client 启动失败 → 写断点、60s 退避重试。

### 4.5 429 限流闭环退避
每次 `query` 后按状态：
- `2xx` → 成功；连续成功满 `CLEAN_STREAK=50` 把 interval 缩回 `CALL_INTERVAL=1.2`。
- `429` → interval 翻倍至 `BACKOFF_CAP=300`。
- `401/403` → 连续 3 次视为登录态失效，写断点、`status=auth_failed`、300s 退避（需人工重登，不无限刷）。
- 其他非 2xx → 同一 (行,类型) 连续失败满 `MAX_RETRY_PER_ROW=3` 写空占位行并前进（避免单行死循环）。

### 4.6 Flask 宿主（端口 8082）
- `AUTO_START=True`（config.py）：宿主进程启动即自动 `_ensure_worker()` 拉起 worker，无需手动 `/resume`；配合计划任务看门狗（宿主被重启→worker 自动回来）实现端到端自愈。`_ensure_worker` 有锁 + `is_alive` 守卫，不会双开。⚠️ `user_stopped` 内存态、不落盘，`/stop` 后若宿主被看门狗重启会被 AUTO_START 重新拉起，长期停用需 `Disable-ScheduledTask WB_QuerywordsHost`。
- `POST /resume` / `POST /start` → 拉起 worker 线程（幂等，已在跑返回 409）。
- `POST /stop` → 优雅停止：置 `_stop_event`；worker 在当前调用结束后落 state 并退出。
- `GET /status` → `{worker_alive, input_date, max_input_date, processed_index, total_rows, current_recom_index, status, last_error, pending_local_dates{类型:[日期]}, uploaded[台账键], state}`。

### 4.7 分片、合并与统一上传
- 每次调用**立即** append 到 `output/<类型>/<日期>_<类型>_shard_NNN.csv`（崩溃不丢）。`calls_in_shard` 满 `SHARD_SIZE=100` → 本地滚到下一片（**不上传**）。
- **统一上传时机**：`_rescan_due` / `_ensure_current` 检测到更新 input（或启动补传）时，对旧日期 `merge_shards(类型, 日期)` 把该类型全部分片合并成 `output/<类型>/<日期>.csv`（单表头 + 全数据行）→ `upload_merged` 传 OSS `wb_querywords/output/<类型>/<日期>.csv` → 成功后删除该日期该类型的临时分片。
- **幂等**：合并上传后分片即删；`uploaded.json` 台账按 `类型/日期.csv` + size 判重；上传失败保留分片下次重试。
- OSS key 由「类型 + input 日期」决定（一对一），与 `wildberries` 同级、不走 base prefix。

### 4.8 A / B 分支差异（唯一改动点）
两分支共用同一套代码文件，只在 `config.py` 里通过一个常量区分：

| 分支 | `RECOM_TYPES` | 输出类型目录 | `current_recom_index` |
|---|---|---|---|
| A `nmid-fetch` | `["searchViewItem"]` | `output/searchViewItem/` | 始终 0（本行完成即前进） |
| B `nmid-fetch-two` | `["searchOrders", "searchAddToCart"]` | `output/searchOrders/`、`output/searchAddToCart/` | 0/1 两段推进 |

`worker.py` / `api_client.py` / `csv_io.py` / `state.py` / `app.py` 完全按 `RECOM_TYPES` 长度自适应，无分支硬编码。

## 5. 提交与部署节奏（本轮约定）

1. **A 分支**：本方案 + 全套代码 + `RECOM_TYPES=["searchViewItem"]` + `.gitignore`。写完停下等你审 & 提交。
2. 你审 & 提交 A → 我把 B 分支快进到最新基线，把同一套代码搬进 B 并只改 `RECOM_TYPES=["searchOrders","searchAddToCart"]`。
3. 生产切换由你手动执行：`Disable-ScheduledTask WB_FetchHost / WB_NmidCoordinator` → `Enable/注册 WB_QuerywordsHost`（B 同理）。

## 6. 补充说明（避免误解）

- **input 来自 OSS**：`oss_uploader.list_input_dates` 用 `oss2.ObjectIterator` 列举 `wb_querywords/input/`，`max_input_date` 取最大 `YYYYMMDD`，`download_input` 用 `get_object_to_file` 下到本地 `input/`。OSS 是 input 唯一源，bucket 建不起来则主循环退避重试。
- **列名识别**：`config.TEXT_COL_CANDIDATES` 优先精确 `search_request`，`VOL_COL_CANDIDATES` 优先精确 `search_volume`（靠候选顺序 + 列位置先后，避开 `avg_daily_search_volume`）。未匹配上表头时按位置回退（第 0 列词、第 1 列量）。实际列名不在候选里，往 `config.py` 相应元组追加即可。
- **排序兜底**：上游一般已按 `search_volume` 降序；`read_pairs` 再稳定降序一次，相同量保持原顺序；量解析失败按 0 排末尾。
- **最新日期不落 OSS 的条件**：只有当出现更大日期的 input 时，旧日期才统一上传。若 input 长时间不更新，最新那份会一直留在本地待传（符合"延迟统一上传"设计）。
- **不改现有协调器 / nmid_fetch / fetch_all**：本模块独立，仅通过 Windows 计划任务停掉旧进程达到 exclusive 效果。
