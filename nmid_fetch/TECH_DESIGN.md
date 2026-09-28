# 技术方案：按 nmID 集合抓取 WB 商品数据（nmid_fetch 模块）

> 状态：代码已提交（分支 `nmid-fetch`，commit `22d600f`），生产机 A 部署运行
> 原则：零侵入（不改动任何现有代码）、最大复用、格式一致、可续传、串行调度

---

## 1. 目标

新增独立模块 `nmid_fetch/`，实现：

- 常驻 Flask 服务，循环扫描 OSS `content-opt-pool/` 目录
- 按当前账号规则筛选输入文件：`{卖家ID}_treatment_001.csv` 每轮全量重复跑；`{卖家ID}_control.csv` 每上传一份都跑、每份只跑一次，且排队优先执行
- 下载 CSV（含 nmID 列表）
- 对每个 nmID 调 `tableListv6` 接口（`filter.search = nmID`）查询商品数据
- 每 2000 次查询写一个分片 JSON，异步转 CSV 并上传 OSS
- 轮初一次性把本轮全部输入下载落盘，再按队列串行执行（control 在前、treatment_001 在后）；001 全部处理完立即开始下一轮；本轮无输入可跑（无符合规则的输入 / control 全部已记账）则等待 60s
- **不改动任何现有文件**
- **旧模块继续跑全量，新旧模块通过外部协调器串行调度**

---

## 2. 设计原则

| 原则 | 说明 |
|---|---|
| **零侵入** | 不修改 `fetch_all.py`、`script.py`、`supervisor.py`、`app.py`、`config.py`、`wb_to_oss.py` |
| **最大复用** | 从现有模块导入通用函数（选店、捕获请求、调接口、限流处理、锁、OSS 上传等） |
| **格式一致** | 输出分片 JSON 结构与现有 `tableListv6_shard_*.json` 兼容，直接复用 `wb_to_oss.py` 转换 |
| **可续传** | 记录已处理 nmID 索引，崩溃后从断点继续 |
| **循环处理** | 常驻服务，treatment 处理完立即重新开始；无事可做时等待，不空转刷日志/OSS |
| **变体隔离** | control 独立工作目录 `control/<taskId>/`，treatment 保持 `<taskId>/`；分片名带全变体标记，共用 taskId 也不互相覆盖 |
| **control 幂等** | control 每份跑完写台账一条（OSS key + mtime），每份只跑一次；同名覆盖（mtime 变化）视为新的一份重跑 |
| **串行调度** | 新模块与旧模块共用 Chrome profile，通过外部协调器时间片轮转，互不冲突 |

---

## 3. 新增文件清单

```
nmid_fetch/
├── __init__.py              # 包标识 + 模块说明
├── README.md                # 使用说明 + 风险记录
├── oss_input.py             # OSS 输入文件扫描/下载/解析
├── fetch_by_nmid.py         # 核心逻辑：按 nmID 查询 + 分片 + 上传
├── app_nmid.py              # Flask 宿主入口：常驻循环 + HTTP 接口（端口 8081）
├── coordinator.py           # 外部协调器：新旧模块天级轮转调度（含重试/心跳守护）
└── check_health.py          # 一键健康检测：旧/新/协调器存活 + 互斥检查
```

运行后生成的数据目录：

```
nmid_data/
├── control_done.json           # control 台账：{store_id: {OSS key: {name, last_modified, finished_at, ...}}} 每店多条、每份一条
├── store1/
│   ├── <taskId>/               # treatment_001 工作目录（布局不变）
│   │   ├── <卖家ID>_treatment_001.csv      # 输入保留 OSS 原名，同名覆盖（如 25013****_treatment_001.csv）
│   │   ├── <taskId>_<卖家ID>_treatment_001_shard_NNN_<run_ts>.json   # treatment 分片（带全 _treatment_001 标记）
│   │   ├── csv/
│   │   │   └── <taskId>_<卖家ID>_treatment_001_shard_NNN_<run_ts>.csv
│   │   ├── state_treatment.json    # treatment 断点
│   │   └── summary_treatment.json  # treatment 汇总
│   ├── control/
│   │   └── <taskId>/           # control 独立工作目录（与 treatment 分离）
│   │       ├── <卖家ID>_control.csv        # 如 25013****_control.csv，同名覆盖
│   │       ├── <taskId>_<卖家ID>_control_shard_NNN_<run_ts>.json
│   │       ├── csv/
│   │       │   └── <taskId>_<卖家ID>_control_shard_NNN_<run_ts>.csv
│   │       ├── state_control.json      # control 断点
│   │       └── summary_control.json    # control 汇总
│   └── fetch_by_nmid.lock      # PID 锁（店铺层）
└── store2/
    └── ...
```

实义示例（taskId=`7ea403`、卖家 `25014****` → store2，首片、运行始于 2026-09-28 10:15:00）：

```
nmid_data/store2/7ea403/25014****_treatment_001.csv                        # 输入（001）
nmid_data/store2/7ea403/7ea403_25014****_treatment_001_shard_000_20260928_101500.json
nmid_data/store2/control/7ea403/25014****_control.csv                      # 输入（control）
nmid_data/store2/control/7ea403/7ea403_25014****_control_shard_000_20260928_101500.json
```

上传成功后分片 JSON/CSV 从本地删除（与旧行为一致）；`state_<tag>.json` / `summary_<tag>.json` / 台账保留。

---

## 4. 输入格式

### OSS 输入路径

```
content-opt-pool/
├── <taskId_1>/
│   ├── <卖家ID_1>_treatment_001.csv   ← 实验组 001：跑
│   ├── <卖家ID_1>_treatment_002.csv   ← 实验组 002+：不跑
│   ├── <卖家ID_1>_control.csv         ← 对照组：每上传一份都跑一次，排队优先
│   └── <卖家ID_2>_...csv
├── <taskId_2>/
│   └── ...
```

### 命名解析（`oss_input.parse_csv_name`）

| 文件名 | variant | index | 是否跑 |
|---|---|---|---|
| `<卖家ID>_treatment_001.csv` | `treatment` | 1 | ✅ 每轮重复 |
| `<卖家ID>_treatment_002.csv` | `treatment` | 2 | ❌ 忽略 |
| `<卖家ID>_control.csv` | `control` | - | ✅ 每份都入队（上传时间序、队首优先），每份只跑一次，台账按 key+mtime 去重 |
| `<卖家ID>.csv`（旧命名） | `legacy` | - | ❌ 忽略（仅 CLI 未指定变体时兜底） |
| 其他 | `unknown` | - | ❌ 忽略，每轮汇总告警一次 |

### 规则编排（`oss_input.plan_round`）

一次 OSS 扫描给出本轮待跑清单：

- `treatment`：每个 taskId 下编号 = `TREATMENT_INDEX`（1）的文件各一份；
- `control`：**全部**按 `(last_modified, key)` 上传时间序入队，且排在**队首**（treatment 之前）；每份只跑一次，`latest_control_per_seller` 已删除；
- `sellers` 参数限定只编排 `config.json` 里已配置的卖家，其余直接不入清单；
- 返回 `(jobs, unknown_names)`，job 里带 `key/name/seller_id/variant/index/last_modified/task_id`。

`control` 是否真的跑由宿主再查一次台账（`control_already_done`：key 与 mtime 均相同 → 跳过，**连下载都不发生**）。

### CSV 文件格式

```csv
nm_id
13334444
13334445
```

- 只有一列 `nm_id`，每行一个 nmID
- 文件名前缀 = WB 数字卖家 ID（如 `25014****_treatment_001.csv`）

### 店铺映射

文件名里的卖家 ID → 反查 `config.json` → 得到 `store1`/`store2`：

```json
{"store1": 25013****, "store2": 25014****}
```

---

## 5. 输出格式

### OSS 输出路径

```
wildberries/wbRatingData/{oss_segment}/json/{上传当天BJT日期}/{文件名}
wildberries/wbRatingData/{oss_segment}/csv/{上传当天BJT日期}/{文件名}
```

### 文件名规则

```
{taskId}_{卖家ID}_treatment_001_shard_{NNN}_{run_ts}.json   # treatment_001（带全标记）
{taskId}_{卖家ID}_control_shard_{NNN}_{run_ts}.json         # control
```

- `run_ts`：本次运行时间戳 `YYYYMMDD_HHMMSS`，每轮产生新文件，**不覆盖旧文件**
- 变体标记取自输入文件（`oss_input.variant_tag`）：treatment_001 带全 `_treatment_001`、control 带 `_control`、legacy/CLI 未指定变体不带标记（兼容历史产物）；CSV 与 JSON 同名只换后缀
- OSS 上传路径不变，下游区分 treatment / control 数据看文件名里的 `_treatment_001` / `_control` 标记

### 分片 JSON 格式

与现有 `tableListv6_shard_*.json` 兼容：

```json
[
  {"data": {"cards": [命中 nmID 的卡片]}},
  {"data": {"cards": []}}
]
```

---

## 6. 核心流程

```
app_nmid.py 启动（Flask + waitress，端口 8081；worker 由 /resume 或协调器拉起）
    │
    ▼
while True:  # 常驻循环
    ├─ plan_round（一次 OSS 扫描）→ jobs = [全部 control（上传时间序）] + [每 taskId 一份 treatment_001]
    ├─ 过滤 control：台账里 key+mtime 相同的已跑过 → 剔除（连下载都不发生）
    ├─ jobs 为空 / 剔完为空（无符合规则的输入 + control 全部已记账）→ 置 idle，睡 60s（可被 /stop 打断）后重查
    ├─ 下载阶段（轮初一次性拉取本轮全部输入，日志 [DL]）：
    │   ├─ control → nmid_data/<store>/control/<taskId>/<原始文件名>（如 25014****_control.csv）
    │   ├─ treatment_001 → nmid_data/<store>/<taskId>/<原始文件名>（如 25014****_treatment_001.csv）
    │   ├─ 保留 OSS 原名、同名覆盖；下载的输入文件一律不删除
    │   └─ 某份下载失败 → 该份本轮跳过执行，记本轮未完成
    ├─ 执行阶段（按队列顺序串行，control 先跑）：
    │   ├─ 反查 config.json → store_id（未配置的卖家每轮汇总告警一次）
    │   ├─ 获取 PID 锁（nmid_data/<store>/fetch_by_nmid.lock）
    │   ├─ process_task(local_csv=已下载的本地输入，不二次下载）→ 解析 nm_id 列
    │   ├─ 清理残留 Chrome → 启动 Chrome → 打开 WB 页面
    │   ├─ 自动选店 + 复核 → 捕获首条 tableListv6 请求
    │   ├─ 读取断点 state_<tag>.json（tag=treatment|control）→ 恢复已处理索引
    │   ├─ 对每个 nmID（从断点开始）：
    │   │   ├─ 构造请求体：filter.search = nmID
    │   │   ├─ 调 tableListv6（call_api）
    │   │   ├─ 未命中 → 直接丢弃
    │   │   ├─ 命中 → 加入 buffer
    │   │   ├─ 每 2000 次查询 → 写分片 → 异步转 CSV + 上传 OSS
    │   │   └─ 更新 state_<tag>.json
    │   ├─ 写 summary_<tag>.json；若是 control 且 finished → 写 control_done.json 台账一条
    │   └─ 释放锁 → 关闭 Chrome
    ├─ 整轮一个任务也没执行（如锁被占）→ 睡 60s 后重试，不空转
    └─ 全部 finished → 只重置 **treatment** 的断点，立即开始下一轮
        （control 不重置：每份只跑一次，新上传的份下一轮自然插队队首）
```

---

## 7. 与现有代码的复用关系

| 来源 | 复用内容 | 用途 |
|---|---|---|
| `fetch_all.py` | `wait_first_request` / `build_fetch_headers` / `call_api` | 捕获请求 + 调接口 |
| `fetch_all.py` | `ensure_selected_store` / `verify_selected_store` | 自动选店 + 复核 |
| `fetch_all.py` | `pre_launch_cleanup` / `acquire_single_instance` / `release_single_instance` / `_lock_file` | 清理残留 + PID 锁 |
| `fetch_all.py` | `RATE_LIMIT_PAUSE` / `SERVER_FAIL_LIMIT` / `CAPTURE_TIMEOUT` | 限流/退避常量 |
| `script.py` | `PAGE_URL` / `create_driver` | 启动浏览器 |
| `config.py` | `get_store` / `get_seller_id` / `resolve_store` | 店铺配置 |
| `wb_to_oss.py` | `oss_target` / `convert_and_upload_shard` / `oss_key_for` | 分片转 CSV + 上传 OSS |

**不复用**：`fetch_all.run_fetch`（要插入 nmID 查询逻辑）、`supervisor.py` / `app.py`（新模块有自己的 Flask 宿主）。

**新模块自定义常量**：`CALL_INTERVAL = 1.5`（限流 60 秒 40 次）、`SHARD_SIZE = 2000`。

---

## 8. 关键设计点

### 8.1 分片策略

- 每 **2000 次查询**写一个分片
- 20 万 nmID → 100 个分片
- 风险：崩溃时最多丢失 2000 次查询结果（**实测约 1 小时 50 分**：2000 × 3.3s ≈ 110 分钟；旧文“约 50 分钟”是 1.5s 理论下限估算，非实测）

### 8.2 断点续传

`state_<tag>.json`（tag=treatment|control，落在各自 run_dir，按变体区分避免撞名）：

```json
{
  "task_id": "task_001",
  "store_id": "25013****",
  "run_ts": "20260918_120000",
  "shard_index": 3,
  "query_count": 4500,
  "matched_count": 4200,
  "processed_index": 4500,
  "finished": false
}
```

### 8.3 锁机制

- 新模块锁：`nmid_data/<store>/fetch_by_nmid.lock`
- 旧模块锁：`<data_dir>/fetch_all.lock`
- 两锁隔离，但**共用同一个 Chrome profile**，通过协调器串行运行

### 8.4 请求体构造

基于捕获的首条请求模板，只替换 `filter.search`：

```json
{
  "sort": [{"columnID": 11, "order": "desc"}],
  "filter": {"search": "<nmID>", "paidOptions": {}},
  "cursor": {"n": 20}
}
```

### 8.5 未命中处理

- 查询返回空 cards → **直接丢弃**，不记录、不重试
- `summary_<tag>.json` 记录 `query_count` / `matched_count` / `missed_count`

### 8.6 数据安全（暂停不丢失）

- stop 时 `finally` 块把 buffer 剩余数据写入分片
- stop 时保存 `state_<tag>.json` 断点
- stop 时等待所有异步上传线程完成（`join(timeout=30)`）
- resume 时从 `processed_index` 继续，不重复查询

### 8.7 下载过滤（只处理已配置的两店 + 只处理规则内的变体）

- 每轮一次 `plan_round(sellers=已配置卖家)`：内部 `list_task_ids()` 取 `content-opt-pool/` 一级目录，`list_input_csvs(tid)` 只列 `.csv` 后缀文件（manifest.json 等非 CSV 直接忽略）并解析出 variant / index / mtime
- 纳入 `_treatment_001.csv`（每轮重复）与**全部** `_control.csv`（每上传一份都入队，上传时间序、control 排队首优先）；`_treatment_002+`、旧命名 `{sellerId}.csv`、命名不符的文件一律不入清单
- 文件名前缀 = WB 数字 sellerId；`store_id_for_seller()` 按 `config.json` 反查店铺（当前仅 store1=25013****、store2=25014****）
- **非这两个店铺**（sellerId 不在 config.json）：**不下载、不处理**；告警按轮聚合成一条
  `[WARN] 以下卖家未在 config.json 配置，本轮共跳过 N 个输入文件: …`（早期逐文件打印导致日志暴涨，已收敛）
- 命名不符的文件同样每轮只汇总告警一次，并计入 `/status` 快照的 `skipped`
- 店铺锁被其他进程占用：本轮跳过该 job，睡 60s 后重试（整轮没执行任何任务时不空转）
- 因此在 OSS 增删 CSV 只影响"本轮跑哪些文件"；未配置店铺 / 规则外变体的文件永远不会被拉取到本地

### 8.8 control 去重与台账

- 台账文件 `nmid_data/control_done.json`：`{store_id: {OSS key: {name, task_id, seller_id, last_modified, finished_at, query_count, matched_count, run_dir}}}` 每店多条、每份一条；旧版每店单条格式首读自动转换
- **只在 `finished=True` 时记账**：中途 stop / 崩溃 / 限流退出不写台账，下次从 `control/<taskId>/state_control.json` 断点接着跑完再记
- 跳过判据 `control_already_done()`：台账里同 OSS key 的 `last_modified` 与本轮该份**相同** → 跳过（**连下载都不发生**）；
  同名文件在 OSS 上被覆盖（mtime 变化）→ 视为"新的一份"，重跑一次
- 空 CSV（无 nmID）也算跑完并记账，避免每轮重复下载
- 写入用临时文件 + `os.replace`，避免半截 JSON；读失败（损坏/不存在）当空台账处理
- `reset_state_for_new_round` 只对 treatment 的 run_dir 调用，control 断点不会被重置

---

## 9. 协调器调度策略（方案 A）

### 9.1 为什么需要协调器

Chrome profile 独占：同一 profile 同一时刻只能被一个 Chrome 进程打开。新旧模块共用 profile，必须串行运行。

### 9.2 调度规则

| 规则 | 说明 |
|---|---|
| **新模块未准备好** | 旧模块 100% 跑 |
| **新模块准备好后** | **天级轮转**：新模块每天 **00:00-12:00（12h）** 跑，旧模块跑 12:00-24:00（12h） |
| **比例可调** | 改协调器 `NEW_HOURS` / `NEW_START_HOUR` 后重启协调器生效（窗口不跨天） |
| **新模块窗口内未跑完** | 窗口外旧模块跑；新模块第二天窗口从断点继续 |
| **OSS 清空** | 旧模块 100% 跑 |

### 9.3 协调器逻辑

```python
# coordinator.py（实际代码要点，标准库 urllib，无新增依赖）
OLD_MODULE = "http://127.0.0.1:8080"
NEW_MODULE = "http://127.0.0.1:8081"
NEW_START_HOUR = 0        # 新模块每天窗口开始小时（可调）
NEW_HOURS = 12            # 新模块每天运行小时数（可调；旧模块跑剩余 24-NEW_HOURS 小时）
HEARTBEAT_FILE = "nmid_data/coordinator.heartbeat"  # 每循环写一次，供 check_health 判活

def _in_new_window(now):  # 天级窗口判定，边界切换
    return NEW_START_HOUR <= now.hour < NEW_START_HOUR + NEW_HOURS

def switch_to_new():      # 任一步失败返回 False
    _http_post(f"{OLD_MODULE}/stop?store=all")
    if not wait_worker_stopped(OLD_MODULE):   # 轮询 /status 确认 worker_alive=false
        return False      # 超时未停止 → 放弃本次切换，避免 profile 冲突
    return _http_post(f"{NEW_MODULE}/resume")

def switch_to_old():
    _http_post(f"{NEW_MODULE}/stop")
    if not wait_worker_stopped(NEW_MODULE):
        return False
    return _http_post(f"{OLD_MODULE}/resume?store=all")

# 守护：切换失败重试 3 次（_switch_with_retry）
#       停止等待超时 180s（覆盖旧模块优雅停止最坏：首捕 120s + 限流暂停 30s）
#       新模块窗口内每分钟补发旧模块 /stop，防旧宿主 AUTO_START/崩溃重启复活 worker
#       主循环 try/except 包裹，单次异常不杀死协调器进程
```

**切换机制：主循环决策（每 60s 一轮）**

1. 写心跳文件；
2. `_pending_new_tasks()`：新模块 `/status` 的 `pending_task_count == 0` → 无数据：若新 worker 活着则 `switch_to_old`，旧模块 100% 跑，本轮结束；
3. 有数据 + **在新窗口内**（`NEW_START_HOUR ≤ 当前小时 < NEW_START_HOUR + NEW_HOURS`）：
   - 旧 worker 活着 → 先补发旧模块 `/stop?store=all`（**持续压制**，防旧宿主 AUTO_START/崩溃重启复活 worker 抢 profile）；
   - 新 worker 不在跑 → `switch_to_new`；
4. 有数据 + **窗口外**：
   - 新 worker 活着 → `switch_to_old`；
   - 否则旧 worker 也不在跑 → 补发旧模块 `/resume?store=all`（旧模块猝死后拉活）；
5. sleep 60s；单轮异常被 try/except 接住，不致死协调器。

**单次切换的完整序列（以 OLD→NEW 为例，`switch_to_new`）**

1. `POST 旧模块/stop?store=all` → 失败则记 `FAILED(old /stop failed)` 返回；
2. `wait_worker_stopped(旧模块)`：每 2s 轮询 `/status` 直到 `worker_alive=false`，上限 `STOP_WAIT_TIMEOUT=180s` → 超时则**放弃本次切换**（记 `FAILED(old stop timeout)`），**不强行 resume 新模块**，避免两个 Chrome 抢同一 profile；
3. `POST 新模块/resume` → 失败则记 `FAILED(new /resume failed)` 返回；
4. 全程成功 → 记 `OK`（方向 `OLD->NEW`，备注窗口时段）。

`switch_to_old`（NEW→OLD）为镜像序列：新模块 `/stop` → 等停 180s → 旧模块 `/resume?store=all`。

**失败语义与留档**

- 任一步失败 → `_switch_with_retry` 重试 3 次（间隔 5s）；重试用尽交还主循环，**60s 后下轮自动再试**，期间不强行操作；
- 停止超时放弃切换后，可能出现两模块都停着的空窗：主循环下轮检测到“该跑的模块没在跑”会补发 resume 自愈；
- 每次切换（成/败）都追加一行到 `nmid_data/switch_history.log`：`时间戳 | 方向 | OK/FAILED | 备注`，供人工/脚本核对切换时刻；
- 180s 停止等待的依据：旧模块优雅停止最坏 = 首捕 CAPTURE_TIMEOUT 120s + 限流暂停 RATE_LIMIT_PAUSE 30s + 周期余量。

### 9.4 对旧模块的影响

- **代码零改动**：协调器只调用旧模块已有的 `/stop` `/resume` HTTP 接口
- **数据不丢失**：旧模块 stop 时保存断点，resume 从断点继续
- **变慢**：旧模块在新模块窗口（每天 12h）暂停，其余 12h 全速跑；断点续传不丢数据（已确认接受）
- **启动开销**：每次 resume 增加 10-30 秒（重启 Chrome + 选店 + 捕获请求）；天级窗口下每天仅 2 次切换（00:00/12:00），相对 12h 窗口占比 <0.1%，可忽略（旧文“约 2.8%”为分钟级轮转口径，已废弃）

### 9.5 快速切换方式

```powershell
# 手动切到新模块
curl -X POST "http://127.0.0.1:8080/stop?store=all"
curl -X POST "http://127.0.0.1:8081/resume"

# 手动切回旧模块
curl -X POST "http://127.0.0.1:8081/stop"
curl -X POST "http://127.0.0.1:8080/resume?store=all"
```

### 9.6 心跳与健康检测

- 协调器每轮主循环写一次 `nmid_data/coordinator.heartbeat`（内容为时间戳）
- 一键检测（Windows 机运行）：

```powershell
python -m nmid_fetch.check_health
```

- 检测项：
  - 旧模块（8080）：HTTP 存活 + worker 是否在跑
  - 新模块（8081）：HTTP 存活 + worker 是否在跑 + 待处理 task 数
  - 协调器：心跳文件年龄（<180s 视为存活）
  - 互斥检查：新旧 worker 同时为 true 时告警（profile 冲突风险）

### 9.7 开机自愈（Windows 计划任务）

协调器自愈（窗口内拉起死 worker）与计划任务（机器重启后拉起三个进程）是两层保护，缺一不可。三进程开机自愈覆盖现状：

| 进程 | 计划任务 | 机器重启后 |
|---|---|---|
| 老宿主 app.py(8080) | ✅ WB_FetchHost（登录自启 + 每 2 分钟看门狗） | 自愈 |
| 协调器 coordinator.py | ✅ 已注册（登录自启 + 每 2 分钟看门狗，照抄 WB_FetchHost；用户陈述 2026-09 完成） | 自愈 |
| 新宿主 app_nmid.py(8081) | ❌ 未注册（待补） | **不会自起** → 协调器活着也调不动 8081，新模块 + 天级轮转停摆 |

- 计划任务为机器级 schtasks 配置，仓库无对应提交；胶水 bat 若只存在于生产机有丢失风险，建议照 `run_fetch.bat` 模板入库
- app_nmid 需起 Chrome GUI，其计划任务属性必须“只在用户登录时运行”；coordinator 纯 HTTP 调度无 GUI 依赖

---

## 10. 效率与风险

### 效率

- **实测口径（2026-09-20 生产实测，主口径）**：稳态 **≈3.6s/nmID** = 1.5s 强制间隔 + ~2.1s 签名重放往返；含 Chrome 启动/首捕/偶发 429 的整任务均值 ≈4.9s/nmID（保守口径）；1.5s/nmID 仅为理论下限
- **上传不是瓶颈（实测）**：满 2000 条分片 6.68MB json → 转 csv → 上传 json+csv（共 7.8MB）→ 清理本地，全程 **1~2 秒**；30 万 = 150 个分片 ≈ 5 分钟，可忽略
- 瓶颈 = 逐条签名查询 + 1.5s 间隔；换算公式 = 规模 × 单条秒数 ÷ 每天窗口小时数
- 每天窗口时长换算（新模块单路串行，实测 3.6s/条）：

| 新模块窗口（NEW_HOURS） | 20 万需跑天数 | 30 万需跑天数 |
|---|---|---|
| 12h（现状） | ~16.7 天 | ~25 天 |
| 18h | ~11.1 天 | ~16.7 天 |
| 20h | ~10 天 | ~15 天 |

  （保守口径 4.9s/条：30 万 @12h ≈ 34 天；24h 连跑 @3.6s = 12.5 天，与早期估算吻合）
- 当前实际数据仅约数千 nmID，每天 12h 窗口内即可完成，窗口剩余时间继续下一轮循环（空转问题与暂缓决策见 §13）
- 批量查询（`filter.search` 分隔符拼多个 nmID）WB 是否支持**未验证**，不是“不支持”；评估结论与暂缓决策见 §13 决策记录

### 风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 崩溃丢失 | 分片 2000 次，崩溃最多丢 2000 次结果（实测约 1h50m） | 断点续传恢复 |
| 效率低 | 30 万条 ≈ 275-300h（实测 3.3-3.6s/条），@12h 窗口 ≈ 23-25 天 | 接受（缩短杠杆见 §10 效率） |
| WB 风控 | 长时间高频查询可能触发限流 | 复用现有 429 退避逻辑 |
| 登录态过期 | 长时间运行 token 可能过期 | 每次 resume 重新捕获请求头 |
| 旧模块变慢 | 新模块窗口内暂停，每天只跑 12h | 已确认接受 |
| 切换冲突 | Chrome 未完全关闭时切换 | 协调器轮询 /status 确认停止；**超时放弃本次切换**，不强行 resume |
| 旧模块复活 | 旧宿主重启（reboot/崩溃）AUTO_START=True 复活 worker，抢 profile | 新模块窗口内协调器每分钟补发 /stop 持续压制 + 停止等待 180s |
| 协调器猝死/切换失败 | 无进程级守护；单次 HTTP 失败可能错过切换 | 主循环 try/except 防猝死 + 心跳文件 + check_health 检测；切换失败重试 3 次后等下轮循环 |

---

## 11. 实施步骤

| 步骤 | 内容 | 状态 |
|---|---|---|
| 1 | 创建 `nmid_fetch/` 目录、`__init__.py`、`README.md` | ✅ 已完成 |
| 2 | 创建 `oss_input.py`：OSS 扫描/下载/解析 | ✅ 已完成 |
| 3 | 创建 `fetch_by_nmid.py`：查询循环 + 分片 + 断点 | ✅ 已完成 |
| 4 | 创建 `app_nmid.py`：Flask 宿主 + HTTP 接口 | ✅ 已完成 |
| 5 | 创建 `coordinator.py`：协调器（重试/心跳守护） | ✅ 已完成 |
| 5.5 | 创建 `check_health.py`：一键健康检测 | ✅ 已完成 |
| 5.6 | 代码推送远程 `nmid-fetch`（commit `53fce1e`） | ✅ 已完成 |
| 6 | 测试：小批量 nmID 验证全流程 | 待执行（Windows 机） |

---

## 12. 测试方案

| 阶段 | 内容 | 验证点 |
|---|---|---|
| 1. 单元验证 | `oss_input.py` 函数 | OSS 扫描、CSV 解析、店铺映射 |
| 2. 小批量端到端 | 10 个 nmID | Chrome 启动、选店、查询、分片、上传 |
| 3. 协调器切换 | 短周期（2 分钟） | 新旧模块交替、无冲突 |
| 3.5 健康检测与守护 | `python -m nmid_fetch.check_health`；手动杀协调器再重启 | 三方状态准确、互斥告警生效；协调器重启后调度自动恢复 |
| 4. 断点续传 | Ctrl+C 后 --resume | 从断点继续、不重复查询 |
| 5. 压力测试 | 真实 20 万 nmID | 内存、分片、上传稳定性 |

---

## 13. 决策记录：nmID 查询保持单条现状（2026-09 用户确认）

> **单独标注**：本节为冻结的决策记录。下列优化已评估但**暂缓不实施**；当前代码行为即最终现状，重启任一优化需重新评估并经用户确认。

### 13.1 现行语义确认（现状即设计）

| 情况 | 含义 | 现行处理 |
|---|---|---|
| call_api 异常 / 429 / 5xx / 其它非 200 | 请求级故障（暂态） | 对同一条 nmID 重试（10s / 30s / 指数退避 30s→300s / 5s），各有连败上限 |
| HTTP 200 + 空 cards | 商品不存在/已下架（业务级未命中） | **立即跳过、不重试、无额外等待**，推进下一条 |
| HTTP 200 + 非 JSON | WB 偶发脏应答 | 同样立即跳过、不重试 |
| 每次调用间隔 | `CALL_INTERVAL=1.5s` | WB 限流下限（60s/40 次），命中与未命中都付，**不是重试等待** |

### 13.2 已评估但暂缓的优化（均未写代码）

1. **批量查询**（`filter.search` 分隔符拼多个 nmID）：可把 1.5s 间隔与 ~2s 签名同时摊给 N 条（30 万单条实测 ~300h → N=20 约 17h，12h 窗口 ≈1.4 天）。前提：生产机实测 WB 是否支持分隔符（`;` / `,` / 空格）；上线需启动预检对照（单查 vs 合查），不一致自动降级单条，防静默全 miss 丢数据。
2. **命中校验补强**：现状 cards 非空即命中、整包入 buffer，未校验 card 的 nmID 与查询值一致；若 `filter.search` 为模糊匹配存在污染数据风险。补强方式 = 入库前按 card nmID 归属过滤。
3. **排序全扫 + 本地过滤**（备选路线）：成本与目标集合大小无关，命中率高时优于搜索模式；可按任务命中率自适应选择。

### 13.3 重启评估条件

- 生产机实测确认 `filter.search` 支持多值分隔符 → 评估 13.2.1；
- 发现数据混入无关商品（污染证据） → 立即实施 13.2.2；
- 目标集合与店铺目录命中率持续偏高 → 评估 13.2.3。

## 14. 抓取速率与日产量（业务说明）

> 一句话版：现在单店**每分钟抓 ~21–22 条、每天 ~30,000 条、其中有效 ~28,500 条**；撞到 WB 限流时系统**自动减速让路、限流过自动回全速**，全程不需要人管；速度在"单店串行"前提下已到顶，**要加产量只能加店铺并行**。
> 数据来源：生产日志与 bench 实测（2026-09-21/22）；工程实现见 commit 2918257。14.1–14.5 是业务结论；全部底层数据、模型与推导口径在附录 14.6，供技术审查。

### 14.1 一天能抓多少

| 指标 | 当前实测 |
|---|---|
| 抓取速度 | ~21–22 条/分钟 |
| 日处理量 | ~30,000 条 |
| 日有效数据（命中率 ~95%） | ~28,500 条 |
| 日产量波动区间 | ~28,000–31,000 条（WB 服务器响应速度随时段变化：早上快、晚上慢） |

### 14.2 为什么不是更快或更慢

WB 对我们的请求限流：请求太密就返回"429 限流"并让我们**罚站 30 秒**。所以速度不是想快就快：

- **更快**：罚站更频繁——每次白等 30 秒，反而更慢，还伤账号健康；
- **更慢**：每条白白排队——实测对比：每条间隔从 2.0s 放慢到 3.0s，罚站率只从 ~1.5% 降到 <1%，日产量却从 ~29,000 掉到 ~26,000 条。**慢买来的稳定远小于丢掉的速度，不划算**；
- **当前选择**：默认**全速跑**（间隔 1.6s），**撞限流自动减速让路、限流过自动回全速**。

### 14.3 改动前后对比：从"硬撞限流"到"撞了自动让路"

改动前（固定间隔）：

- 不管撞不撞限流都按同一个节奏冲；罚站 30 秒结束后立刻按原节奏再冲，**可能连着撞**，每次白等 30 秒；
- WB 限流严松随时段变（凌晨松、早晚高峰严），固定间隔只能赌一个时段，高峰"太快"、凌晨"太慢"。

改动后（撞限流自动让路，即"闭环退避"，已在生产运行）：

1. 平时全速跑；
2. **撞一次限流立刻慢一档**（间隔 1.6s → 2.4s，一步到位），不给连撞机会；
3. **连续 50 条没撞限流就试着快一点**（每次回收 5%），直到回到全速；全速就是 1.6s 下限——**回收算出来比 1.6s 更低也会拉回 1.6s**，不会跑得比这更快。

实测效果：

- **限流命中率降约三分之一**（同时段对比：2.05% → 1.34%；高峰时优势更大）；
- **速度不变**：仍 ~21 条/分——少罚站省下的时间 ≥ 跑慢一点多花的时间。这是账号健康上的净赚，不是拿速度换的；
- **零人工调参**：WB 限流严松随时段漂移，系统自己跟着调，任何时段都跑在"刚好"的节奏上。

### 14.4 可靠性保证

- **撞限流不丢数据**：撞到的条自动重试，已抓进度实时落盘；
- **断电/重启自动续传**：不用重抓；
- **多店铺并行**：每店独立账号和浏览器，日产量按店铺数线性叠加（单店 ~30,000 条/天是串行上限）。

### 14.5 唯一一个还在验证的数字（诚实披露）

早高峰（8–11 点）是唯一还没和老策略对账完的时段：这时 WB 限流最严，当前"自动减速"做法在这里是否最优，需要一整天的数据裁定（预计 09-23 11:00 前后攒满）。若数据显示早高峰有损失，修复方向已备好（早高峰不主动减速），改动仅一行代码、不影响业务。对账方法与进度见附录 14.6.4。

最终对账方式（已定）：闭环跑满一天后，再切固定 1.6 跑一天，**天 vs 天直接对账**。预期结果＝本次优化的成功签名：**两天日处理量基本相同（差 ≤5%），闭环那天限流次数明显更少（预期少 ≥30%）**——"产量不变、限流变少"两者缺一不可：产量平但限流没少＝退避白做；限流少但产量明显掉＝退得过度。注意不要求精确相等：WB 服务器状态逐日不同，单日对比自带 ±几个点噪声，故用容忍带 + 逐小时桶判定。

### 14.6 附录：实测数据与推导口径（技术审查用）

#### 14.6.1 速率表与间隔模型

| 请求间隔 | 净速率 | 日处理量 | 日有效数据（命中率 ~95%） | 备注 |
|---|---|---|---|---|
| **1.6s（当前采用·实测）** | **~22 条/分** | **~30,000 条** | **~28,500 条** | 吞吐最高 |
| 2.0s（实测） | ~21.5 条/分 | ~29,000 条 | ~27,500 条 | 常态略慢 |
| 2.5s（推算） | ~21 条/分 | ~28,000 条 | ~26,500 条 | 更慢 |
| 3.0s（推算） | ~19 条/分 | ~26,000 条 | ~24,500 条 | 最慢 |

注：1.6s / 2.0s 为生产实测，2.5s / 3.0s 按间隔模型推算；日产量按 24h 连续运行、命中率 95% 计。

推算模型：净速率 = 60 ÷ ( max(interval, dt) + 429率 × 30 )。三个实测输入：① 单请求耗时 dt（早上 ~1.75s、晚上 ~2.2–2.3s，bench p50=2.18s）；② 罚停常数 30s/次；③ 429 率随间隔单调递减（实测锚点 1.6s→≈1.9%、2.0s→≈1.5%）。自校验：同一公式代入 1.6s / 2.0s 得 ~21–22 / ~21.5 条/分，与实测误差 <5%。

名义与实测：名义规则为 60 秒 40 次，但实测有效容忍度更低且随时段漂移（早松晚紧）：~18.5 次/分已出现零星 429（≈0.15%）。故"60s/40 次"只是名义上限；floor 1.6s / cap 2.4s 的取值依据是实测容忍度，真实边界由闭环退避实时跟踪。

#### 14.6.2 限流命中密度实测

| 窗口 | 配置 | 时段 | 429 密度 | 实际节奏 |
|---|---|---|---|---|
| 09-21 22:00–09-22 10:00（12h） | 固定 1.6 | 夜间+早高峰 | 1.92%（298 次 / ~15,500 条） | ≈dt（1.7–2.3s） |
| 其中凌晨低谷 04–05 | 固定 1.6 | 凌晨 | 0.95–1.18% | ≈2.25s |
| 其中早高峰 08–10 | 固定 1.6 | 早高峰 | 2.67–2.73%（每 37–38 条 1 次） | ≈1.7s |
| 晚间同窗 A/B | 固定 1.6 → 固定 2.0 | 晚间 | 2.05% → 1.34%（−35%） | 均 ≈2.28s |
| 09-22 午间窗口（2,700 条） | 闭环 | 午间 | 1.48%（每 ~68 条 1 次） | 2.28↔2.40 |
| 午间节奏采样（55 个） | 闭环 | 午间 | — | 2.40s×71%、2.28s×27%、1.6s×1 |

罚停代价（固定 1.6、12h）：298 × 30s ≈ 2.5h，约占产能 16–19%。限流强度随时段漂移（早高峰 ≈ 凌晨低谷的 3 倍）→ 任何固定间隔都不可能全时段最优，这是闭环退避存在的实测依据。

#### 14.6.3 高峰边际账（2026-09-22 记录）

判据：每行有效耗时 = pacing + 429率 × 30s（pacing = max(base 间隔, dt)）；抬间隔划算的条件：Δ(429率) × 30 > Δ间隔。

| base 间隔 | 429率 | pacing | 罚站税 | 有效 s/行 |
|---|---|---|---|---|
| 2.28（回收态） | 2.0%（实测，每 50 行 1 次） | 2.28 | 0.60 | 2.88 |
| 2.40（当前 cap） | 1.42%（实测，每 71 行 1 次） | 2.40 | 0.42 | 2.82 |
| ~2.60（外推） | ~0.6% | 2.60 | 0.18 | ~2.78 |
| ~2.70（外推，429→0） | ~0% | 2.70 | ~0 | ~2.70 ← 模型最优 |
| 3.00（外推） | 0% | 3.00 | 0 | 3.00（过头） |

斜率由正午实测两点（2.28→2.40）标定 ≈ −4.8%/s。模型结论：有效耗时在 ~2.7s 触底，比 cap 2.4 快 ~4–5%、比 3.0 快 ~10%。**注意：本账仅在 base > dt 的时段（正午/夜间）成立；早高峰 dt≈1.7s 的情形见 14.6.4。**

#### 14.6.4 对账 1.6 基线与对账节奏

表中值 = 每行有效耗时（越小越好）；实测 = 逐小时桶直接换算，外推 = 模型，待验证 = 需对照实验裁定。

| 时段（dt） | 429率@1.6 | 固定 1.6（吃 30s） | 当前闭环（cap 2.4） | 固定 2.7（参考） |
|---|---|---|---|---|
| 早高峰 08–11（dt≈1.7） | 2.7% | **2.48（实测**，1450–1500 行/h） | 2.4–2.8（外推，待验证） | 2.70 |
| 正午/夜间高峰 22–03（dt≈2.25） | ~2.1% | ~2.78（实测，1250–1400 行/h） | ≈2.82（外推） | ~2.75 |
| 凌晨低谷 04–05（dt≈2.25） | ~1.0% | ~2.55（实测，1050–1100 行/h） | ≈2.55（base 到不了 cap） | 2.70 |
| 平峰白天（dt≈1.9） | ~1.5% | ~2.35（外推） | ≈2.3–2.5（外推） | 2.70 |

对账结论：

1. **对任何固定 ≥2.4s 间隔：当前闭环全时段不输**——"随时段自适应"是核心优势；
2. **对固定 1.6：优劣分时段，早高峰是唯一悬念**——正午/夜间小赢 ~2–3%、凌晨打平；早高峰取决于 2.7% 的 429 是否对间隔同样敏感，现有数据区分不了；
3. **日级**：各时段互抵，闭环 vs 固定 1.6 吞吐 ≈ 持平；真实价值在鲁棒性（打断连撞链、自动适应、零人工调参）；
4. **对账节奏：按天、分三步**。固定 1.6 基线来自 log.old（至少 09-21 21:00 – 09-22 11:16 ≈14h，起点是否更早待生产机确认）；闭环自 09-22 11:16 起攒第一个完整天（预计 09-23 11:00 前后跑满）。① 闭环满一天后出逐小时桶 + 日总量；② 先对重叠时段（21–11h，早高峰在内）与基线对账，缺失时段按模型推算（预期打平）；③ 仅当 ② 显示早高峰损失 ≥5% 时，申请生产对照跑一天固定 1.6 补真·全天 A/B，否则 ② 即结论。若损失坐实，候选修复 = "dt < 2.0 时不抬 base" 或分时段 cap。全天 A/B 通过线（若执行）：日处理量两天差 ≤5%；闭环日 429 次数比固定 1.6 日少 ≥30%；逐小时桶 22–03 时闭环 ≥ 基线、08–11 时跌幅 ≤5%。

工程参数（commit 2918257，fetch_by_nmid.py）：floor `CALL_INTERVAL=1.6`、cap `BACKOFF_CAP=2.4`、撞 429 `base×1.5` 一步到顶 + 停 30s、每 50 条连续干净 `base×0.95` 慢回收且 `max(…, 1.6)` 钳制不低于 floor（算出比 1.6 低会改回 1.6）；5xx/异常不参与退避。观测点：nmid_data/app_nmid.log 的 `[RATE-LIMIT]` 行、`[PROGRESS]` 行尾 `间隔 X.XXs`、`/status` 的 `interval` 字段。
