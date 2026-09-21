# 技术方案：按 nmID 集合抓取 WB 商品数据（nmid_fetch 模块）

> 状态：代码已完成并推送（分支 `nmid-fetch`，commit `53fce1e`），待 Windows 部署测试
> 原则：零侵入（不改动任何现有代码）、最大复用、格式一致、可续传、串行调度

---

## 1. 目标

新增独立模块 `nmid_fetch/`，实现：

- 常驻 Flask 服务，循环扫描 OSS `content-opt-pool/` 目录
- 下载各 taskId 下的店铺 CSV（含 nmID 列表）
- 对每个 nmID 调 `tableListv6` 接口（`filter.search = nmID`）查询商品数据
- 每 2000 次查询写一个分片 JSON，异步转 CSV 并上传 OSS
- 所有文件处理完立即开始下一轮循环
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
| **循环处理** | 常驻服务，处理完所有 taskId 立即重新开始 |
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
├── store1/
│   ├── <taskId>/
│   │   ├── <taskId>_<storeId>_shard_NNN_<run_ts>.json
│   │   ├── csv/
│   │   │   └── <taskId>_<storeId>_shard_NNN_<run_ts>.csv
│   │   ├── state.json          # 断点状态
│   │   └── summary.json        # 汇总
│   └── fetch_by_nmid.lock      # PID 锁
└── store2/
    └── ...
```

---

## 4. 输入格式

### OSS 输入路径

```
content-opt-pool/
├── <taskId_1>/
│   ├── <storeId_1>.csv    ← store1 的 nmID 列表（文件名 = WB 数字卖家 ID）
│   └── <storeId_2>.csv    ← store2 的 nmID 列表
├── <taskId_2>/
│   └── ...
```

### CSV 文件格式

```csv
nm_id
13334444
13334445
```

- 只有一列 `nm_id`，每行一个 nmID
- 文件名 = WB 数字卖家 ID（如 `250132124.csv`）

### 店铺映射

CSV 文件名（店铺 ID）→ 反查 `config.json` → 得到 `store1`/`store2`：

```json
{"store1": 250132124, "store2": 250149024}
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
{taskId}_{storeId}_shard_{NNN}_{run_ts}.json
{taskId}_{storeId}_shard_{NNN}_{run_ts}.csv
```

- `run_ts`：本次运行时间戳 `YYYYMMDD_HHMMSS`，每轮产生新文件，**不覆盖旧文件**

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
app_nmid.py 启动（Flask + waitress，端口 8081）
    │
    ▼
while True:  # 常驻循环
    ├─ list OSS content-opt-pool/ 下所有 taskId
    ├─ 对每个 taskId：
    │   ├─ list 该 taskId 下所有 *.csv
    │   ├─ 对每个 CSV（文件名 = storeId）：
    │   │   ├─ 反查 config.json → store_id
    │   │   ├─ 下载 CSV → 解析 nm_id 列
    │   │   ├─ 获取 PID 锁（nmid_data/<store>/fetch_by_nmid.lock）
    │   │   ├─ 清理残留 Chrome → 启动 Chrome → 打开 WB 页面
    │   │   ├─ 自动选店 + 复核 → 捕获首条 tableListv6 请求
    │   │   ├─ 读取断点 state.json → 恢复已处理索引
    │   │   ├─ 对每个 nmID（从断点开始）：
    │   │   │   ├─ 构造请求体：filter.search = nmID
    │   │   │   ├─ 调 tableListv6（call_api）
    │   │   │   ├─ 未命中 → 直接丢弃
    │   │   │   ├─ 命中 → 加入 buffer
    │   │   │   ├─ 每 2000 次查询 → 写分片 → 异步转 CSV + 上传 OSS
    │   │   │   └─ 更新 state.json
    │   │   ├─ 写 summary.json → 释放锁 → 关闭 Chrome
    │   └─ 该 taskId 全部店铺处理完
    ├─ 所有 taskId 处理完
    └─ 立即开始下一轮（不 sleep）
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

`state.json`：

```json
{
  "task_id": "task_001",
  "store_id": "250132124",
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
- `summary.json` 记录 `query_count` / `matched_count` / `missed_count`

### 8.6 数据安全（暂停不丢失）

- stop 时 `finally` 块把 buffer 剩余数据写入分片
- stop 时保存 `state.json` 断点
- stop 时等待所有异步上传线程完成（`join(timeout=30)`）
- resume 时从 `processed_index` 继续，不重复查询

### 8.7 下载过滤（只处理已配置的两店）

- 每轮实时列 OSS：`list_task_ids()` 取 `content-opt-pool/` 一级目录；`list_store_csvs(tid)` 只列 `.csv` 后缀文件（manifest.json 等非 CSV 直接忽略）
- CSV 文件名 = WB 数字 sellerId；`store_id_for_seller()` 按 `config.json` 反查店铺（当前仅 store1=250132124、store2=250149024）
- **非这两个店铺**（sellerId 不在 config.json）：打印 `[WARN] sellerId=… 未在 config.json 配置，跳过`，**不下载、不处理**，继续下一个 CSV
- 店铺锁被其他进程占用：本轮跳过该 CSV，下一轮重试
- 因此在 OSS 增删 CSV 只影响"本轮跑哪些店"；未配置店铺的文件永远不会被拉取到本地

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
