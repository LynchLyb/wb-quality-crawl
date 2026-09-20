# 技术方案：按 nmID 集合抓取 WB 商品数据（nmid_fetch 模块）

> 状态：设计评审中（分支 `nmid-fetch`）
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
└── coordinator.py           # 外部协调器：新旧模块时间片轮转调度
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
- 风险：崩溃时最多丢失 2000 次查询结果（约 50 分钟）

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

---

## 9. 协调器调度策略（方案 A）

### 9.1 为什么需要协调器

Chrome profile 独占：同一 profile 同一时刻只能被一个 Chrome 进程打开。新旧模块共用 profile，必须串行运行。

### 9.2 调度规则

| 规则 | 说明 |
|---|---|
| **新模块未准备好** | 旧模块 100% 跑 |
| **新模块准备好后** | 按 **80:20 时间片**轮转（新 48 分钟 / 旧 12 分钟，周期 60 分钟） |
| **每天新模块优先** | 每天 00:00 优先启动新模块 |
| **新模块当天完成** | 剩余时间旧模块 100% 跑 |
| **新模块跨天未完成** | 第二天继续 80:20 轮转 |

### 9.3 协调器逻辑

```python
# coordinator.py
OLD_MODULE = "http://127.0.0.1:8080"
NEW_MODULE = "http://127.0.0.1:8081"
NEW_RATIO = 0.8
CYCLE_MINUTES = 60

def has_new_data():
    """调新模块 /status，检查是否有待处理任务"""
    r = requests.get(f"{NEW_MODULE}/status", timeout=5)
    return r.json().get("pending_task_count", 0) > 0

def switch_to_new():
    requests.post(f"{OLD_MODULE}/stop?store=all")
    wait_worker_stopped(OLD_MODULE)   # 轮询 /status 确认 worker_alive=false
    requests.post(f"{NEW_MODULE}/resume")

def switch_to_old():
    requests.post(f"{NEW_MODULE}/stop")
    wait_worker_stopped(NEW_MODULE)
    requests.post(f"{OLD_MODULE}/resume?store=all")
```

### 9.4 对旧模块的影响

- **代码零改动**：协调器只调用旧模块已有的 `/stop` `/resume` HTTP 接口
- **数据不丢失**：旧模块 stop 时保存断点，resume 从断点继续
- **变慢 5 倍**：旧模块只有 20% 时间跑（已确认接受）
- **启动开销**：每次 resume 增加 10-30 秒（重启 Chrome + 选店 + 捕获请求），占比约 2.8%

### 9.5 快速切换方式

```powershell
# 手动切到新模块
curl -X POST "http://127.0.0.1:8080/stop?store=all"
curl -X POST "http://127.0.0.1:8081/resume"

# 手动切回旧模块
curl -X POST "http://127.0.0.1:8081/stop"
curl -X POST "http://127.0.0.1:8080/resume?store=all"
```

---

## 10. 效率与风险

### 效率

- `CALL_INTERVAL = 1.5` 秒（限流 60 秒 40 次）
- 20 万 nmID × 1.5 秒 ≈ **83.3 小时/店铺**
- 新模块占 80% 时间 → 实际需要 **104 小时 ≈ 4.3 天/店铺**
- 接口不支持批量查询，无法优化

### 风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 崩溃丢失 | 分片 2000 次，崩溃最多丢 2000 次结果 | 断点续传恢复 |
| 效率低 | 83.3 小时/店铺 | 接受 |
| WB 风控 | 长时间高频查询可能触发限流 | 复用现有 429 退避逻辑 |
| 登录态过期 | 长时间运行 token 可能过期 | 每次 resume 重新捕获请求头 |
| 旧模块变慢 | 只有 20% 时间跑 | 已确认接受 |
| 切换冲突 | Chrome 未完全关闭时切换 | 协调器轮询 /status 确认停止 |

---

## 11. 实施步骤

| 步骤 | 内容 | 状态 |
|---|---|---|
| 1 | 创建 `nmid_fetch/` 目录、`__init__.py`、`README.md` | ✅ 已完成 |
| 2 | 创建 `oss_input.py`：OSS 扫描/下载/解析 | 待执行 |
| 3 | 创建 `fetch_by_nmid.py`：查询循环 + 分片 + 断点 | 待执行 |
| 4 | 创建 `app_nmid.py`：Flask 宿主 + HTTP 接口 | 待执行 |
| 5 | 创建 `coordinator.py`：协调器 | 待执行 |
| 6 | 测试：小批量 nmID 验证全流程 | 待执行 |

---

## 12. 测试方案

| 阶段 | 内容 | 验证点 |
|---|---|---|
| 1. 单元验证 | `oss_input.py` 函数 | OSS 扫描、CSV 解析、店铺映射 |
| 2. 小批量端到端 | 10 个 nmID | Chrome 启动、选店、查询、分片、上传 |
| 3. 协调器切换 | 短周期（2 分钟） | 新旧模块交替、无冲突 |
| 4. 断点续传 | Ctrl+C 后 --resume | 从断点继续、不重复查询 |
| 5. 压力测试 | 真实 20 万 nmID | 内存、分片、上传稳定性 |
