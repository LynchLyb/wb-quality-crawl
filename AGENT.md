# AGENT.md · WB 卖家数据抓取 — Agent 快速上手

> 给在**新电脑**上克隆本仓库、要尽快把抓取跑起来的 AI agent / 开发者看：一句话定位 + 硬约束 + 上手步骤 + 必知的坑。
> 深度设计见项目根 `方案.md`；完整操作手册与**故障速查表**见 `.qoder/skills/wb-seller-data-fetch/SKILL.md`。

## 0. 这是什么

在 **Windows 本机**用「带真实登录态的 Chrome」打开 WB 卖家后台商品页，捕获一次 `tableListv6` 真实请求后，在页面上下文用 `fetch` 循环重放翻页，**零渲染**全量拉取商品数据（10 万级以上）→ 分片落盘 → 转 32 列 CSV → 上传阿里云 OSS。常驻 Flask 宿主 + APScheduler 巡检自愈做无人值守；支持**多店铺并行**与**抓取前按 `config.json` 自动选店**（防爬错店）。

## 1. 硬约束（违反必翻车，先读）

- **只能跑 Windows 本机**：登录 cookie 由 Windows DPAPI 加密、绑定本机当前用户，**不能进 Docker/Linux**。
- **profile 不能跨机复制**：`chrome_profile/`、`profiles/` 换到别的电脑解不开。**换新机 = 每个店重新人工登录一次**（`login_store.py`）。本仓库不含任何登录态（已 gitignore）。
- **Chrome 136+**：禁止对日常 Chrome 默认 User Data 目录远程调试 → 必须用项目内独立 profile。
- **鉴权头实时捕获**：`AuthorizeV3` / `Wb-Seller-Lk` 会过期，**严禁硬编码**。
- **游标链串行**：分页 `cursor` 是串行依赖，**不可多线程拉同一条链**。
- **`CALL_INTERVAL` ≥ 1s**：否则频繁触发 429 限流。

## 2. 新机器上手（按顺序）

前置：Windows + Chrome + Python（项目在 **3.14** 验证，建议 3.11+）+ git。

**① 虚拟环境 + 依赖**

```powershell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -U pip
.venv\Scripts\pip install -r requirements.txt
```

依赖：flask / waitress / apscheduler / selenium / oss2（详见 `requirements.txt`）。

**② chromedriver.exe**（**已 gitignore，不在仓库**）：下载与本机 Chrome 大版本匹配的驱动放到项目根（Chrome for Testing 提供下载）。

**③ 两个运行配置**（从 example 复制后填真值；**真值文件已 gitignore，切勿提交**）

```powershell
copy config.json.example config.json
copy oss_config.json.example oss_config.json
```

- `config.json`：每店 WB **数字卖家 ID**，如 `{"store1":250132124,"store2":250149024}`，用于抓取前自动选店。ID 在 WB 后台店铺切换下拉框 “ID <数字>” 处读取。
- `oss_config.json`：`access_key_id` / `access_key_secret` / `endpoint` / `bucket` / `region`（`region` 供 OSS **AuthV4** 签名）。可先用 `oss_conn_test.py` 自检连通性与权限。

**④ 每店首次人工登录**（DPAPI 决定无法自动；生成该机专属 profile）

```powershell
.venv\Scripts\python.exe login_store.py store1   # 弹浏览器 → 手动登录 → 回车关闭
.venv\Scripts\python.exe login_store.py store2
```

store1 落到 `chrome_profile/`，其余店落到 `profiles/<id>/`（以 `config.py` 的 `STORES` 为准）。

**⑤ 起常驻宿主**（Flask，默认 `127.0.0.1:8080`）

```powershell
.venv\Scripts\python.exe -u app.py
```

`AUTO_START=True` 时宿主一起来就自动 `--resume` 拉起各店 worker。

## 3. 运行与控制（HTTP 接口）

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/health` | 存活探针 + 店铺清单 |
| GET | `/status?store=<id>` | 单店快照；不带 `store` 返回全部店（worker 存活 / restart_count / call_count / card_count / cursor / shard_index / status / last_error）|
| POST | `/start?store=<id>&resume=1` | 启动某店（`resume=0` 全新）；`store` 缺省=DEFAULT_STORE，`all`=全部；已运行返回 409 |
| POST | `/resume?store=<id>` | 等价 `/start?resume=1` |
| POST | `/stop?store=<id>` | 优雅停止：本调用周期结束后存断点退出，该店巡检不再自动重启 |

```powershell
Invoke-RestMethod http://127.0.0.1:8080/status | ConvertTo-Json -Depth 6
Invoke-RestMethod -Method POST http://127.0.0.1:8080/stop?store=store2
```

单店 CLI 也可独立跑：`.venv\Scripts\python.exe -u fetch_all.py [--resume] [--store <id>]`（宿主在跑时会被每店 PID 锁 `[LOCK]` 挡下，防撞车）。

## 4. 文件地图

| 文件 | 作用 |
|---|---|
| `fetch_all.py` | 主脚本：直连拉取 + 分片 + 断点续传；`run_fetch()` 供宿主调用；采集前 `ensure_selected_store` 自动选店、切换后 `verify_selected_store` 复核 |
| `script.py` | 提供 `create_driver(headless, profile_dir)`（**fetch_all 依赖，勿删**）；含滚动方案（备用）|
| `app.py` | Flask 宿主：多店 supervisor + 路由 + `waitress.serve` |
| `supervisor.py` | `FetchSupervisor`：每店一 worker 线程 + `threading.Lock` + APScheduler cron 巡检自愈 |
| `config.py` | 宿主配置：HOST/PORT/CRON_EXPR/AUTO_START/DEFAULT_STORE + `STORES` 多店注册表 + `get_seller_id()` |
| `wb_to_oss.py` | 分片 → 32 列 CSV → 上传 OSS（`oss_target` 三元组 + 上传当天 BJT 日期目录 + 店铺段）|
| `login_store.py` | 新店一次性人工登录，cookie 落到该店独立 profile |
| `oss_conn_test.py` | OSS 连通性/权限自检（命令行 / 环境变量 / 配置文件 三种凭据来源）|
| `_count_nmid.py` / `_diag_oss.py` / `_recover_cursor.py` | nmID 统计 / OSS 诊断 / 游标恢复 小工具 |
| `create_wb_product_cards.sql` | 商品数据落库建表 SQL（对应 tableListv6 的 32 列 schema）|
| `config.json`、`oss_config.json` | 运行配置（真值 gitignore；`.example` 为模板）|
| `方案.md` | 完整设计 + 运维手册（架构演进、线程与锁、部署、已知限制）|
| `.qoder/skills/wb-seller-data-fetch/` | 本项目 skill：操作手册 + **故障速查表** + `scripts/run_fetch.bat` 模板 |

## 5. Agent 改代码前必知的坑

- **print 里禁用 emoji**（✓ / ✅ / ❌ / 🔁）：Windows 宿主 stdout 常是 GBK，非 GBK 字符会直接 `UnicodeEncodeError` 崩掉 worker；`fetch_all.py` 顶部已 `reconfigure(utf-8, errors=replace, line_buffering=True)` 兜底，日志标记一律用 ASCII（`[OK]` / `[X]` / `*`）。中文可编码，不受影响。
- **Flask 不能开 debug/reloader**（模块双加载 → 双 scheduler / 双 worker）；`waitress.serve(app, ...)` 直接传对象，不走导入字符串。
- **Selenium driver 线程亲和**：worker 线程内独占 `create_driver → 用 → quit`，绝不跨线程。
- **profile 目录名勿互为前缀**（用 `profiles/storeN`）；`pre_launch_cleanup` 按路径边界只清本店残留 chrome，别 blanket-kill 全部 chromedriver（会误伤并行兄弟店）。
- **自动选店只用稳定选择器**（`data-testid` / `input[type=radio][name=supplier]` / `data-name="Text"` / “ID <数字>” 正则），**避开 WB 构建期哈希 class**（改版即失效）；**不读 cookie/JWT**（子账号共享）。切换必须在**捕获请求之前**完成。
- **选店失败是终态、不自动重启**：`store_not_available`（下拉框无此店）/ `store_mismatch`（切换后复核不符）/ `store_verify_failed`（展不开或点不动）→ 需人工确认店铺后 `/resume`。

## 6. 提交规范（本仓库）

- **绝不提交**密钥 / 登录态 / 数据 / 大文件：`oss_config.json`、`config.json`、`chrome_profile/`、`profiles/`、`data/`、`tableListv6_*/`、`*.log`、`chromedriver.exe`、`.venv/`（均已在 `.gitignore`）。改动 `.gitignore` 后用 `git add -A --dry-run` + `git check-ignore -v <文件>` 复核。
- **`.gitignore` 不支持行尾注释**：`#` 必须独占一行，否则整行会被当成匹配模式而失效（本仓库曾因此差点漏提交密钥）。
- 进版本库的只有：源码 / 文档 / `.example` 模板 / `.qoder/skills`。

## 7. 深入阅读

- `方案.md`：数据源、方案演进（踩坑）、最终架构、线程与锁正确性、两种部署（bat 定时 / Flask 常驻）、已知限制、文件清单。
- `.qoder/skills/wb-seller-data-fetch/SKILL.md`：操作手册 + 故障速查（401 / 429 / 500 / DevToolsActivePort / capture_timeout / GBK 崩溃 / 0 字节日志 / 选店失败 等）。
