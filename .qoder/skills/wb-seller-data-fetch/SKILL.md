---
name: wb-seller-data-fetch
description: Wildberries 卖家后台商品数据全量拉取工作流：运行/续传 fetch_all.py 直连 tableListv6 接口、分片落盘、nmID 统计、Flask 常驻宿主（app.py + supervisor + APScheduler cron 巡检自愈）、多店铺并行（每店独立 Chrome profile/锁/断点/OSS prefix 段）、抓取前自动选店（按 config.json 切到目标店防爬错店）、注册 Windows 定时任务，以及 401/429/DevToolsActivePort 等故障排查。Use when 用户提到 Wildberries、WB 卖家后台、tableListv6、商品数据拉取/续传/统计、Flask 常驻宿主/巡检自愈、多店铺/多账号并行抓取，或需要为拉取任务配置定时调度。
---

# WB 卖家后台商品数据拉取

完整背景与方案演进见项目根目录 `方案.md`。本 skill 是操作手册。

## 核心事实（勿凭直觉改动）

- 接口：`POST https://seller-content.wildberries.ru/ns/viewer/content-card/viewer/tableListv6`，鉴权靠请求头 `AuthorizeV3` / `Wb-Seller-Lk`（**每次运行从页面实时捕获**，勿硬编码）。
- 分页游标在**请求体** `cursor` 对象里（含时间戳 `value` 与 `nmID`），响应 `data.cursor` 原样回填翻页；游标链是串行依赖，**不可多线程并行拉同一条链**。
- Chrome 136+ 禁止对默认 User Data 目录远程调试，因此必须用项目内独立 `chrome_profile/`（从日常 Chrome 整目录复制，含 `Local State`，DPAPI 绑定本机用户，**不可迁移到 Linux/Docker**）。
- 调用间隔 <1s 会频繁触发 429 限流；当前 `CALL_INTERVAL=1.0`（偶发 429 由脚本自动暂停 30s 重试，实测可接受）。
- `fetch_all.py` 依赖 `script.py` 的 `create_driver`，两个文件都不能删。
- **抓取前自动选店**：多店共用子账号、且 store1/store2 在 WB 下拉框里**显示名完全相同**，无法靠名字区分；`fetch_all.py` 在**捕获 tableListv6 请求之前**先 JS 点开店铺切换下拉，按 `config.json` 的数字卖家 ID 找到目标行，若当前 `checked` 不是它则**自动点击切换**（切换会改供应商上下文，故必须在捕获前完成；切换后清旧店 perf 日志+重载页面再捕获），捕获后再只读复核一次。失败码：目标店不在下拉框→`store_not_available`，切换后仍不符→`store_mismatch`，展不开/读不到→`store_verify_failed`，均为终态不重启。**不依赖 cookie**（子账号共享）。

## 工作流

### 1. 全新拉取

```powershell
# 前置：确认无残留进程（有则先 Stop-Process）
Get-Process chrome,chromedriver -ErrorAction SilentlyContinue
# 运行（-u 保证日志实时输出）
.venv\Scripts\python.exe -u fetch_all.py
```

输出目录 `tableListv6_<启动时间戳>/`：每 100 次调用一个 `*_shard_NNN_*.json` 分片，`state.json` 为断点，`summary.json` 为汇总。

### 2. 断点续传（崩溃 / 超时 / Ctrl+C 之后）

```powershell
.venv\Scripts\python.exe -u fetch_all.py --resume
```

自动找最近未完成目录，恢复游标、计数与分片编号续写。旧目录无 `state.json` 时会从最后一个分片的最后一条响应恢复游标。

### 3. 统计结果（nmID 总数/去重）

用 `_count_nmid.py`，先把其中 `FILE` 指向目标分片；统计整轮需遍历目录下全部 shard。数据按 `nmID` 去重（续传边界可能重叠一页）。

### 4. 注册定时任务（无人值守 · 旧 bat 方案 A）

> 推荐改用下方「### 5. Flask 常驻宿主模式（方案 B）」：cron 巡检自愈取代 bat 的重试循环。以下为旧 bat 胶水层方案。

1. 把 [scripts/run_fetch.bat](scripts/run_fetch.bat) 复制到项目根目录；
2. 注册（时间按需改）：

```powershell
schtasks /Create /TN "WB_FetchAll" /TR "C:\Users\yuanbo\PycharmProjects\WelcomeScreen\run_fetch.bat" /SC DAILY /ST 03:00 /F
```

3. 任务属性必须为 **"只在用户登录时运行"**（Chrome GUI 无法在会话 0 启动）。

bat 职责：搭环境（venv/编码/工作目录）→ 日志落盘 `logs\` → 检查 `state.json.finished`，未完成自动 `--resume` 重试（最多 3 轮）→ 锁文件防重叠。注意：bat 异常中断可能残留 `%TEMP%\wb_fetch_all.lock`，导致后续任务空跑，排查时先删锁。

### 5. Flask 常驻宿主模式（推荐无人值守）

把 `fetch_all` 装进常驻 Flask 宿主进程（**非 Docker**，DPAPI/Chrome 约束不变）：后台 worker 线程拉取 + APScheduler cron 守护线程巡检自愈，`waitress` 单进程承载。新增文件：`config.py`（HOST/PORT/CRON_EXPR/AUTO_START）、`supervisor.py`（FetchSupervisor）、`app.py`（路由+waitress）、`requirements.txt`。

**启动**（`AUTO_START=True` 时宿主一起来就 `--resume` 拉起 worker）：

```powershell
.venv\Scripts\python.exe -u app.py
```

**HTTP 接口**（默认 `127.0.0.1:8080`，改 `config.py`）：

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/health` | 存活探针 + 店铺清单 |
| GET | `/status?store=<id>` | 单店快照；无 `store` 参返回全部店字典（worker 存活/重启计数/巡检时间 + 进度：call_count、card_count、cursor、shard_index、status、last_error） |
| POST | `/start?store=<id>&resume=1` | 启动某店 worker（`resume=0` 全新）；`store` 缺省=DEFAULT_STORE，`store=all` 遍历全部；已在运行返回 409 |
| POST | `/resume?store=<id>` | 等价 `/start?resume=1` |
| POST | `/stop?store=<id>` | 优雅停止某店：当前调用周期结束后退出并存断点，**该店巡检不再自动重启** |

```powershell
Invoke-RestMethod http://127.0.0.1:8080/status | ConvertTo-Json -Depth 6              # 全部店
Invoke-RestMethod http://127.0.0.1:8080/status?store=store2 | ConvertTo-Json -Depth 6  # 单店
Invoke-RestMethod -Method POST http://127.0.0.1:8080/stop?store=store2
```

**巡检语义**（`config.CRON_EXPR`，默认 `*/2 * * * *`）：worker 未运行 且 非用户主动停 且 未拉完（`finished!=true` 且错误非 `no_resume_state`/`locked`/`store_mismatch`/`store_verify_failed`/`store_not_available` 终态）→ 自动 `--resume` 重启，`/status.restart_count` 递增。

**线程与锁**：worker 线程内独占 `create_driver→用→quit`（Selenium 线程亲和，勿跨线程）；进程内 `threading.Lock` 保单 worker；跨进程由**每店独立**的 `<data_dir>/fetch_all.lock`（PID 级）防撞车——宿主跑着时再手动 `python fetch_all.py --store <id> --resume` 会被 `[LOCK]` 挡下；启动前 `pre_launch_cleanup(profile_dir)` **只清本店 profile** 的残留 chrome（按路径边界匹配 + 关联父 chromedriver，不再 blanket-kill 全部 chromedriver，避免误伤并行兄弟店）。CLI：`python fetch_all.py [--resume] [--store <id>]`。

**部署**（登录自启，取代旧 bat 重试循环；用 `pythonw.exe` 无窗口常驻，观测走 `/status`）：

```powershell
schtasks /Create /TN "WB_FetchHost" /TR "C:\Users\yuanbo\PycharmProjects\WelcomeScreen\.venv\Scripts\pythonw.exe C:\Users\yuanbo\PycharmProjects\WelcomeScreen\app.py" /SC ONLOGON /F
```

任务属性必须 **“只在用户登录时运行”**（Chrome GUI 需交互式会话，会话 0 起不来）。宿主被强杀会留孤儿 chrome，下次启动 `pre_launch_cleanup()` 会按店自动清理。

### 6. 多店铺并行（多账号）

同一台机器抓多个 WB 店铺：每店一个独立 Chrome profile（隔离 cookie，同页不同账号不撞登录态）、一个 `FetchSupervisor` worker 线程、一套独立断点/锁/运行目录/OSS prefix 段，三店可并发。

**配置**（`config.py` 的 `STORES`，每店一条；相对路径按项目根解析）：

| 字段 | 含义 |
|---|---|
| `id` | 店铺唯一标识；用作 `?store=` 参数、锁文件名、日志前缀 |
| `profile_dir` | 该店独立 Chrome user-data-dir；cookie 隔离的关键 |
| `data_dir` | 该店运行目录（`tableListv6_*` 分片 + `fetch_all.lock`）；现有店用 `.` 保断点零迁移 |
| `oss_segment` | OSS prefix 追加段：最终 `wildberries/wbRatingData/<oss_segment>/json/<上传当天BJT日期>/文件名`（日期目录按每次上传实时算，日期后直接是文件名，无时分秒层） |
| `auto_start` | 可选，缺省=全局 `AUTO_START`；新店登录前置 `False`，避免未登录空转重启 |

**新店首次登录**（DPAPI cookie 无法自动登录，必须人工一次）：

```powershell
.venv\Scripts\python.exe login_store.py store2   # 弹浏览器→手动登录该店→回车关闭，cookie 落到 profiles/store2
```

登录后把该店 `auto_start` 置 `True`（随宿主自启），或 `POST /start?store=store2` 手动拉起。

**要点**：
- profile 目录名不要互为前缀（用 `profiles/storeN` 而非 `chrome_profile_store2`）；`pre_launch_cleanup` 另有边界正则双保险，但清爽命名最稳。
- 三店并发 = 3 个 chrome 常驻，内存/CPU 约 3 倍；各店独立账号，429 互不影响。若实为同账号多店，限流叠加，可调大 `CALL_INTERVAL` 或改串行。
- 单独控制：`/stop?store=store2`；一键全停/全启：`/stop?store=all`、`/start?store=all`。

### 7. 抓取前自动选店（按 config.json 切到目标店，防爬错店）

多店可能共用同一登录子账号，且不同店在下拉框里**显示名可能完全相同**，靠肉眼/名字无法区分。`fetch_all.py` 在 `driver.get(PAGE_URL)` 之后、**捕获 tableListv6 请求之前**先自动把下拉框切到 `config.json` 指定的店（切换会改供应商 cookie/token 上下文，必须在捕获前完成，否则重放的是旧店请求）：

- `config.json`（项目根）登记每店期望的 WB 数字卖家 ID：`{"store1": 250132124, "store2": 250149024}`；`config.get_seller_id(id)` 读取。
- `ensure_selected_store(driver, expected_id)`：JS `.click()` 点开 chip（`[data-testid="desktop-profile-select-button-chips-component"]`；普通 click 展不开），轮询等 `input[type=radio][name=supplier]`，遍历每 `<li>` 取 `[data-name="Text"]` 文本、从 "ID <数字> • 税号 <数字>" 解析数字 ID。返回 `already`（当前 checked 即目标，不点击）/`switched`（点击目标行 radio 切换）/`not_found`（下拉框无此店）/`click_failed`/`open_failed`。
- `switched` 后：`time.sleep(3)` 等切换落地 → `driver.get_log("performance")` 丢弃旧店请求日志 → `driver.get(PAGE_URL)` 干净重载 → 再 `wait_first_request` 捕获目标店请求 → `verify_selected_store` 只读复核 checked==目标。
- 结果码：`not_found`→`store_not_available`，复核不符→`store_mismatch`，展不开/读不到→`store_verify_failed`，均 `driver.quit()` 退出且**巡检不自动重启**（需人工确认店铺）。未配置该店 ID→跳过选店并提示补上（向后兼容）。
- **只用稳定选择器**（`data-testid` / `type=radio name=supplier` / `data-name="Text"` / "ID <数字>" 正则），刻意避开 WB 构建期哈希 class（`text_Text__CfKFk` 等，改版即失效）；**不读 cookie/JWT**（子账号共享，纯 DOM 方案）。store1 已停在目标店时走 `already`，行为等同旧版仅校验。

## 故障速查

| 现象 | 处理 |
|---|---|
| `DevToolsActivePort file doesn't exist` | chrome_profile 被锁：结束残留 chrome/chromedriver 进程后重跑 |
| HTTP 401 | 登录态失效：手动跑一次 `script.py` 在弹出的浏览器里重新登录 WB |
| HTTP 429 | 脚本自动暂停 30s 重试，无需干预；频发则调大 `CALL_INTERVAL` |
| HTTP 500（WB 后端 internalError/存储节点失联） | 服务端故障，与本地无关：脚本按 30s→300s 指数退避自动重试（最长约 4 小时）；若已退出，服务恢复后 `--resume` 续传即可 |
| ReadTimeout / 浏览器失联崩溃 | `--resume` 续传即可，数据不丢（分片实时落盘） |
| 捕获到 OPTIONS 请求 | 已修复（自动跳过预检）；若复发检查 `wait_first_request` 的过滤逻辑 |
| 定时任务未执行 | 电脑未开机/未登录，或锁文件残留 |
| Flask：`/status` 显示 `user_stopped=true` 不自动重启 | `/stop` 后巡检尊重用户意图；用 `/resume` 或 `/start` 重新启动 |
| Flask：worker 反复重启（`restart_count` 持续增长） | 多为登录态失效致 capture_timeout；看 `/status` 的 `last_error`，手动跑 `script.py` 重登后 `/resume` |
| Flask：启动报端口占用 | 8080 被其他程序占用；改 `config.PORT` 后重启 `app.py` |
| 多店：新店 worker 反复 capture_timeout 重启 | 该店 profile 未登录：跑 `login_store.py <id>` 登录；未登录前把该店 `auto_start` 置 `False` |
| 多店：怕 cleanup 误杀兄弟店 chrome | 已按 profile 路径边界匹配 + 只杀关联父 chromedriver；profile 命名勿互为前缀（用 `profiles/storeN`） |
| 多店：`last_error=store_mismatch` | 自动切换后复核仍与 `config.json` 期望 ID 不符：人工在该店 profile 里手动切到目标店（或核对登录账号）后 `/resume`；终态不自动重启 |
| 多店：`last_error=store_not_available` | 下拉框里没有 `config.json` 期望的店 ID：该子账号可能无此店权限，核对账号/店铺 ID；终态不自动重启 |
| 多店：`last_error=store_verify_failed` | 下拉框没展开/没读到/点击切换失败（改版或渲染慢）：看日志 `[SELECT]`/`[VERIFY]` 行，必要时更新选择器；终态不自动重启 |
| worker 反复重启，`last_error` 含 `UnicodeEncodeError: 'gbk'` | 日志 print 里有非 GBK 字符（emoji ✓/✅/❌）；已在 `fetch_all.py` 顶部 `reconfigure(utf-8, errors=replace, line_buffering=True)` 兜底，print 内禁用 emoji（用 `[OK]`/`[X]`/`*`），中文 GBK 可编码不受影响 |
| `host_new.log` 0 字节但 `/status` 正常在爬 | `Start-Process -WindowStyle Hidden -RedirectStandardOutput` 不落盘（PS 坑）；改 `-NoNewWindow`，或 `python -u app.py > host_new.log 2>&1` |

## 禁止事项

- 不要把 `chrome_profile` 或浏览器环节搬进 Docker/Linux 容器（DPAPI cookie 解不开）。
- 不要把 `--user-data-dir` 指回日常 Chrome 的 `User Data` 目录（Chrome 136+ 调试限制）。
- 不要为提速把 `CALL_INTERVAL` 降到 1s 以下（429 限流）。
