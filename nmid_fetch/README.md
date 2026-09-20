# nmid_fetch — 按 nmID 集合抓取 WB 商品数据

## 功能

常驻 Flask 服务，循环扫描 OSS `content-opt-pool/` 目录：

1. 发现所有 `taskId/店铺ID.csv` 文件
2. 下载 CSV，解析 `nm_id` 列
3. 对每个 nmID 调 `tableListv6` 接口（`filter.search = nmID`）
4. 每 2000 次查询写一个分片 JSON
5. 异步转 CSV 并上传 OSS
6. 所有文件处理完立即开始下一轮

## 文件说明

| 文件 | 作用 |
|---|---|
| `__init__.py` | 包标识 |
| `fetch_by_nmid.py` | 主脚本：常驻循环 + 按 nmID 查询 + 分片上传 |
| `oss_input.py` | OSS 输入文件扫描/下载 |
| `app_nmid.py` | Flask 宿主入口（可选，或直接跑 fetch_by_nmid.py） |

## 运行

```powershell
# 启动常驻服务
.venv\Scripts\python.exe -u nmid_fetch/fetch_by_nmid.py

# 或指定店铺
.venv\Scripts\python.exe -u nmid_fetch/fetch_by_nmid.py --store store1
```

## 配置

- 店铺映射：复用根目录 `config.json`（`{"store1": 250132124, ...}`）
- OSS 凭据：复用根目录 `oss_config.json`
- 输入路径：`content-opt-pool/{taskId}/{storeId}.csv`
- 输出路径：`wildberries/wbRatingData/{oss_segment}/json/{日期}/{taskId}_{storeId}_shard_{NNN}_{run_ts}.json`

## 风险

- **效率**：20 万 nmID × 1.2 秒 ≈ 66 小时/店铺
- **分片大小**：2000 次查询一个分片，崩溃最多丢 2000 次结果
- **重复处理**：每轮重新下载 CSV，全量重新查询，OSS 文件名含时间戳不覆盖
