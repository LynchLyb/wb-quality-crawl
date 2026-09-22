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

## 抓取速率与日产量（间隔 1.6s = 实测最优）

单店单账号串行抓取，请求间隔以 **1.6 秒**为基准——经生产实测调优出的**吞吐最优点**；遇服务端限流时会自动、临时地上调间隔以保稳定，限流缓解后自动回到 1.6 秒。

| 请求间隔 | 净速率 | 日处理量 | 日有效数据（命中率 ~95%） | 备注 |
|---|---|---|---|---|
| **1.6s（当前采用·实测）** | **~22 条/分** | **~30,000 条** | **~28,500 条** | 吞吐最高 |
| 2.0s（实测） | ~21.5 条/分 | ~29,000 条 | ~27,500 条 | 常态略慢 |
| 2.5s（推算） | ~21 条/分 | ~28,000 条 | ~26,500 条 | 更慢 |
| 3.0s（推算） | ~19 条/分 | ~26,000 条 | ~24,500 条 | 最慢 |

> 1.6s / 2.0s 为生产实测，2.5s / 3.0s 按间隔模型推算；日产量按 24h 连续运行、命中率 95% 计，WB 服务端延迟随时段波动，实测日产量落在 ~28,000–31,000 区间。

**为什么 1.6s 是最优**

- **再低（<1.6s）**：更贴近 WB 限流红线，限流（HTTP 429）频次上升，净速度不升反降。
- **再高（>1.6s）**：**间隔数字设得越大，越没有时间优势**——常态净速度越低、日产量越少（见上表 2.0→2.5→3.0s 逐级递减）。所以常态下 1.6s 就是净吞吐最高点；系统仅在服务端限流时临时、小幅上调间隔换取稳定（宁可略慢也不硬撞 429），限流一过立即回到 1.6s——那是稳定性保护而非提速。
- **1.6s** 正好卡在两者之间，是当前账号 / 接口条件下的**净吞吐最高点**。

**可靠性**

- 偶发 429 属 WB 服务端正常限流：脚本会**自动微调请求间隔**换取稳定并**自动重试**、**数据零丢失**，限流缓解后自动回到 1.6s 最优间隔，对最终结果无影响。
- 分片**实时落盘**，断电 / 重启后**自动断点续传**，已抓进度不丢。

**扩展性**

- 单店 ~30,000 条/天为串行上限；**多店铺可并行**（每店独立账号 + 独立浏览器 profile），日产量近似线性叠加。

## 数据接口与返回格式

按 nmID 抓单个商品时，WB 卖家后台有两个可用接口，返回结构差别很大。**项目当前只用 `tableListv6`**（`fetch_by_nmid.py` 重放时读 `data.cards`）；`GetCardInfo` 是单卡详情接口，**尚未接入**，列在这里备查/评估。

### 接口一览

| | tableListv6（现用） | GetCardInfo（未接入） |
|---|---|---|
| Method | `POST` | `GET` |
| URL | `https://seller-content.wildberries.ru/ns/viewer/content-card/viewer/tableListv6` | `https://seller-content.wildberries.ru/ns/viewer/content-card/viewer/GetCardInfo?nmID=<nmID>` |
| 返回结构 | `data.cards[]` 数组 + `data.cursor` 分页游标 | `data` 内联单卡字段，无 `cards`/`cursor` |
| 定位 | 列表/可翻页，一次可多卡；按 nmID 抓时把 `filter.search` 换成 nmID 重放，返回单卡 | 单实体详情，一次一卡 |
| 鉴权 | 请求头 `AuthorizeV3` / `Wb-Seller-Lk`（运行时实时捕获，勿硬编码） | 同属卖家后台接口，鉴权头一致 |

### 相同点

- **外层信封一致**：`{ data, error:false, errorText:"", additionalErrors:{} }`。
- **同一商品同一批核心值**：`nmID`/`id`、`vendorCode`、`title`、品牌、`subject`、6 张图、2 个尺码（`techSize`/`wbSize`/`skus`/`currency`/价格 414·492）、`discount=10`、评分 `9.5` + `isCardRated:true` + 同样 3 条 `errors`、`feedbacks:null`。

### 字段差异

| 含义 | tableListv6 | GetCardInfo |
|---|---|---|
| 商品 ID | `nmID` | `id`（改名） |
| 品牌 | `brand` + `brandRaw`（两个） | `brandName`（一个） |
| 图片 | `mediaFiles`：`value`(c516x688)+`thumbnail`+`mimeType` | `photos`：`value`(**big 大图**)+`mimeType`，无 thumbnail |
| 尺码 ID / 价 | `sizes[].sizeID` / `currentPrice` | `sizes[].id` / `price` |
| 评分 | `meta.ratingData.*` | 顶层 `rating.*` |
| 折扣 | 顶层 `discount` + `discountSource` | `priceInfo.discount`（无 source） |

- **GetCardInfo 独有**：`sizes[].discountedPrice`（折后价 372.6/442.8 = price×0.9）、`priceInfo.{minPrice,maxPrice}`、`subjectId`、`/big/` 高清图 URL。
- **tableListv6 独有（GetCardInfo 全缺）**：`imtID`、`updateAt`、`colors`、`stocks`、`tags`、`externalBan`、`feedbackRating`、`discountSource`、`hasPaidOptions`（4 项）、`documents`，以及整个 `meta` 合规块（`needKiz`/`needUIN`/`dimensionsWarning`/`tnvedWarning`/`withABTest`/`richModerationFail`/`hasCutoutPhoto` 等）。

### 对 CSV 转换的影响（重要）

根目录 `wb_to_oss.py` 的 32 列 `CANONICAL_COLS` + `flatten_card` 是**照 tableListv6 定制**的，直接换 GetCardInfo 会出问题：

1. `convert_shard` 只认 `data.cards` / `data.list`；GetCardInfo 的 `data` 两者都无 → `cards=[]` → **0 行，空 CSV**。
2. 即便改转换器去读内联对象，32 列里也只有约 5 列（`vendorCode`/`title`/`subject`/`feedbacks`/`sizes_count`）能直接对上；`rating` 内容虽同但路径变了（`data.rating` vs `meta.ratingData`）→ 现码取不到 → 空；`nmID`/`brand`/`mediaFiles_count`/`sizes_detail` 里的 `price` 全因改名落空 → **等于重写整套字段映射**。
3. GetCardInfo 能多给折后价 / 价格区间 / `subjectId` / 大图，但现有 32 列无对应位（`CANONICAL_COLS` 注释：“顺序与模板 xlsx 一致，不得增删改”）→ 要扩列 + 改 `flatten_card`。

**结论**：两者非 drop-in 可换。tableListv6 信息更全、是现 CSV 数据源；GetCardInfo 更精简、偏价格详情，若要用须单独适配转换层。

### 返回样例（nmID 1641296819）

#### 样例① tableListv6 —— `POST`，`filter.search=nmID` 命中单卡

```json
{
  "data": {
    "cards": [
      {
        "imtID": 4156184942,
        "nmID": 1641296819,
        "vendorCode": "0-1005007185307724-H9023607378040424834-V2289980390-20260915148c0",
        "updateAt": "2026-09-20T16:01:26Z",
        "brandRaw": "SLWIKERS",
        "title": "Фиолетовый чехол для iPhone X/XS",
        "mediaFiles": {
          "0": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/1.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/1.webp",
            "mimeType": "image/jpeg"
          },
          "1": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/2.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/2.webp",
            "mimeType": "image/jpeg"
          },
          "2": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/3.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/3.webp",
            "mimeType": "image/jpeg"
          },
          "3": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/4.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/4.webp",
            "mimeType": "image/jpeg"
          },
          "4": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/5.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/5.webp",
            "mimeType": "image/jpeg"
          },
          "5": {
            "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/c516x688/6.webp",
            "thumbnail": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/tm/6.webp",
            "mimeType": "image/jpeg"
          }
        },
        "brand": "SLWIKERS",
        "subject": "手机壳",
        "colors": [
          "紫罗兰色"
        ],
        "sizes": [
          {
            "sizeID": 2603505545,
            "techSize": "A",
            "skus": [
              "12000039735387017"
            ],
            "wbSize": "1",
            "currency": "RUB",
            "currentPrice": 414
          },
          {
            "sizeID": 2603505546,
            "techSize": "B",
            "skus": [
              "12000039735387017-GE"
            ],
            "wbSize": "2",
            "currency": "RUB",
            "currentPrice": 492
          }
        ],
        "tags": [],
        "stocks": 17776,
        "meta": {
          "dimensionsWarning": false,
          "hasDimensionDeviation": false,
          "tnvedWarning": false,
          "needKiz": false,
          "needUIN": false,
          "ratingData": {
            "rating": "9.5",
            "isCardRated": true,
            "errors": [
              {
                "field": "characteristics",
                "details": [
                  "MessageMissingCharcs",
                  "MessageUpperCharc",
                  "MessageNeedKiz"
                ]
              }
            ]
          },
          "withABTest": false,
          "noWeightBruttoWarning": false,
          "hasWeightBruttoDeviation": false,
          "richModerationFail": false,
          "hasCutoutPhoto": false
        },
        "externalBan": null,
        "feedbackRating": 0,
        "feedbacks": null,
        "hasPaidOptions": {
          "hasPhotoTags": false,
          "hasRichContent": false,
          "hasAutoplayVideo": false,
          "hasClientTryOn": false
        },
        "documents": {},
        "discount": 10,
        "discountSource": 1
      }
    ],
    "cursor": {
      "next": false,
      "n": 1,
      "value": null,
      "nmID": 0
    }
  },
  "error": false,
  "errorText": "",
  "additionalErrors": {}
}
```

#### 样例② GetCardInfo —— `GET ?nmID=1641296819`

```json
{
  "data": {
    "id": 1641296819,
    "brandName": "SLWIKERS",
    "photos": {
      "0": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/1.webp",
        "mimeType": "image/jpeg"
      },
      "1": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/2.webp",
        "mimeType": "image/jpeg"
      },
      "2": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/3.webp",
        "mimeType": "image/jpeg"
      },
      "3": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/4.webp",
        "mimeType": "image/jpeg"
      },
      "4": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/5.webp",
        "mimeType": "image/jpeg"
      },
      "5": {
        "value": "https://basket-49.wbbasket.ru/vol16412/part1641296/1641296819/images/big/6.webp",
        "mimeType": "image/jpeg"
      }
    },
    "sizes": [
      {
        "id": 2603505545,
        "techSize": "A",
        "wbSize": "1",
        "skus": [
          "12000039735387017"
        ],
        "price": 414,
        "currency": "RUB",
        "discountedPrice": "372.6"
      },
      {
        "id": 2603505546,
        "techSize": "B",
        "wbSize": "2",
        "skus": [
          "12000039735387017-GE"
        ],
        "price": 492,
        "currency": "RUB",
        "discountedPrice": "442.8"
      }
    ],
    "subject": "手机壳",
    "subjectId": 390,
    "title": "Фиолетовый чехол для iPhone X/XS",
    "vendorCode": "0-1005007185307724-H9023607378040424834-V2289980390-20260915148c0",
    "priceInfo": {
      "discount": 10,
      "minPrice": "372.6",
      "maxPrice": "442.8"
    },
    "feedbacks": null,
    "rating": {
      "rating": "9.5",
      "isCardRated": true,
      "errors": [
        {
          "field": "characteristics",
          "details": [
            "MessageMissingCharcs",
            "MessageUpperCharc",
            "MessageNeedKiz"
          ]
        }
      ]
    }
  },
  "error": false,
  "errorText": "",
  "additionalErrors": {}
}
```

## 风险

- **效率**：单店串行稳态 ~22 条/分（≈3 万条/天）；20 万 nmID 约需 6-7 天/店铺，多店并行可线性缩短
- **分片大小**：2000 次查询一个分片，崩溃最多丢 2000 次结果
- **重复处理**：每轮重新下载 CSV，全量重新查询，OSS 文件名含时间戳不覆盖
