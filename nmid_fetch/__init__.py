# -*- coding: utf-8 -*-
"""按 nmID 集合抓取 WB 商品数据模块。

从 OSS content-opt-pool/ 下载输入 CSV（nmID 列表）：
只跑 {卖家ID}_treatment_001.csv（每轮重复）与 {卖家ID}_control.csv（每卖家只跑最新一份、台账去重），
逐个调 tableListv6 的 filter.search 查询，
分片转 CSV 后上传 OSS。
"""
