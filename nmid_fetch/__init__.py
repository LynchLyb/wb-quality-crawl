# -*- coding: utf-8 -*-
"""按 nmID 集合抓取 WB 商品数据模块。

从 OSS content-opt-pool/ 下载店铺 CSV（nmID 列表），
逐个调 tableListv6 的 filter.search 查询，
分片转 CSV 后上传 OSS。
"""
