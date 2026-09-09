# -*- coding: utf-8 -*-
"""统计捕获结果中的 nmId 数量（总数 / 去重数）。"""
import json
from collections import Counter

FILE = r"c:\Users\yuanbo\PycharmProjects\WelcomeScreen\tableListv6_response_20260901_172522.json"

ids = []


def walk(o):
    if isinstance(o, dict):
        for k, v in o.items():
            if k == "nmID":
                ids.append(v)
            else:
                walk(v)
    elif isinstance(o, list):
        for i in o:
            walk(i)


with open(FILE, encoding="utf-8") as f:
    data = json.load(f)

# 从每个响应的商品卡片列表（cards）中直接取 nmID
cards_ids = []
for resp in data:
    body = resp["body"]
    if not isinstance(body, dict):
        continue
    payload = body.get("data") or body
    cards = None
    if isinstance(payload, dict):
        cards = payload.get("cards") or payload.get("list") or payload.get("items")
    if isinstance(cards, list):
        cards_ids.extend(c.get("nmID") for c in cards if isinstance(c, dict) and c.get("nmID") is not None)

walk(data)
dups = [(i, c) for i, c in Counter(ids).items() if c > 1]
print(f"捕获响应数: {len(data)}")
print(f"卡片列表中的 nmID 总数: {len(cards_ids)}，去重后: {len(set(cards_ids))}")
print(f"全文 nmID 出现次数: {len(ids)}，去重后: {len(set(ids))}")
print(f"重复的 nmID 个数: {len(dups)}")
if dups:
    print("重复示例:", dups[:10])
