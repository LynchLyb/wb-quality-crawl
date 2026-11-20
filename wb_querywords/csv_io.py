# -*- coding: utf-8 -*-
"""输入读取（含按搜索量降序）与分片暂存 / 合并。

输入（OSS 下载的本地文件）：
    - 从 input_path 读，取「搜索词」+「搜索量」两列。
    - 上游通常已按搜索量降序；这里再稳定降序排一次兜底，相同搜索量保持原顺序。
    - 有表头按候选名匹配；无表头按位置回退（第0列词、第1列量）。
    - 容错：搜索量非数字（千分位/空格）尽量解析；解析失败按 0 排到末尾。

输出（本地临时分片 → 合并）：
    - 每 SHARD_SIZE 次调用（= 处理 SHARD_SIZE 个搜索词）在 output/<recomType>/ 下刷一片，
      每次调用即时 append（崩溃不丢）。
    - 切换到更新 input 时，把某 input 日期下该类型的全部临时分片合并成 output/<recomType>/<date>.csv，
      再上传 OSS。
    - 列：searchText, nmID, title, mainPhoto, recomType。空结果写占位行。
"""
import csv
import glob
import io
import os

from . import config


# ------------------------------------------------------------ 输入读取（含排序）
def _to_float(v):
    """搜索量解析成 float：去千分位逗号与空白；失败返回 0.0（排到末尾）。"""
    if v is None:
        return 0.0
    s = str(v).strip().replace(",", "").replace("\u00a0", "").replace(" ", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _detect_columns(header_row):
    """由表头行推断 (text_idx, vol_idx)；找不到 vol 候选时 vol_idx=None（退化为不排序）。"""
    norm = [h.strip().lower() for h in header_row]

    def pick(cands):
        for c in cands:
            for i, h in enumerate(norm):
                if h and (c == h or c in h):
                    return i
        return None

    text_idx = pick(config.TEXT_COL_CANDIDATES)
    vol_idx = pick(config.VOL_COL_CANDIDATES)
    if text_idx is None:
        text_idx = config.TEXT_COL_INDEX_FALLBACK
    if vol_idx is not None and vol_idx == text_idx:
        vol_idx = None      # 词列与量列撞同一列 → 视为无有效量列，不排序
    return text_idx, vol_idx


def _looks_like_header(row):
    """首行第 2 列非数值且非空 → 判为表头（正常数据行第 2 列是数字搜索量）。"""
    if len(row) < 2:
        return False
    v = row[1].strip()
    if not v:
        return False
    return _to_float(v) == 0.0 and not v.replace(".", "").replace("-", "").isdigit()


def read_pairs(input_path):
    """读输入文件，返回按搜索量降序的 [(search_text, volume), ...]。文件缺失返回 []。"""
    if not os.path.exists(input_path):
        return []
    rows = []
    text_idx, vol_idx = 0, None
    header_checked = False
    with io.open(input_path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.reader(f):
            if not row or all(not c.strip() for c in row):
                continue
            if not header_checked:
                header_checked = True
                if _looks_like_header(row):
                    text_idx, vol_idx = _detect_columns(row)
                    continue
                text_idx, vol_idx = config.TEXT_COL_INDEX_FALLBACK, config.VOL_COL_INDEX_FALLBACK
            text = row[text_idx].strip() if text_idx < len(row) else ""
            if not text:
                continue
            vol = _to_float(row[vol_idx]) if (vol_idx is not None and vol_idx < len(row)) else 0.0
            rows.append((text, vol))
    if vol_idx is not None:
        rows.sort(key=lambda x: x[1], reverse=True)   # 稳定：相同量保持原顺序
    return [(t, v) for t, v in rows]


# ------------------------------------------------------------ 本地临时分片写入
class ShardWriter:
    """某 recomType 某店的本地临时分片写入器：按分片序号即时 append（每次调用一刷，崩溃不丢）。

    分片落在 output/<recomType>/<store>/ 下（多店接力各写各店，互不混写）；日期用 input 日期（一对一）。
    """

    def __init__(self, recom_type, store, date_str):
        self.recom = recom_type
        self.store = store
        self.date = date_str
        self._open_idx = None
        self._fh = None

    def ensure_open(self, shard_index):
        if self._open_idx == shard_index and self._fh is not None:
            return
        self.close()
        path = config.shard_path(self.recom, self.store, shard_index, self.date)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        new_file = not os.path.exists(path)
        self._fh = io.open(path, "a", encoding="utf-8-sig", newline="")
        if new_file:
            self._fh.write(",".join(config.CSV_HEADER) + "\r\n")
            self._fh.flush()
        self._open_idx = shard_index

    def append(self, shard_index, search_text, recs):
        """写一条调用结果（recs 为空也写占位行）。"""
        self.ensure_open(shard_index)
        if recs:
            for r in recs:
                nm = r.get("nmID", "")
                title = (r.get("title") or "").replace("\r", " ").replace("\n", " ")
                photo = r.get("mainPhoto") or ""
                self._row(search_text, nm, title, photo)
        else:
            self._row(search_text, "", "", "")

    def _row(self, search_text, nm, title, photo):
        buf = io.StringIO()
        csv.writer(buf, lineterminator="\r\n").writerow(
            [search_text, nm, title, photo, self.recom])
        self._fh.write(buf.getvalue())
        self._fh.flush()

    def close(self):
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
        self._fh = None
        self._open_idx = None

    def path(self, shard_index):
        return config.shard_path(self.recom, self.store, shard_index, self.date)


# ------------------------------------------------------------ 合并分片为最终文件
def shard_glob(recom_type, date_str):
    """某类型某日期的全部临时分片路径（glob 模式）：跨该类型下**所有店**子目录（接力无重叠）。"""
    return os.path.join(config.type_output_dir(recom_type), "*", f"{date_str}_*_shard_*.csv")


def merge_shards(recom_type, date_str):
    """把该类型该日期的全部分片（跨多店）合并成 output/<recomType>/<date>.csv（单表头 + 全数据行）。

    接力共享游标、两店分片无重叠，按店目录名 + 分片序号排序后拼接即完整。
    返回合并后本地路径；无分片返回 None。
    """
    shards = sorted(glob.glob(shard_glob(recom_type, date_str)))
    if not shards:
        return None
    merged = config.merged_path(recom_type, date_str)
    os.makedirs(os.path.dirname(merged), exist_ok=True)
    with io.open(merged, "w", encoding="utf-8-sig", newline="") as out:
        out.write(",".join(config.CSV_HEADER) + "\r\n")
        for sp in shards:
            with io.open(sp, "r", encoding="utf-8-sig", newline="") as inf:
                reader = csv.reader(inf)
                header_written = False
                for row in reader:
                    if not row:
                        continue
                    # 跳过每个分片的表头行（首行等于 CSV_HEADER）
                    if not header_written:
                        header_written = True
                        if [c.strip() for c in row] == config.CSV_HEADER:
                            continue
                    buf = io.StringIO()
                    csv.writer(buf, lineterminator="\r\n").writerow(row)
                    out.write(buf.getvalue())
    return merged
