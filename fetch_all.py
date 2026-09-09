# -*- coding: utf-8 -*-
"""
通过页面上下文 fetch 直接翻页拉取 tableListv6 全量数据（不滚动、不渲染，浏览器负担小）。

流程:
    1. 打开商品列表页，从 performance 日志中捕获首条 tableListv6 请求，
       解析出真实请求方法/请求头/请求体（含鉴权头与分页游标结构）
    2. 之后在浏览器 console 里用 fetch 循环调用该接口，
       从响应的 data.cursor 取下一页游标，替换请求体中的 cursor 字段继续请求
    3. 每 SHARD_SIZE 次调用写一个分片文件，全部拉完后写汇总
    4. 每个分片落盘后立即开异步线程: 转成 CSV 并上传 OSS（wb_to_oss.py），
       不阻塞拉取；JSON+CSV 都上传成功后删除本地文件（OSS 为唯一存档），
       线程失败/中断未传的，事后用 wb_to_oss.py --upload-only 补传

用法:
    python fetch_all.py                          # 全新拉取（默认店 config.DEFAULT_STORE）
    python fetch_all.py --resume                 # 从最近未完成运行的游标断点续传
    python fetch_all.py --resume --store store2  # 指定店铺并行拉取

也可作为库被 Flask 宿主（supervisor.py）以线程方式调用（每店一个 worker）:
    run_fetch(resume=True, stop_event=<threading.Event>, progress=<dict>, store=<已解析店铺dict>)
    - stop_event 置位后在一个调用周期内优雅停止并保存断点
    - progress["snap"] 持续发布原子进度快照，供 /status 读取
    - PID 级单实例锁 + 启动前清理残留自动化 chrome，保证同机不撞车
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time

import config
# 复用 script.py 的驱动配置（profile 现按店传入，多店各用独立资料目录）
from script import PAGE_URL, create_driver
from wb_to_oss import convert_and_upload_shard, oss_target

# Windows 宿主 stdout 默认常为 GBK：日志里的非 GBK 字符（emoji/箭头等）会触发
# UnicodeEncodeError 直接崩掉 worker；统一把标准输出/错误切到 UTF-8，无法编码者降级替换，杜绝此类崩溃。
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

TARGET_URI = "seller-content.wildberries.ru/ns/viewer/content-card/viewer/tableListv6"
SHARD_SIZE = 100        # 每 100 次调用写一个分片文件
CALL_INTERVAL = 1.2     # 两次调用之间的间隔（秒）；遇 429 会自动暂停重试，可承受较激进的间隔
CAPTURE_TIMEOUT = 120   # 等待首条请求出现的最长时间（秒）
RATE_LIMIT_PAUSE = 30   # 遇到 429 限流后的长暂停（秒）
SERVER_FAIL_LIMIT = 60  # 服务端 5xx 连续错误容忍上限（指数退避下约可扛 4 小时故障）

# 进程级单实例锁：每店一个锁文件 <data_dir>/fetch_all.lock，
# 防止两个进程（宿主 worker 与手动 CLI）抢同一店的 chrome profile
def _lock_file(data_dir):
    return os.path.join(data_dir, "fetch_all.lock")


def _pid_alive(pid):
    """判断给定 PID 的进程是否存活。

    Windows 上用 OpenProcess/GetExitCodeProcess，绝不用 os.kill(pid, 0)——
    后者在 Windows 上会走 TerminateProcess 分支，可能误杀目标进程。
    """
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        ERROR_ACCESS_DENIED = 5
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            # 打开失败：权限不足说明进程存在（保守判活），其余（如无效参数）判死
            return ctypes.get_last_error() == ERROR_ACCESS_DENIED
        try:
            code = wintypes.DWORD()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return code.value == STILL_ACTIVE
            return True  # 句柄有效但查询失败，保守判活
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_single_instance(lock_file):
    """获取 PID 级单实例锁。返回 True=拿到锁可运行，False=被其他存活进程占用。

    同进程重入放行（supervisor 在宿主内重启 worker 时 old_pid == 当前 pid）；
    其他进程持锁且仍存活则拒绝，防止两个进程抢同一店 profile。
    """
    me = os.getpid()
    try:
        if os.path.exists(lock_file):
            with open(lock_file, encoding="utf-8") as f:
                old_pid = int((f.read() or "0").strip() or 0)
            if old_pid == me:
                return True  # 同进程重入
            if _pid_alive(old_pid):
                print(f"[LOCK] 已有进程 PID={old_pid} 正在运行该店 fetch_all，本次拒绝启动（防撞车）")
                return False
            print(f"[LOCK] 发现失效锁（PID={old_pid} 已不在），接管")
    except (OSError, ValueError) as e:
        print(f"[LOCK] 读取锁文件异常，尝试接管: {e}")
    try:
        with open(lock_file, "w", encoding="utf-8") as f:
            f.write(str(me))
    except OSError as e:
        print(f"[LOCK] 写入锁文件失败: {e}")
    return True


def release_single_instance(lock_file):
    """释放 PID 锁：仅当锁文件属于本进程时删除，避免误删他人刚接管的锁。"""
    me = os.getpid()
    try:
        if os.path.exists(lock_file):
            with open(lock_file, encoding="utf-8") as f:
                old_pid = int((f.read() or "0").strip() or 0)
            if old_pid == me:
                os.remove(lock_file)
    except (OSError, ValueError):
        pass


def _profile_match(profile_dir):
    """返回匹配"命令行里出现本店 profile 路径"的正则。

    边界断言 (?!...) 防止 profile 名互为前缀时误伤：如 chrome_profile 不应命中
    chrome_profile_store2（本项目新店用 profiles/storeN，本就无前缀关系，此为双保险）。
    """
    return re.compile(re.escape(profile_dir) + r"(?![\w\-.\\/])", re.IGNORECASE)


def pre_launch_cleanup(profile_dir):
    """启动前清理本店残留的自动化 chrome/chromedriver（绝不误伤兄弟店与日常浏览器）。

    上次被强杀会留下孤儿 chrome 占着该 profile 的 SingletonLock，导致新实例打不开
    （DevToolsActivePort 缺失 / Chrome instance exited）。做法：
      1. 杀命令行含"本店 profile 路径"（边界匹配）的 chrome.exe；
      2. 顺带杀这些 chrome 的父进程里属于 chromedriver.exe 的（按 ParentProcessId 精确关联）。
    不再 blanket-kill 全部 chromedriver——那会连带杀掉并行运行的兄弟店驱动。
    """
    if os.name != "nt":
        return
    pat = _profile_match(profile_dir)
    killed = 0
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-CimInstance Win32_Process -Filter \"name='chrome.exe' OR name='chromedriver.exe'\" | "
             "Select-Object ProcessId,ParentProcessId,Name,CommandLine | ConvertTo-Json -Compress"],
            capture_output=True, text=True, timeout=30)
        data = (out.stdout or "").strip()
        rows = []
        if data:
            parsed = json.loads(data)
            rows = parsed if isinstance(parsed, list) else [parsed]
        chrome_pids, parent_pids = set(), set()
        for row in rows:
            if (row.get("Name") or "").lower() != "chrome.exe":
                continue
            cmd = row.get("CommandLine") or ""
            if cmd and pat.search(cmd) and row.get("ProcessId"):
                chrome_pids.add(int(row["ProcessId"]))
                if row.get("ParentProcessId"):
                    parent_pids.add(int(row["ParentProcessId"]))
        driver_pids = {
            int(row["ProcessId"]) for row in rows
            if (row.get("Name") or "").lower() == "chromedriver.exe"
            and row.get("ProcessId") and int(row["ProcessId"]) in parent_pids
        }
        for pid in chrome_pids | driver_pids:
            subprocess.run(["taskkill", "/F", "/PID", str(pid)], capture_output=True, timeout=15)
            killed += 1
    except (subprocess.SubprocessError, OSError, json.JSONDecodeError, ValueError) as e:
        print(f"[CLEANUP] 扫描进程失败（忽略）: {e}")
    if killed:
        print(f"[CLEANUP] 已清理 {killed} 个残留自动化进程（profile={os.path.basename(profile_dir)}）")


def wait_first_request(driver, timeout=CAPTURE_TIMEOUT):
    """等待并捕获首条 tableListv6 请求，返回 {url, method, postData}。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for entry in driver.get_log("performance"):
            try:
                msg = json.loads(entry["message"])["message"]
            except (KeyError, json.JSONDecodeError):
                continue
            if msg.get("method") != "Network.requestWillBeSent":
                continue
            req = msg.get("params", {}).get("request", {})
            if TARGET_URI not in req.get("url", ""):
                continue
            # 跳过 CORS 预检请求，只捕获真实的数据请求（GET/POST）
            if req.get("method", "GET").upper() == "OPTIONS":
                continue
            return {
                "url": req["url"],
                "method": req.get("method", "GET"),
                "postData": req.get("postData"),
                "headers": req.get("headers", {}),
            }
        time.sleep(0.5)
    return None


# fetch 时不重放的请求头（由浏览器/credentials 自动处理或属于连接层）
SKIP_HEADERS = {
    "host", "content-length", "connection", "accept-encoding", "user-agent",
    "cookie", "referer", "origin",
}


def build_fetch_headers(raw_headers):
    """从捕获的请求头中筛选出可重放的头（鉴权头等）。"""
    return {
        k: v for k, v in raw_headers.items()
        if k.lower() not in SKIP_HEADERS and not k.lower().startswith("sec-")
    }


def call_api(driver, url, method, headers, post_data=None):
    """在页面上下文执行 fetch（带捕获的鉴权头），返回 (http_status, body_text)。"""
    raw = driver.execute_async_script("""
        const [url, method, headers, postData] = arguments;
        const cb = arguments[arguments.length - 1];
        const opts = {method: method, credentials: 'include', headers: headers};
        if (postData) { opts.body = postData; }
        fetch(url, opts)
            .then(r => r.text().then(t => cb(JSON.stringify([r.status, t]))))
            .catch(e => cb(JSON.stringify([0, 'FETCH_ERROR: ' + e])));
    """, url, method, headers, post_data)
    status, text = json.loads(raw)
    return status, text


def flush_shard(buffer, shard_index, run_ts, out_dir):
    """将缓冲的响应写入分片文件，返回 (下一个分片编号, 分片路径)。"""
    path = os.path.join(out_dir, f"tableListv6_shard_{shard_index:03d}_{run_ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(buffer, f, ensure_ascii=False)
    print(f"[SHARD] 第 {shard_index} 个分片已写入: {path}（{len(buffer)} 次调用）")
    return shard_index + 1, path


def _shard_worker(run_dir, path, bucket, prefix):
    """异步线程体：转换单个分片并上传，任何异常只记日志，不影响拉取主流程。

    JSON+CSV 都上传成功后删除本地这两个文件（OSS 为唯一存档，控制磁盘占用）；
    任一失败或未配置 OSS 时保留本地文件，待补传/重跑。
    """
    try:
        err = convert_and_upload_shard(run_dir, path, bucket, prefix)
        if err:
            print(f"[OSS-ASYNC] {err}")
            return
        if bucket is None or prefix is None:
            return  # 未配置 OSS：只转换不上传，本地文件保留
        csv_path = os.path.join(
            run_dir, "csv", os.path.splitext(os.path.basename(path))[0] + ".csv")
        removed = []
        for f in (path, csv_path):
            try:
                os.remove(f)
                removed.append(os.path.basename(f))
            except OSError as e:
                print(f"[OSS-ASYNC] 删除失败 {os.path.basename(f)}: {e}")
        if removed:
            print(f"[OSS-ASYNC] 已上传并清理本地: {', '.join(removed)}")
    except Exception as e:
        print(f"[OSS-ASYNC] 线程异常: {type(e).__name__}: {e}")


def save_state(out_dir, run_ts, shard_index, call_count, card_count, last_cursor, finished=False):
    """保存断点状态，供 --resume 续传使用。"""
    state = {
        "run_ts": run_ts,
        "shard_index": shard_index,      # 下一个分片编号
        "call_count": call_count,
        "card_count": card_count,
        "last_cursor": last_cursor,      # 下一次请求应使用的游标
        "finished": finished,
    }
    with open(os.path.join(out_dir, "state.json"), "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def find_resume_state(data_dir):
    """在指定店的 data_dir 内查找最近一次未完成的运行，返回 (out_dir, state)；找不到返回 (None, None)。

    优先读 state.json；旧运行目录没有 state.json 时，
    从最后一个分片的最后一条响应里恢复 data.cursor。
    """
    base = data_dir
    if not os.path.isdir(base):
        return None, None
    # 只认目录，跳过同名的 .zip 备份等文件
    dirs = sorted(
        (d for d in os.listdir(base)
         if d.startswith("tableListv6_2") and os.path.isdir(os.path.join(base, d))),
        reverse=True,
    )
    for d in dirs:
        p = os.path.join(base, d)
        state_path = os.path.join(p, "state.json")
        if os.path.exists(state_path):
            with open(state_path, encoding="utf-8") as f:
                st = json.load(f)
            if st.get("finished") or not st.get("last_cursor"):
                continue
            return p, st
        # 无 state.json：从分片+summary 推导（兼容崩溃退出的旧运行）；
        # 已上传的分片可能被本地清理，从最后一个仍可读的分片恢复
        shards = sorted(f for f in os.listdir(p) if f.startswith("tableListv6_shard_"))
        summary_path = os.path.join(p, "summary.json")
        if not shards or not os.path.exists(summary_path):
            continue
        with open(summary_path, encoding="utf-8") as f:
            summary = json.load(f)
        cursor, shard_index = None, None
        for shard_name in reversed(shards):
            try:
                with open(os.path.join(p, shard_name), encoding="utf-8") as f:
                    last_bodies = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue
            cursor = (last_bodies[-1].get("data") or {}).get("cursor") if last_bodies else None
            if cursor:
                shard_index = int(shard_name.split("_")[2]) + 1
                break
        if not cursor:
            continue
        st = {
            "run_ts": summary.get("run_ts") or d.replace("tableListv6_", ""),
            "shard_index": shard_index,
            "call_count": summary.get("total_calls", 0),
            "card_count": summary.get("total_cards", 0),
            "last_cursor": cursor,
            "finished": False,
        }
        return p, st
    return None, None


# ------------------------------------------------------------ 抓取前店铺校验
# 用稳定选择器（data-testid / 语义属性 type=radio name=supplier / data-name=Text），
# 刻意避开 WB 前端构建期哈希 class（如 text_Text__CfKFk、suppliers-list_...__9lMrO），
# 那些哈希随 WB 改版会变，硬编码进生产会失效。
_PROFILE_CHIP_SEL = '[data-testid="desktop-profile-select-button-chips-component"]'
_SUPPLIER_RADIO_CSS = 'input[type="radio"][name="supplier"]'


def _open_supplier_dropdown(driver, timeout=25):
    """点开店铺切换下拉并等待列表渲染。返回 True/False。

    实测普通 .click() 展不开、需 JS .click()；这里全程用 execute_script 轮询点击，
    直到出现 input[type=radio][name=supplier]（每个可选店铺一个 radio）。
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cnt = driver.execute_script(
                "return document.querySelectorAll(arguments[0]).length;", _SUPPLIER_RADIO_CSS)
        except Exception:
            cnt = 0
        if cnt and cnt > 0:
            return True
        try:
            driver.execute_script(
                "var b=document.querySelector(arguments[0]);"
                "if(b){b.scrollIntoView({block:'center'});b.click();}", _PROFILE_CHIP_SEL)
        except Exception:
            pass
        time.sleep(1.0)
    return False


def _parse_supplier_id(texts):
    """从店铺行的多段文本解析数字卖家 ID：形如 'ID 250132124 • 税号 7703380158'。

    取 '•' 之前那段的数字（卖家 ID），避免误取税号；多档兜底提高鲁棒性。
    """
    for t in texts:
        if not t:
            continue
        head = t.split("•")[0] if "•" in t else t
        m = re.search(r"ID\s*(\d{5,})", head) or re.search(r"(\d{5,})", head)
        if m:
            return m.group(1)
    for t in texts:
        m = re.search(r"ID\s*(\d{5,})", t or "")
        if m:
            return m.group(1)
    return None


_SUPPLIER_ROWS_JS = r"""
var inputs=[].slice.call(document.querySelectorAll('input[type="radio"][name="supplier"]'));
return JSON.stringify(inputs.map(function(inp){
  var li = inp.closest('li') || inp.parentElement;
  var texts = li ? [].slice.call(li.querySelectorAll('[data-name="Text"]')).map(function(s){return (s.innerText||'').trim();}) : [];
  return {uuid: inp.id||'', checked: !!inp.checked, texts: texts};
}));
"""


def _read_supplier_rows(driver):
    """读取下拉框所有店铺行；返回 [{uuid, checked, texts}]，读取异常返回 None。"""
    try:
        raw = driver.execute_script(_SUPPLIER_ROWS_JS)
        return json.loads(raw) if raw else []
    except Exception as e:
        print(f"[SELECT] 读取下拉列表失败: {e}")
        return None


def _close_dropdown(driver):
    """收起下拉（best-effort，纯 JS；即便不收起也不影响后续 fetch 式采集）。"""
    try:
        driver.execute_script(
            "document.dispatchEvent(new KeyboardEvent('keydown',"
            "{key:'Escape',keyCode:27,which:27,bubbles:true}));")
    except Exception:
        pass


def _log_supplier_rows(rows, tag="SELECT"):
    """逐行打印店铺（选中标记 + 数字 ID + 名字），返回 checked 行的数字 ID。"""
    selected_id = None
    for r in rows:
        pid = _parse_supplier_id(r.get("texts", []))
        nm = (r.get("texts") or [""])[0][:24]
        print(f"[{tag}]   {'*选中' if r.get('checked') else '     '} ID={pid or '?'} | {nm}")
        if r.get("checked") and selected_id is None:
            selected_id = pid
    return selected_id


def _click_supplier_radio(driver, uuid):
    """点击指定 uuid 的店铺 radio 以切换店铺：JS 点击 input，找不到则退到其 label。返回 True/False。"""
    js = r"""
    var el = document.getElementById(arguments[0]);
    if (!el) {
      var lbls = document.querySelectorAll('label[for="' + arguments[0] + '"]');
      el = lbls.length ? lbls[lbls.length - 1] : null;
    }
    if (el) { el.scrollIntoView({block:'center'}); el.click(); return true; }
    return false;
    """
    try:
        return bool(driver.execute_script(js, uuid))
    except Exception as e:
        print(f"[SELECT] 点击店铺 radio 失败: {e}")
        return False


def ensure_selected_store(driver, expected_id, settle=3.0):
    """确保下拉框当前选中店铺 == expected_id；不是则自动点击切换（按 config.json 自动选店）。

    返回 (status, current_id, rows)：
      status ∈ {"already","switched","not_found","click_failed","open_failed"}
      current_id = 操作前 checked 行的数字 ID（日志用）；rows = 店铺行列表。
    "switched" 仅表示已点击目标店并等其落地；调用方需重载页面 + 复核确认。
    """
    if not _open_supplier_dropdown(driver):
        print("[SELECT] 下拉框未能展开（未找到店铺 radio）")
        return "open_failed", None, []
    rows = _read_supplier_rows(driver)
    if not rows:
        print("[SELECT] 下拉框已展开但没读到店铺行")
        _close_dropdown(driver)
        return "open_failed", None, rows or []
    current_id = _log_supplier_rows(rows, "SELECT")
    target = None
    for r in rows:
        if _parse_supplier_id(r.get("texts", [])) == expected_id:
            target = r
            break
    if target is None:
        _close_dropdown(driver)
        return "not_found", current_id, rows
    if target.get("checked"):
        _close_dropdown(driver)
        return "already", current_id, rows
    if not _click_supplier_radio(driver, target.get("uuid", "")):
        _close_dropdown(driver)
        return "click_failed", current_id, rows
    # 点击后 WB 切换供应商并（通常）重载页面；等其落地，调用方再显式重载 + 捕获目标店请求
    time.sleep(settle)
    _close_dropdown(driver)
    return "switched", current_id, rows


def verify_selected_store(driver):
    """只读校验：返回下拉框中当前选中(checked)店铺的数字卖家 ID（不切换）。

    返回 (selected_id_str | None, rows)：rows 为每个店铺行的 {uuid, checked, texts}，
    selected_id 为 checked 行解析出的数字 ID（展开失败/读不到则 None）。
    """
    if not _open_supplier_dropdown(driver):
        print("[VERIFY] 下拉框未能展开（未找到店铺 radio）")
        return None, []
    rows = _read_supplier_rows(driver)
    if not rows:
        _close_dropdown(driver)
        return None, rows or []
    selected_id = _log_supplier_rows(rows, "VERIFY")
    _close_dropdown(driver)
    return selected_id, rows


def _fetch_impl(resume, progress, stop_event, store):
    """实际拉取流程（由 run_fetch 在持锁后调用）。progress/stop_event 可为 None。

    store 为已解析店铺 dict（id/profile_dir/data_dir/oss_segment）。
    返回 dict: {finished, call_count, card_count, out_dir, error}
    """
    data_dir = store["data_dir"]

    def _prog(**kw):
        """原子发布进度快照：合并旧快照+新字段+updated_at，整体替换引用（读侧无锁）。"""
        if progress is None:
            return
        prev = progress.get("snap") or {}
        merged = dict(prev)
        merged.update(kw)
        merged["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        progress["snap"] = merged

    def _stopped():
        return stop_event is not None and stop_event.is_set()

    result = {"finished": False, "call_count": 0, "card_count": 0, "out_dir": None, "error": None}
    st = None
    if resume:
        out_dir, st = find_resume_state(data_dir)
        if out_dir is None:
            print("[ERROR] 未找到可续传的运行目录（均已完成或无有效游标）")
            _prog(status="idle", error="no_resume_state", last_error="无可续传运行")
            result["error"] = "no_resume_state"
            return result
        run_ts = st["run_ts"]
    else:
        run_ts = time.strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(data_dir, f"tableListv6_{run_ts}")
    os.makedirs(out_dir, exist_ok=True)
    result["out_dir"] = out_dir

    # OSS 随传随传: 每个分片落盘后异步转换+上传；配置缺失时只拉取，不影响主流程
    bucket, prefix, oss_err = oss_target(oss_segment=store["oss_segment"])
    if oss_err:
        print(f"[OSS] 流式转换上传不可用，仅本地落盘: {oss_err}")
    upload_threads = []

    def on_shard_written(shard_path):
        t = threading.Thread(target=_shard_worker,
                             args=(out_dir, shard_path, bucket, prefix), daemon=True)
        upload_threads.append(t)
        t.start()

    # 启动前清理残留自动化 chrome（孤儿进程占着 profile 锁会让新实例打不开）——仅本店 profile
    pre_launch_cleanup(store["profile_dir"])
    _prog(status="launching_browser", store_id=store["id"], out_dir=out_dir, run_ts=run_ts)
    driver = create_driver(headless=False, profile_dir=store["profile_dir"])
    driver.set_script_timeout(120)
    print(f"[{store['id']}] 打开页面: {PAGE_URL}")
    print(f"[{store['id']}] 结果目录: {out_dir}")

    driver.get(PAGE_URL)

    # ---- 抓取前自动选店：按 config.json 把下拉框切到目标店 ----
    # 切换店铺会改变供应商上下文（cookie/token），必须在捕获 tableListv6 请求之前完成，
    # 否则捕获并重放的是旧店的请求。store1/store2 显示名可能完全相同，只能靠每行 "ID <数字>" 区分。
    expected_id = config.get_seller_id(store["id"])
    switched = False
    if expected_id is None:
        print(f"[{store['id']}][SELECT] config.json 未配置该店卖家 ID，跳过自动选店（建议补上以防爬错店）")
    else:
        _prog(status="selecting_store", store_id=store["id"])
        print(f"[{store['id']}][SELECT] 目标店铺 ID={expected_id}，检查并自动切换下拉框选中项 ...")
        status, cur_id, rows = ensure_selected_store(driver, expected_id)
        if status == "switched":
            switched = True
            print(f"[{store['id']}][SELECT] 已切换 ID={cur_id or '未知'} -> ID={expected_id}，重载页面以捕获该店请求")
            time.sleep(3)                        # 等切换在客户端落地（写供应商上下文）
            try:
                driver.get_log("performance")    # 丢弃切换前(旧店)的请求日志，避免捕获到旧店请求
            except Exception:
                pass
            driver.get(PAGE_URL)                 # 干净重载：此时页面处于目标店上下文
        elif status == "already":
            print(f"[{store['id']}][SELECT] [OK] 当前已是 ID={expected_id}（下拉框共 {len(rows)} 个店），无需切换")
        else:
            # not_found / click_failed / open_failed：无法确定或切到目标店，终态失败防爬错店
            code = "store_not_available" if status == "not_found" else "store_verify_failed"
            reason = {
                "not_found": f"下拉框中找不到 ID={expected_id}（该子账号可能无此店权限）",
                "click_failed": "点击目标店铺切换失败",
                "open_failed": "店铺下拉框未能展开或读取",
            }.get(status, "店铺选择失败")
            print(f"[{store['id']}][SELECT] [X] {reason}（当前选中 ID={cur_id or '未知'}）")
            _prog(status="error", error=code,
                  last_error=f"{reason}: 期望{expected_id} 当前{cur_id or '未知'}")
            try:
                driver.quit()
            except BaseException:
                pass
            result["error"] = code
            return result

    print("等待捕获首条请求以解析参数...\n")
    first = wait_first_request(driver)
    if first is None:
        print("[ERROR] 超时未捕获到请求，请确认已登录且页面正常加载")
        _prog(status="error", last_error="捕获首条请求超时")
        try:
            driver.quit()
        except BaseException:
            pass
        result["error"] = "capture_timeout"
        return result

    print("[CAPTURED] 请求参数解析成功:")
    print(f"  URL: {first['url']}")
    print(f"  方法: {first['method']}, postData: {first['postData'] or '无'}")
    headers = build_fetch_headers(first["headers"])
    print(f"  重放请求头: {sorted(headers.keys())}")

    # ---- 切换后复核：确认页面确实落在目标店（纯 DOM），防切换未生效而爬错店 ----
    if expected_id is not None and switched:
        _prog(status="verifying_store", store_id=store["id"])
        print(f"[{store['id']}][VERIFY] 切换后复核当前店铺是否为 ID={expected_id} ...")
        selected_id, rows = verify_selected_store(driver)
        if selected_id == expected_id:
            print(f"[{store['id']}][VERIFY] [OK] 一致（下拉框共 {len(rows)} 个店），开始抓取")
        else:
            code = "store_mismatch" if selected_id else "store_verify_failed"
            reason = "切换后店铺仍不匹配" if selected_id else "切换后无法确认当前店铺"
            print(f"[{store['id']}][VERIFY] [X] {reason}：期望 ID={expected_id}，实际 ID={selected_id or '未知'}")
            _prog(status="error", error=code,
                  last_error=f"{reason}: 期望{expected_id} 实际{selected_id or '未知'}")
            try:
                driver.quit()
            except BaseException:
                pass
            result["error"] = code
            return result

    # 请求体解析为 dict，方便每页替换其中的 cursor 字段（游标在请求体里）
    body_json = None
    if first["postData"]:
        try:
            body_json = json.loads(first["postData"])
        except json.JSONDecodeError:
            pass

    buffer, shard_index, call_count, card_count = [], 1, 0, 0
    # 续传时保留断点游标：本轮若一页都没成功，finally 也不会把好断点覆盖成空
    last_cursor = st.get("last_cursor") if st else None
    consecutive_fail = 0
    server_fail = 0
    finished = False
    if st:
        # 断点续传：恢复计数与游标（请求头仍用本次新捕获的，避免 token 过期）
        shard_index = st["shard_index"]
        call_count = st["call_count"]
        card_count = st["card_count"]
        if body_json is not None and st.get("last_cursor"):
            body_json["cursor"] = st["last_cursor"]
        print(f"[RESUME] 从 {out_dir} 续传：第 {call_count + 1} 次调用、分片 {shard_index} 起，"
              f"cursor={st.get('last_cursor')}")
    _prog(status="running", call_count=call_count, card_count=card_count,
          cursor=last_cursor, shard_index=shard_index, out_dir=out_dir, last_error=None)

    try:
        while True:
            # 优雅停止：supervisor/CLI 置位 stop_event 后，最多滞后一个调用周期退出
            if _stopped():
                print("[STOP] 收到停止信号，保存断点后优雅退出...")
                break
            call_count += 1
            post_data = json.dumps(body_json, ensure_ascii=False) if body_json is not None else first["postData"]
            try:
                status, text = call_api(driver, first["url"], first["method"], headers, post_data)
            except Exception as e:
                # 脚本超时/浏览器失去响应等：重试几次，仍失败则保存断点退出
                consecutive_fail += 1
                _prog(status="running", last_error=f"调用异常(连续{consecutive_fail}): {e}")
                print(f"[WARN] 第 {call_count} 次调用异常（连续 {consecutive_fail} 次）: {e}")
                if consecutive_fail >= 3:
                    print("[ERROR] 连续 3 次异常，保存断点后退出，稍后可用 --resume 续传")
                    break
                time.sleep(10)
                call_count -= 1
                continue
            if status == 429:
                # 服务端限流：长暂停后重试，本次不计入失败次数（数据还没拉完，不能提前退出）
                _prog(status="running", last_error="429 限流暂停中")
                print(f"[RATE-LIMIT] 第 {call_count} 次被限流(429)，暂停 {RATE_LIMIT_PAUSE} 秒后重试...")
                time.sleep(RATE_LIMIT_PAUSE)
                call_count -= 1
                continue
            if status != 200:
                if 500 <= status < 600:
                    # WB 后端故障（internalError / 存储节点失联等），与客户端问题区分：
                    # 指数退避长等待（30s→300s 封顶），容忍数小时级的服务中断后自愈
                    server_fail += 1
                    _prog(status="running", last_error=f"HTTP {status} 服务端错误(连续{server_fail})")
                    if server_fail >= SERVER_FAIL_LIMIT:
                        print(f"[ERROR] 服务端连续错误 {SERVER_FAIL_LIMIT} 次，保存断点后退出，"
                              f"服务恢复后用 --resume 续传")
                        break
                    pause = min(30 * 2 ** (server_fail - 1), 300)
                    print(f"[WARN] 第 {call_count} 次调用失败: HTTP {status}（服务端错误，"
                          f"连续 {server_fail} 次），等待 {pause} 秒后重试: {text[:160]}")
                    time.sleep(pause)
                    continue
                consecutive_fail += 1
                _prog(status="running", last_error=f"HTTP {status}(连续{consecutive_fail})")
                print(f"[WARN] 第 {call_count} 次调用失败: HTTP {status}: {text[:200]}")
                if consecutive_fail >= 5:
                    print("[ERROR] 连续失败 5 次，停止拉取（已写入的分片不受影响）")
                    break
                time.sleep(5)
                continue
            consecutive_fail = 0
            server_fail = 0

            try:
                body = json.loads(text)
            except json.JSONDecodeError:
                print(f"[WARN] 第 {call_count} 次返回不是 JSON，停止: {text[:200]}")
                break

            buffer.append(body)
            data = body.get("data") or {}
            cards = data.get("cards") or data.get("list") or []
            card_count += len(cards)
            cursor = data.get("cursor")
            print(f"[PAGE {call_count}] 状态 {status}, 本页 {len(cards)} 条, "
                  f"累计 {card_count} 条, cursor={cursor or '空'}")
            _prog(status="running", call_count=call_count, card_count=card_count,
                  cursor=cursor, shard_index=shard_index, last_error=None)

            # 落盘分片，并同步保存断点状态
            if len(buffer) >= SHARD_SIZE:
                shard_index, shard_path = flush_shard(buffer, shard_index, run_ts, out_dir)
                buffer = []
                save_state(out_dir, run_ts, shard_index, call_count, card_count, cursor)
                on_shard_written(shard_path)

            # 终止条件：无新游标 / 游标不变 / 本页为空；否则把新游标写回请求体翻下一页
            if not cursor or cursor == last_cursor or not cards:
                print("[DONE] 游标耗尽，全部数据拉取完毕")
                finished = True
                break
            last_cursor = cursor
            if body_json is not None:
                body_json["cursor"] = cursor  # 游标是对象/字符串都由服务端原样返回，直接回填
            time.sleep(CALL_INTERVAL)
    except KeyboardInterrupt:
        print("\n手动停止，正在保存已拉取的数据...")
    finally:
        if buffer:
            shard_index, shard_path = flush_shard(buffer, shard_index, run_ts, out_dir)
            on_shard_written(shard_path)
        # 保存断点状态（含最后游标），未完成时可 --resume 续传
        save_state(out_dir, run_ts, shard_index, call_count, card_count, last_cursor, finished)
        summary = {
            "run_ts": run_ts,
            "total_calls": call_count,
            "total_cards": card_count,
            "finished": finished,
            "last_cursor": last_cursor,
            "first_request": first,
        }
        with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"\n共调用 {call_count} 次，累计 {card_count} 条商品，"
              f"分片与汇总已保存到 {out_dir}")
        if not finished:
            print("[提示] 本次未拉完，续传请运行: python fetch_all.py --resume")
        # 等待异步转换上传收尾：正常拉完则等全部结束，中断时最多每个线程 30s
        if upload_threads:
            if finished:
                print(f"[OSS] 等待 {len(upload_threads)} 个分片的转换上传线程结束...")
                for t in upload_threads:
                    t.join()
            else:
                for t in upload_threads:
                    t.join(timeout=30)
                print("[提示] 未完成的上传可稍后用 python wb_to_oss.py --upload-only 补传")
        # 发布终态快照：finished=拉完；stopped=被 stop_event 停；idle=其他退出
        final_status = "finished" if finished else ("stopped" if _stopped() else "idle")
        _prog(status=final_status, finished=finished, call_count=call_count,
              card_count=card_count, cursor=last_cursor, shard_index=shard_index)
        try:
            driver.quit()
        except BaseException:
            pass  # 浏览器已失联或退出动作被 Ctrl+C 打断时，数据已保存，直接忽略

    result.update(finished=finished, call_count=call_count, card_count=card_count, out_dir=out_dir)
    return result


def run_fetch(resume=True, stop_event=None, progress=None, store=None):
    """拉取入口（CLI 与 Flask 宿主共用）。加该店 PID 单实例锁后调用 _fetch_impl。

    Args:
        resume: True 从最近未完成运行的断点续传；False 全新拉取。
        stop_event: threading.Event；置位后在一个调用周期内优雅停止并保存断点。
        progress: dict；持续写入原子进度快照 progress["snap"]，供 /status 读取。
        store: 已解析店铺 dict（config.resolve_store 的产物）；None 时用 config.DEFAULT_STORE。

    Returns:
        dict: {finished, call_count, card_count, out_dir, error}
    """
    if store is None:
        store = config.get_store()
    os.makedirs(store["data_dir"], exist_ok=True)
    lock_file = _lock_file(store["data_dir"])
    if not acquire_single_instance(lock_file):
        if progress is not None:
            progress["snap"] = {
                "status": "locked", "finished": False, "store_id": store["id"],
                "error": "locked", "last_error": "已有进程持锁运行",
                "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {"finished": False, "call_count": 0, "card_count": 0, "out_dir": None, "error": "locked"}
    try:
        return _fetch_impl(resume, progress, stop_event, store)
    except Exception as e:
        # 未预期异常（如 create_driver 失败）：记录后返回，交由 supervisor 决定是否重启
        err = f"{type(e).__name__}: {e}"
        print(f"[{store['id']}][ERROR] 拉取流程异常退出: {err}")
        if progress is not None:
            prev = progress.get("snap") or {}
            merged = dict(prev)
            merged.update(status="error", last_error=err, store_id=store["id"],
                          updated_at=time.strftime("%Y-%m-%d %H:%M:%S"))
            progress["snap"] = merged
        return {"finished": False, "call_count": 0, "card_count": 0, "out_dir": None, "error": err}
    finally:
        release_single_instance(lock_file)


def main():
    """CLI 入口：python fetch_all.py [--resume] [--store <id>]。"""
    ap = argparse.ArgumentParser(description="拉取 WB 卖家后台 tableListv6 全量商品数据")
    ap.add_argument("--resume", action="store_true", help="从最近未完成运行的断点续传")
    ap.add_argument("--store", help="店铺 id（缺省 config.DEFAULT_STORE）")
    args = ap.parse_args()
    store = config.get_store(args.store)
    if store is None:
        ids = ", ".join(s["id"] for s in config.STORES)
        print(f"[ERROR] 未知店铺 id: {args.store}（可选: {ids}）")
        return 2
    run_fetch(resume=args.resume, store=store)
    return 0


if __name__ == "__main__":
    sys.exit(main())
