# -*- coding: utf-8 -*-
"""掉线自愈：某店 WB 登录态失效时，从同账号下仍健康的兄弟店 profile 克隆登录态。

背景：多店共用同一个 WB 账号，但每店用各自独立的 Chrome profile（cookie 隔离、各自
独立老化）。因此常见"部分店掉线、部分店仍在线"。DPAPI cookie 绑定 Windows 用户而非
目录，同机同用户下把一个健康店的 profile 复制给掉线店即可免人工重登；克隆后
fetch_by_nmid 的抓取前自动选店（ensure_selected_store）会把下拉框切到掉线店自己的目标
卖家 ID，故克隆来的会话停在源店也不影响。

安全边界（务必遵守，否则会放大故障）：
  - 只在确认源店"当前已登录"后才克隆（本轮已知健康 or 探针实测），绝不把死会话扩散；
  - 克隆前先 pre_launch_cleanup 目标店残留 chrome，并排除 lockfile/DevToolsActivePort，
    否则目标店 Chrome 启动会误判"已在运行"；
  - 克隆后校验关键文件存在且 Cookies 的 MD5 与源一致；
  - 每店带冷却窗口，克隆失败不紧凑重试，交回上层长退避 + 人工 login_store.py。
"""
import hashlib
import os
import subprocess
import time

import config
from script import PAGE_URL, create_driver
from fetch_all import pre_launch_cleanup, wait_first_request

# 落地 URL 含这些片段即视为被踢到登录/鉴权页（与 _check_login.py 一致）
LOGIN_HINTS = ("login", "passport", "auth", "signin", "sso", "identity")

# 触发克隆自愈的错误码：掉线（落地登录页）或下拉框展不开（登录页无店铺 radio）
AUTH_HEAL_ERRORS = frozenset({"not_logged_in", "store_select_open_failed"})

# 同一店两次自愈之间的最小间隔（秒）：防止源也失效时反复克隆抖动
HEAL_COOLDOWN = 900
_last_heal = {}

# 克隆时需排除的 Chrome 锁文件（Windows 无 SingletonLock，只这两个）
_LOCK_FILES = ("lockfile", "DevToolsActivePort")
# 克隆后必须存在的关键文件（相对 profile 根）
_KEY_FILES = ("Local State", os.path.join("Default", "Network", "Cookies"),
              os.path.join("Default", "Login Data"), os.path.join("Default", "Preferences"))


def looks_logged_out(url):
    """落地 URL 是否被重定向到登录/鉴权页（掉线信号）。"""
    u = (url or "").lower()
    return any(h in u for h in LOGIN_HINTS)


def _cookies_path(profile_dir):
    return os.path.join(profile_dir, "Default", "Network", "Cookies")


def _md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def probe_logged_in(store, capture_timeout=30, log=print):
    """只读探针：开该店 profile -> 访问卖家页 -> 判断是否仍登录（不抓数据）。

    返回 True=已登录（落地非登录页且捕获到 tableListv6）；False=掉线/打不开/被占用。
    探针会启动再关闭一个浏览器，故只在需要判断未知兄弟店健康度时调用。
    """
    pdir = store["profile_dir"]
    if not os.path.isdir(pdir):
        log(f"[HEAL-PROBE] {store['id']} profile 不存在: {pdir}")
        return False
    driver = None
    try:
        pre_launch_cleanup(pdir)
        driver = create_driver(headless=False, profile_dir=pdir)
        driver.set_script_timeout(60)
        driver.get(PAGE_URL)
        time.sleep(3)
        cur = driver.current_url or ""
        if looks_logged_out(cur):
            log(f"[HEAL-PROBE] {store['id']} 掉线（落地 {cur}）")
            return False
        first = wait_first_request(driver, capture_timeout)
        ok = first is not None
        log(f"[HEAL-PROBE] {store['id']} {'在线' if ok else '未捕获到 tableListv6'}")
        return ok
    except Exception as e:
        log(f"[HEAL-PROBE] {store['id']} 探针异常: {type(e).__name__}: {e}")
        return False
    finally:
        if driver is not None:
            try:
                driver.quit()
            except BaseException:
                pass
            time.sleep(2)


def clone_profile(src_store, dst_store, log=print):
    """把 src 店已登录 profile 克隆给 dst 店（robocopy /E，排除锁文件），并校验。

    返回 (ok, msg)。ok=True 表示关键文件齐全、锁文件不存在、Cookies MD5 与源一致。
    """
    src, dst = src_store["profile_dir"], dst_store["profile_dir"]
    if not os.path.isdir(src):
        return False, f"源 profile 不存在: {src}"
    # 1) 清理目标店残留 chrome（掉线店的浏览器可能仍占着 profile），并删陈旧锁文件
    pre_launch_cleanup(dst)
    for lf in _LOCK_FILES:
        p = os.path.join(dst, lf)
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass
    # 2) 备份目标店原 Cookies 以便回滚（best-effort）
    dst_cookies = _cookies_path(dst)
    if os.path.exists(dst_cookies):
        try:
            bak = dst_cookies + ".preheal." + time.strftime("%Y%m%d_%H%M%S")
            with open(dst_cookies, "rb") as fi, open(bak, "wb") as fo:
                fo.write(fi.read())
        except OSError as e:
            log(f"[HEAL] 备份目标 Cookies 失败（继续克隆）: {e}")
    # 3) robocopy 整目录克隆（排除锁文件）；exit code <8 视为成功
    cmd = ["robocopy", src, dst, "/E", "/XF", *_LOCK_FILES,
           "/R:1", "/W:1", "/NFL", "/NDL", "/NP", "/NJH", "/NJS"]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True,
                           creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    except Exception as e:
        return False, f"robocopy 调用失败: {type(e).__name__}: {e}"
    if p.returncode >= 8:
        return False, f"robocopy 失败 exit={p.returncode}: {(p.stderr or '').strip()[:200]}"
    # 4) 校验：关键文件存在、锁文件不存在、Cookies MD5 与源一致
    missing = [k for k in _KEY_FILES if not os.path.exists(os.path.join(dst, k))]
    if missing:
        return False, f"克隆后关键文件缺失: {missing}"
    leftover = [lf for lf in _LOCK_FILES if os.path.exists(os.path.join(dst, lf))]
    if leftover:
        return False, f"克隆后仍有锁文件: {leftover}"
    try:
        if _md5(_cookies_path(src)) != _md5(dst_cookies):
            return False, "Cookies MD5 与源不一致"
    except OSError as e:
        return False, f"Cookies 校验失败: {e}"
    return True, f"已从 {src_store['id']} 克隆登录态（robocopy exit={p.returncode}, Cookies MD5 一致）"


def heal_from_sibling(target_id, preferred=(), log=print):
    """给掉线的 target_id 找一个健康兄弟店克隆登录态。

    preferred: 本轮已知健康的店 id（优先直接克隆，免探针）；其余配置了卖家 ID 的店
    会逐个探针确认在线后再克隆。返回 (ok, src_id)。带冷却，避免源也失效时反复抖动。
    """
    now = time.time()
    if now - _last_heal.get(target_id, 0) < HEAL_COOLDOWN:
        log(f"[HEAL] {target_id} 距上次自愈不足 {HEAL_COOLDOWN // 60} 分钟，跳过（冷却中）")
        return False, None
    _last_heal[target_id] = now   # 无论成败都置冷却，交回上层长退避，不紧凑重试

    target = config.get_store(target_id)
    if target is None:
        return False, None

    # 候选源：preferred 在前（已知健康），再补其余配置了卖家 ID 的店（需探针）
    ordered = [s for s in preferred if s != target_id]
    for s in config.STORES:
        sid = s["id"]
        if sid == target_id or sid in ordered:
            continue
        if config.get_seller_id(sid):
            ordered.append(sid)

    for cand_id in ordered:
        cand = config.get_store(cand_id)
        if cand is None:
            continue
        if cand_id in preferred:
            log(f"[HEAL] 候选源 {cand_id}（本轮已知健康），直接克隆")
        else:
            log(f"[HEAL] 候选源 {cand_id} 健康度未知，探针确认中...")
            if not probe_logged_in(cand, log=log):
                continue
        ok, msg = clone_profile(cand, target, log=log)
        if ok:
            log(f"[HEAL] {target_id} {msg}")
            return True, cand_id
        log(f"[HEAL] {target_id} 从 {cand_id} 克隆失败: {msg}，尝试下一个候选")
    log(f"[HEAL] {target_id} 无健康兄弟店可克隆（可能全部掉线），需人工 login_store.py {target_id}")
    return False, None
