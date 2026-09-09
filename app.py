# -*- coding: utf-8 -*-
"""Flask 宿主：把多店 fetch_all 各装成常驻后台 worker + cron 巡检，提供监控/控制 HTTP 接口。

用 waitress 常驻（非 Flask dev server），debug/reloader 全关，避免模块双加载 → 双 worker/双 scheduler。
多店并行：config.STORES 里每个店一个 FetchSupervisor（独立 worker 线程 / 锁 / profile / 进度 / 巡检）。

接口（?store=<id> 指定店铺；缺省=config.DEFAULT_STORE；start/stop/resume 传 store=all 遍历全部）:
    GET  /health                    存活探针 + 店铺清单
    GET  /status                    无参=全部店状态字典；?store=<id>=单店快照
    POST /start?store=<id>&resume=1 启动某店 worker（resume=1 续传 / 0 全新；缺省 1）
    POST /resume?store=<id>         等价于 /start?resume=1
    POST /stop?store=<id>           请求优雅停止（当前调用周期结束后退出，巡检不自动重启）

部署：Task Scheduler 登录时启动 `python app.py`，任务勾"仅当用户登录时运行"
（Chrome GUI 需交互式会话，不能跑在 session 0 服务里）。
"""
from flask import Flask, jsonify, request
from waitress import serve

import config
from supervisor import FetchSupervisor

app = Flask(__name__)


def _build_supervisors():
    """每店一个 FetchSupervisor（解析一次 store 配置，带各自的 auto_start）。"""
    sups = {}
    for s in config.STORES:
        st = config.resolve_store(s)
        sups[st["id"]] = FetchSupervisor(st, config.CRON_EXPR, st["auto_start"])
    return sups


# 模块级单例：waitress.serve 直接用本 app 对象，不经导入字符串，杜绝二次加载
supervisors = _build_supervisors()


def _targets(store_param, default_all=False):
    """解析 ?store= 为目标 supervisor 列表。返回 (list, error)。

    - store 为空：default_all=True → 全部店；否则 → DEFAULT_STORE 单店
    - store == "all"：全部店
    - 指定 id：命中返回单店；未知 id 返回 (None, 错误信息)
    """
    if store_param in (None, ""):
        if default_all:
            return list(supervisors.values()), None
        sup = supervisors.get(config.DEFAULT_STORE)
        return ([sup] if sup else []), None
    if store_param == "all":
        return list(supervisors.values()), None
    sup = supervisors.get(store_param)
    if sup is None:
        return None, f"未知店铺 id: {store_param}（可选: {', '.join(supervisors)}）"
    return [sup], None


@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "wb-fetch-host", "stores": list(supervisors)})


@app.get("/status")
def status():
    store = request.args.get("store")
    targets, err = _targets(store, default_all=True)
    if err:
        return jsonify({"ok": False, "error": err}), 404
    # 显式指定单店时直接返回该店快照；无参或 all 时返回 {store_id: 快照} 字典
    if store not in (None, "", "all") and len(targets) == 1:
        return jsonify(targets[0].get_status())
    return jsonify({s.store_id: s.get_status() for s in targets})


def _batch(action, resume=None):
    """start/resume/stop 的公共批处理：对目标店逐个执行并汇总结果。"""
    targets, err = _targets(request.args.get("store"))
    if err:
        return jsonify({"ok": False, "error": err}), 404
    results, allok = {}, True
    for sup in targets:
        if action == "stop":
            ok, msg = sup.stop_worker()
        else:
            r = resume if resume is not None else (
                request.args.get("resume", "1") not in ("0", "false", "False", ""))
            ok, msg = sup.start_worker(resume=r)
        results[sup.store_id] = {"ok": ok, "message": msg}
        allok = allok and ok
    return jsonify({"ok": allok, "results": results}), (200 if allok else 409)


@app.post("/start")
def start():
    return _batch("start")


@app.post("/resume")
def resume():
    return _batch("start", resume=True)


@app.post("/stop")
def stop():
    return _batch("stop")


def main():
    for sup in supervisors.values():
        sup.start()
    try:
        print(f"[HOST] waitress 监听 http://{config.HOST}:{config.PORT}"
              f"（店铺: {', '.join(supervisors)}；/health /status /start /stop /resume）")
        serve(app, host=config.HOST, port=config.PORT, threads=8)
    finally:
        for sup in supervisors.values():
            sup.shutdown()


if __name__ == "__main__":
    main()
