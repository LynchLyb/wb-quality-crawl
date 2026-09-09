# -*- coding: utf-8 -*-
"""阿里云 OSS 连通性 / 权限自检脚本（上传 tableListv6 目录前的体检）。

检查项:
    1. 配置完整性  : AccessKey、endpoint、bucket 是否齐全
    2. 本地目录体检: 待上传目录的文件数、总大小、最大文件（决定用哪种上传方式）
    3. 网络层      : DNS 解析、TCP 连接、TLS 握手耗时（区分"网络不通"和"鉴权失败"）
    4. 鉴权 + 读   : GetBucketInfo（校验区域是否匹配）、ListObjects
    5. 写权限      : 在目标前缀下 Put/Head/Get/Delete 一个探测小文件
    6. 分片上传    : InitiateMultipartUpload + UploadPart + Abort（大文件必需）
    7. 上传测速    : 可选，上传 8 MiB 实测速率并估算整个目录耗时

只读模式（--read-only）仅做第 1~4 项，不会在 Bucket 里创建任何对象。

配置来源（优先级从低到高）: oss_config.json < 环境变量 < 命令行参数
    环境变量: OSS_ACCESS_KEY_ID / OSS_ACCESS_KEY_SECRET / OSS_ENDPOINT /
              OSS_BUCKET / OSS_PREFIX / OSS_REGION / OSS_STS_TOKEN

用法:
    python oss_conn_test.py --init-config        # 生成 oss_config.json 模板后再填写
    python oss_conn_test.py                      # 读 oss_config.json / 环境变量做全量自检
    python oss_conn_test.py --read-only          # 只测连通性与读权限，不写任何对象
    python oss_conn_test.py --speed-test         # 额外做上传测速
    python oss_conn_test.py --endpoint oss-cn-hangzhou.aliyuncs.com --bucket my-bucket \
        --access-key-id LTAI... --access-key-secret xxxx --prefix tableListv6/
"""
import argparse
import json
import os
import re
import socket
import ssl
import sys
import time
from urllib.parse import urlparse

try:
    import oss2
    from oss2.exceptions import OssError
except ImportError:
    print("[ERROR] 未安装 oss2，请先执行: .\\.venv\\Scripts\\python.exe -m pip install oss2")
    sys.exit(2)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DIR = os.path.join(BASE_DIR, "tableListv6_20260901_191657")
DEFAULT_CONFIG = os.path.join(BASE_DIR, "oss_config.json")
CONFIG_TEMPLATE = {
    "access_key_id": "在这里填 AccessKey ID",
    "access_key_secret": "在这里填 AccessKey Secret",
    "endpoint": "https://oss-cn-hangzhou.aliyuncs.com",
    "bucket": "在这里填 Bucket 名称",
    "prefix": "tableListv6/",          # 上传到 Bucket 内的目标目录前缀，可为空
    "region": "cn-hangzhou",           # 仅 --auth v4 时需要，如 cn-hangzhou
    "sts_token": "",                   # 使用 STS 临时凭证时才填
}
ENV_KEYS = {
    "access_key_id": "OSS_ACCESS_KEY_ID",
    "access_key_secret": "OSS_ACCESS_KEY_SECRET",
    "endpoint": "OSS_ENDPOINT",
    "bucket": "OSS_BUCKET",
    "prefix": "OSS_PREFIX",
    "region": "OSS_REGION",
    "sts_token": "OSS_STS_TOKEN",
}
# 常见 OSS 错误码 -> 排查建议
ERROR_HINTS = {
    "InvalidAccessKeyId": "AccessKey ID 不存在/已删除/已禁用，或复制时带了空格。",
    "SignatureDoesNotMatch": "AccessKey Secret 不正确（注意首尾空格、是否用了别的主账号密钥）。",
    "AccessDenied": "RAM 用户/角色缺少该操作权限，需授予对应 Bucket 的读写策略。",
    "NoSuchBucket": "Bucket 名写错，或 Bucket 不在该 endpoint 对应的区域。",
    "InvalidBucketName": "Bucket 名不合法（小写字母/数字/短横线，3-63 字符）。",
    "SecurityTokenExpired": "STS 临时凭证已过期，需重新获取。",
    "InvalidAccessKeyId.NotFound": "同 InvalidAccessKeyId。",
    "RequestTimeTooSkewed": "本机系统时间与标准时间偏差超过 15 分钟，请校时。",
    "SecondLevelDomainForbidden": "不允许用二级域名方式访问，请检查 endpoint 写法。",
    "KmsServiceDisabled": "Bucket 开了 KMS 加密但服务未开通。",
    "UserDisable": "该 AccessKey 所属账号/用户已被禁用（常见于欠费停机、账号冻结、"
                   "或 RAM 用户被停用），需先在控制台恢复账号状态或换一把有效 AK。",
}
PART_MIN_SIZE = 200 * 1024      # 分片探测用的分片大小（> 100 KiB 下限即可）
SPEED_TEST_SIZE = 8 * 1024 * 1024


def log(tag, msg):
    print(f"[{tag}] {msg}")


def hint_of(err):
    """把 oss2 异常翻译成可执行的排查建议。"""
    code = getattr(err, "code", "") or ""
    status = getattr(err, "status", "") or ""
    request_id = getattr(err, "request_id", "") or ""
    lines = [f"HTTP {status} / Code={code or '未知'} / RequestId={request_id or '无'}"]
    if code in ERROR_HINTS:
        lines.append(f"建议: {ERROR_HINTS[code]}")
    body = getattr(err, "body", "") or ""
    if isinstance(body, bytes):
        body = body.decode("utf-8", "replace")
    m = re.search(r"<Message>(.*?)</Message>", str(body), re.S)
    text = (m.group(1) if m else str(body)).strip().replace("\n", " ")
    if text:
        lines.append(f"OSS 返回: {text[:180]}")
    if "does not belong to you" in text:
        lines.append("判断: 该 Bucket 存在，但不属于当前 AccessKey 所在的阿里云账号（跨账号访问），"
                     "或 Bucket 名拼写有误；需要用 Bucket 所属账号的 AK，"
                     "或让拥有者配置 Bucket Policy / RAM 授权。")
    elif code == "UserDisable":
        lines.append("判断: 请求已到达 OSS 并识别出账号，但该账号处于禁用状态，"
                     "任何读写都会被拒；先处理账号状态再试。")
    elif code == "AccessDenied":
        lines.append("说明: 返回 403 而不是 SignatureDoesNotMatch，"
                     "代表 AK/SK 签名已校验通过（凭证本身有效），问题在授权范围。")
    return "\n        ".join(lines)


# ---------------------------------------------------------------- 配置加载

def init_config(path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(CONFIG_TEMPLATE, f, ensure_ascii=False, indent=2)
    log("OK", f"已生成配置模板: {path}")
    print("        请填写后重新运行: python oss_conn_test.py")
    print("        注意: 该文件含密钥，不要提交到 git（或用环境变量代替）。")


def load_config(args):
    """合并 配置文件 / 环境变量 / 命令行参数，返回配置 dict。"""
    cfg = {}
    cfg_path = args.config or DEFAULT_CONFIG
    if os.path.exists(cfg_path):
        with open(cfg_path, encoding="utf-8") as f:
            loaded = json.load(f)
        cfg.update({k: v for k, v in loaded.items() if v})
        log("INFO", f"已读取配置文件: {cfg_path}")
    for key, env in ENV_KEYS.items():
        val = os.environ.get(env)
        if val:
            cfg[key] = val
    for key in ENV_KEYS:
        val = getattr(args, key, None)
        if val:
            cfg[key] = val
    cfg.setdefault("prefix", "")
    if cfg.get("prefix") and not cfg["prefix"].endswith("/"):
        cfg["prefix"] += "/"
    return cfg


def check_config(cfg):
    """校验必填项，返回缺失项列表。"""
    missing = []
    for key in ("access_key_id", "access_key_secret", "endpoint", "bucket"):
        val = str(cfg.get(key) or "").strip()
        if not val or val.startswith("在这里填"):
            missing.append(key)
    return missing


def normalize_endpoint(endpoint):
    if "://" not in endpoint:
        endpoint = "https://" + endpoint
    return endpoint.rstrip("/")


def make_auth(cfg):
    """根据配置构造签名对象（STS / V4 / V1）。"""
    if cfg.get("sts_token"):
        return oss2.StsAuth(cfg["access_key_id"], cfg["access_key_secret"], cfg["sts_token"])
    if cfg.get("auth") == "v4":
        if not cfg.get("region"):
            raise ValueError("--auth v4 需要同时提供 region（如 cn-hangzhou）")
        return oss2.AuthV4(cfg["access_key_id"], cfg["access_key_secret"])
    return oss2.Auth(cfg["access_key_id"], cfg["access_key_secret"])


def make_bucket(cfg, timeout):
    endpoint = normalize_endpoint(cfg["endpoint"])
    auth = make_auth(cfg)
    kwargs = {"connect_timeout": timeout}
    if cfg.get("auth") == "v4":
        kwargs["region"] = cfg["region"]
    return oss2.Bucket(auth, endpoint, cfg["bucket"], **kwargs), endpoint


def diagnose_ownership(cfg, timeout):
    """读权限失败时的归因诊断：列出当前 AK 所属账号能看到的 Bucket（只读）。"""
    print("\n----- 归属诊断（ListBuckets，只读）-----")
    try:
        service = oss2.Service(make_auth(cfg), normalize_endpoint(cfg["endpoint"]),
                               connect_timeout=timeout)
        buckets = list(oss2.BucketIterator(service))
    except OssError as e:
        log("WARN", f"ListBuckets 失败（RAM 用户常无 oss:ListBuckets 权限，不影响上传）:\n        {hint_of(e)}")
        if getattr(e, "code", "") == "UserDisable":
            log("ERROR", "ListBuckets 返回 UserDisable：这不是权限不够，而是 AK 所属账号被禁用，"
                         "属于必须先解决的阻塞项")
        return
    except Exception as e:
        log("WARN", f"ListBuckets 异常: {type(e).__name__}: {e}")
        return
    if not buckets:
        log("WARN", "该 AccessKey 名下看不到任何 Bucket，请确认 AK 是否属于目标账号")
        return
    log("INFO", f"当前 AK 可访问的 Bucket 共 {len(buckets)} 个:")
    target = cfg["bucket"]
    for b in buckets[:50]:
        mark = "  <== 就是它" if b.name == target else ""
        print(f"        - {b.name}  ({b.location}){mark}")
    if not any(b.name == target for b in buckets):
        log("WARN", f"列表里没有 '{target}'：要么名字写错，要么它属于另一个阿里云账号")


# ---------------------------------------------------------------- 各项检查

def check_local_dir(path):
    """统计待上传目录，返回 (文件数, 总字节, 最大文件字节)；目录不存在返回 None。"""
    if not os.path.isdir(path):
        return None
    count, total, biggest = 0, 0, 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                size = os.path.getsize(os.path.join(root, name))
            except OSError:
                continue
            count += 1
            total += size
            biggest = max(biggest, size)
    return count, total, biggest


def check_network(endpoint, timeout):
    """DNS + TCP + TLS 分层探测，返回 (是否通过, 耗时字典)。"""
    parsed = urlparse(endpoint)
    host, scheme = parsed.hostname, parsed.scheme
    port = parsed.port or (443 if scheme == "https" else 80)
    timing = {}

    t0 = time.time()
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as e:
        return False, f"DNS 解析失败: {e}（检查 endpoint 拼写，或本机 DNS/hosts）"
    timing["dns"] = time.time() - t0
    ips = sorted({i[4][0] for i in infos})
    log("INFO", f"DNS: {host} -> {', '.join(ips[:4])}（{timing['dns'] * 1000:.0f} ms）")

    t0 = time.time()
    try:
        sock = socket.create_connection((ips[0], port), timeout=timeout)
    except OSError as e:
        return False, f"TCP 连接 {ips[0]}:{port} 失败: {e}（检查防火墙/代理/是否公网可达）"
    timing["tcp"] = time.time() - t0
    log("INFO", f"TCP: {ips[0]}:{port} 已连通（{timing['tcp'] * 1000:.0f} ms）")

    if scheme == "https":
        t0 = time.time()
        try:
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
                timing["tls"] = time.time() - t0
                log("INFO", f"TLS: {ssock.version()} 握手完成（{timing['tls'] * 1000:.0f} ms），"
                            f"证书到期 {ssock.getpeercert().get('notAfter', '未知')}")
        except (ssl.SSLError, OSError) as e:
            sock.close()
            return False, f"TLS 握手失败: {e}（系统时间不对/证书链缺失/被中间人代理拦截）"
    else:
        sock.close()
    return True, timing


def check_read(bucket, cfg, timeout=15):
    """GetBucketInfo（非致命，用于区域校验）+ ListObjects（致命，校验签名与读权限）。"""
    try:
        info = bucket.get_bucket_info()
        log("OK", f"GetBucketInfo 成功: 区域={info.location}, 存储类型={info.storage_class}, "
                  f"创建时间={info.creation_date}")
        ep_region = normalize_endpoint(cfg["endpoint"]).split("//")[-1].split(".")[0]
        if info.location and ep_region and info.location != ep_region:
            log("WARN", f"endpoint({ep_region}) 与 Bucket 实际区域({info.location}) 不一致，"
                        f"虽可能仍可用，但建议改成 https://{info.location}.aliyuncs.com 以获得最佳速度")
    except OssError as e:
        log("WARN", f"GetBucketInfo 失败（不影响上传，多为缺少 oss:GetBucketInfo 权限）:\n        {hint_of(e)}")
    except Exception as e:
        log("WARN", f"GetBucketInfo 异常: {e}")

    try:
        result = bucket.list_objects_v2(prefix=cfg["prefix"], max_keys=5)
        existing = [o.key for o in result.object_list]
        log("OK", f"ListObjects 成功（签名有效、读权限正常）: prefix='{cfg['prefix']}' 下已有 {len(existing)} 个对象示例")
        for key in existing[:5]:
            print(f"        - {key}")
        return True
    except OssError as e:
        log("ERROR", f"ListObjects 失败:\n        {hint_of(e)}")
    except Exception as e:
        log("ERROR", f"ListObjects 异常: {type(e).__name__}: {e}")
    diagnose_ownership(cfg, timeout)
    return False


def check_write(bucket, cfg):
    """在目标前缀下写一个探测文件，校验 Put/Head/Get/Delete 全链路。"""
    key = f"{cfg['prefix']}_oss_conn_test/probe_{int(time.time())}_{os.getpid()}.txt"
    payload = f"oss connectivity probe at {time.strftime('%Y-%m-%d %H:%M:%S')}".encode("utf-8")
    try:
        bucket.put_object(key, payload)
        log("OK", f"PutObject 成功: {key}")
    except OssError as e:
        log("ERROR", f"PutObject 失败:\n        {hint_of(e)}")
        return False
    except Exception as e:
        log("ERROR", f"PutObject 异常: {type(e).__name__}: {e}")
        return False

    try:
        meta = bucket.head_object(key)
        assert int(meta.content_length) == len(payload), "大小不一致"
        back = bucket.get_object(key).read()
        assert back == payload, "内容不一致"
        log("OK", f"HeadObject + GetObject 校验通过（{len(payload)} 字节, ETag={meta.etag}）")
    except OssError as e:
        log("WARN", f"读取校验失败（写已成功，可能缺 GetObject 权限）:\n        {hint_of(e)}")
    except Exception as e:
        log("WARN", f"读取校验异常: {type(e).__name__}: {e}")

    try:
        bucket.delete_object(key)
        log("OK", f"DeleteObject 成功，探测文件已清理: {key}")
    except OssError as e:
        log("WARN", f"DeleteObject 失败，请手动清理 {key}:\n        {hint_of(e)}")
    except Exception as e:
        log("WARN", f"DeleteObject 异常: {e}")
    return True


def check_multipart(bucket, cfg):
    """分片上传权限探测：init -> upload_part -> abort（不留下残片）。"""
    key = f"{cfg['prefix']}_oss_conn_test/multipart_probe_{int(time.time())}.bin"
    upload_id = None
    try:
        upload_id = bucket.init_multipart_upload(key).upload_id
        part = bucket.upload_part(key, upload_id, 1, b"A" * PART_MIN_SIZE)
        bucket.list_parts(key, upload_id)
        log("OK", f"分片上传可用: InitiateMultipartUpload + UploadPart(200 KiB, ETag={part.etag})")
        return True
    except OssError as e:
        log("ERROR", f"分片上传失败（大文件/断点续传会受影响）:\n        {hint_of(e)}")
    except Exception as e:
        log("ERROR", f"分片上传异常: {type(e).__name__}: {e}")
    finally:
        if upload_id:
            try:
                bucket.abort_multipart_upload(key, upload_id)
                log("INFO", "已 AbortMultipartUpload 清理探测分片")
            except Exception as e:
                log("WARN", f"清理探测分片失败（可在控制台碎片管理中删除）: {e}")
    return False


def check_speed(bucket, cfg):
    """实测上传速率，返回 MB/s；失败返回 None。"""
    key = f"{cfg['prefix']}_oss_conn_test/speed_{int(time.time())}.bin"
    data = os.urandom(SPEED_TEST_SIZE)
    try:
        t0 = time.time()
        bucket.put_object(key, data)
        elapsed = max(time.time() - t0, 1e-6)
    except Exception as e:
        log("WARN", f"测速上传失败: {type(e).__name__}: {e}")
        return None
    finally:
        try:
            bucket.delete_object(key)
        except Exception:
            pass
    mbps = SPEED_TEST_SIZE / 1024 / 1024 / elapsed
    log("OK", f"上传测速: {SPEED_TEST_SIZE // 1024 // 1024} MiB 用时 {elapsed:.2f}s，"
              f"约 {mbps:.2f} MB/s（{mbps * 8:.1f} Mbps）")
    return mbps


# ---------------------------------------------------------------- 主流程

def parse_args():
    p = argparse.ArgumentParser(description="阿里云 OSS 连通性自检")
    p.add_argument("--init-config", action="store_true", help="生成 oss_config.json 模板")
    p.add_argument("--config", help=f"配置文件路径（默认 {DEFAULT_CONFIG}）")
    p.add_argument("--access-key-id", dest="access_key_id")
    p.add_argument("--access-key-secret", dest="access_key_secret")
    p.add_argument("--sts-token", dest="sts_token")
    p.add_argument("--endpoint", help="如 https://oss-cn-hangzhou.aliyuncs.com")
    p.add_argument("--bucket")
    p.add_argument("--prefix", help="Bucket 内目标前缀，如 tableListv6/")
    p.add_argument("--region", help="仅 --auth v4 需要，如 cn-hangzhou")
    p.add_argument("--auth", choices=["v1", "v4"], default="v1", help="签名版本，默认 v1")
    p.add_argument("--dir", default=DEFAULT_DIR, help="待上传目录（仅统计用）")
    p.add_argument("--timeout", type=float, default=15.0, help="连接超时秒数，默认 15")
    p.add_argument("--speed-test", action="store_true", help="额外做 8 MiB 上传测速并估算总耗时")
    p.add_argument("--read-only", action="store_true",
                   help="只读模式：仅做 DNS/TCP/TLS + GetBucketInfo + ListObjects，不写入任何对象")
    return p.parse_args()


def main():
    args = parse_args()
    if args.init_config:
        init_config(args.config or DEFAULT_CONFIG)
        return 0

    print("=" * 72)
    print("阿里云 OSS 连通性自检")
    print("=" * 72)

    cfg = load_config(args)
    cfg["auth"] = args.auth
    missing = check_config(cfg)
    if missing:
        log("ERROR", f"配置缺失: {', '.join(missing)}")
        print("        三种提供方式任选其一:")
        print("        1) python oss_conn_test.py --init-config 生成模板并填写 oss_config.json")
        print("        2) 环境变量: set OSS_ACCESS_KEY_ID=xxx / OSS_ACCESS_KEY_SECRET=xxx / "
              "OSS_ENDPOINT=xxx / OSS_BUCKET=xxx")
        print("        3) 命令行: --endpoint ... --bucket ... --access-key-id ... --access-key-secret ...")
        return 2

    for key in ("access_key_id", "access_key_secret", "sts_token"):
        if cfg.get(key):
            cfg[key] = cfg[key].strip()
    ak = cfg["access_key_id"]
    log("INFO", f"AccessKey: {ak[:4]}****{ak[-4:] if len(ak) > 8 else ''}  "
                f"Bucket: {cfg['bucket']}  Prefix: '{cfg['prefix'] or '(根目录)'}'  签名: {args.auth.upper()}")
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        if os.environ.get(var):
            log("WARN", f"检测到代理 {var}={os.environ[var]}，若连接异常可先关掉代理重试")
    if "-internal.aliyuncs.com" in cfg["endpoint"]:
        log("WARN", "使用的是内网 endpoint，只能在同区域 ECS/VPC 内访问；本机公网请去掉 -internal")
    if normalize_endpoint(cfg["endpoint"]).startswith("http://"):
        log("WARN", "endpoint 是 http 明文（请求头里会带签名，且不走 TLS），"
                    "建议改成 https:// 开头")

    results = {}

    # 1. 本地目录体检
    stats = check_local_dir(args.dir)
    if stats is None:
        log("WARN", f"待上传目录不存在: {args.dir}（仅影响耗时估算，不影响连通性测试）")
    else:
        count, total, biggest = stats
        log("OK", f"本地目录: {args.dir}")
        print(f"        文件 {count} 个，总大小 {total / 1024 / 1024:.1f} MB，"
              f"最大单文件 {biggest / 1024 / 1024:.1f} MB")
        print(f"        建议: 单文件 > 100 MB 用 oss2.resumable_upload（分片 + 断点续传），"
              f"其余用 put_object 即可")
    results["本地目录"] = stats is not None

    # 2. 网络层
    endpoint = normalize_endpoint(cfg["endpoint"])
    ok, detail = check_network(endpoint, args.timeout)
    results["网络层"] = ok
    if not ok:
        log("ERROR", detail)
        print("\n网络层不通，后续鉴权测试无意义，已提前结束。")
        return 1

    # 3~6. 走 OSS API 的检查
    try:
        bucket, endpoint = make_bucket(cfg, args.timeout)
    except ValueError as e:
        log("ERROR", str(e))
        return 2

    results["鉴权与读权限"] = check_read(bucket, cfg, args.timeout)
    if args.read_only:
        log("INFO", "--read-only 模式：跳过写入/分片/测速测试，不会在 Bucket 里产生任何对象")
        results["写权限"] = None
        results["分片上传"] = None
    else:
        results["写权限"] = check_write(bucket, cfg) if results["鉴权与读权限"] else False
        results["分片上传"] = check_multipart(bucket, cfg) if results["写权限"] else False

    # 7. 可选测速
    mbps = (check_speed(bucket, cfg)
            if args.speed_test and not args.read_only and results.get("写权限") else None)
    if mbps and stats:
        eta = stats[1] / 1024 / 1024 / mbps
        log("INFO", f"按实测速率估算：上传 {stats[1] / 1024 / 1024:.1f} MB 约需 "
                    f"{eta / 60:.1f} 分钟（单线程串行，未计并发与小文件请求开销）")

    print("\n" + "=" * 72)
    print("自检结果汇总")
    print("=" * 72)
    for name, ok in results.items():
        mark = "[通过]" if ok is True else ("[跳过]" if ok is None else "[失败]")
        print(f"  {mark} {name}")
    failed = [n for n, ok in results.items() if ok is False]
    if not failed:
        if args.read_only:
            print("\n只读检查全部通过：网络、签名、Bucket 与读权限都正常。")
            print("写入权限尚未验证，正式上传前可去掉 --read-only 再跑一次。")
        else:
            print("\n全部通过，可以开始上传目录了。")
        return 0
    if results["网络层"]:
        print("\n网络层已通（DNS/TCP/TLS 正常），失败项集中在凭证、Bucket 或权限层面。")
    print(f"存在失败项: {', '.join(failed)}，请按上面的建议排查后重试。")
    return 1


if __name__ == "__main__":
    sys.exit(main())
