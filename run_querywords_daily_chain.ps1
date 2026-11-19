#Requires -Version 5.1
<#
.SYNOPSIS
    wb_querywords 每日串行链：先跑搜索词 batch 用完当日推荐配额，再交回 fetch_all。

.DESCRIPTION
    背景：推荐接口每日仅 100 次硬配额，且 wb_querywords 与 fetch_all 共用 store1
    的 Chrome profile，二者不能并行，必须串行。故不用 wb_querywords 常驻宿主，改由
    本脚本每日串三步：
        1) 优雅停 fetch_all worker（POST /stop），等它退出释放 store1 profile；
        2) 跑 python -m wb_querywords.batch（one_shot）：跑完当天 input 或命中
           limitExceeded（日配额用尽）即自动退出进程；
        3) 重新拉起 fetch_all worker（POST /start?resume=1）继续几小时的大拉。

    部署前提：
        - WB_FetchHost（8080 常驻宿主，AUTO_START + 巡检自愈）保持启用；
        - 不要启用 WB_QuerywordsHost（常驻宿主），否则会与 batch 抢同一 run.lock；
        - 本脚本注册为每日计划任务 WB_QuerywordsDailyChain，BJT 10:00 触发（配额重置后）。

    A / B 机通用：脚本只调模块，两类型差异（RECOM_TYPES）在各分支的 config.py 内，
    本脚本无需区分。请按本机实际项目根修改 $ProjectRoot。

.NOTES
    退出码：0 正常收尾（跑完 / 配额用尽 / 无 input）；1 fetch_all 未及时让出 profile。
#>

$ErrorActionPreference = "Stop"

# ==== 按本机实际情况修改 ====
$ProjectRoot    = "C:\Users\yuanbo\PycharmProjects\WelcomeScreen"  # 项目根目录
$Python         = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$FetchHostBase  = "http://127.0.0.1:8080"   # fetch_all 常驻宿主（根 app.py）
$Store          = "store1"                  # 两模块共用的店铺 profile
$StopTimeoutSec = 180                       # 等 fetch_all worker 优雅退出的上限（秒）
# ============================

$LogDir  = Join-Path $ProjectRoot "logs"
$LogFile = Join-Path $LogDir ("querywords_chain_{0}.log" -f (Get-Date -Format "yyyyMMdd"))

function Log($msg) {
    $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $msg
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
}

if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir -Force | Out-Null }
Set-Location $ProjectRoot

Log "===== 每日链开始（store=$Store）====="

# ---- 1. 优雅停 fetch_all worker，释放 store1 profile ----
$stopped = $true
try {
    Invoke-RestMethod -Method Post -Uri "$FetchHostBase/stop?store=$Store" | Out-Null
    Log "已请求 fetch_all /stop，等待 worker 退出（最多 ${StopTimeoutSec}s）..."
    $deadline = (Get-Date).AddSeconds($StopTimeoutSec)
    $alive = $true
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        $s = Invoke-RestMethod -Uri "$FetchHostBase/status?store=$Store"
        $alive = [bool]$s.worker_alive
        if (-not $alive) { break }
    }
    if ($alive) {
        $stopped = $false
        Log "[X] fetch_all worker 在 ${StopTimeoutSec}s 内仍未退出；为不抢 store1 profile，本轮跳过 batch"
    } else {
        Log "[OK] fetch_all worker 已停止，store1 profile 已释放"
    }
} catch {
    # 宿主没在跑 / 端口不通：视为没有 worker 占用，继续跑 batch
    Log "[!] 停 fetch_all 失败（宿主可能没在跑）：$($_.Exception.Message)；继续尝试跑 batch"
}

# ---- 2. 跑 wb_querywords batch（用完配额或跑完当天即自动退出）----
$batchExit = $null
if ($stopped) {
    Log "开始 wb_querywords.batch ..."
    # 原生 python 往 stderr 写日志时，PS5.1 在 ErrorActionPreference=Stop + 2>&1 下会抛终止性
    # NativeCommandError 打断脚本；此处临时放宽为 Continue，只以 $LASTEXITCODE 判定成败。
    $prevEAP = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Python -u -m wb_querywords.batch 2>&1 | ForEach-Object { Add-Content -Path $LogFile -Value $_ -Encoding UTF8; Write-Host $_ }
    $batchExit = $LASTEXITCODE
    $ErrorActionPreference = $prevEAP
    switch ($batchExit) {
        0 { Log "[OK] batch 正常收尾（跑完 / 日配额用尽 / 无 input）" }
        2 { Log "[!] batch 没抢到单实例锁（可能已有实例在跑），本次跳过" }
        default { Log "[X] batch 异常退出码=$batchExit" }
    }
} else {
    Log "[!] 因 fetch_all 未让出 profile，本轮未跑 batch"
}

# ---- 3. 重新拉起 fetch_all worker 继续大拉 ----
try {
    Invoke-RestMethod -Method Post -Uri "$FetchHostBase/start?store=$Store&resume=1" | Out-Null
    Log "[OK] 已请求 fetch_all /start?resume=1 续拉"
} catch {
    Log "[!] 拉起 fetch_all 失败（宿主可能没在跑，巡检会自愈）：$($_.Exception.Message)"
}

Log "===== 每日链结束（batchExit=$batchExit）====="
if ($null -ne $batchExit) { exit $batchExit }
if (-not $stopped) { exit 1 }
exit 0
