#Requires -Version 5.1
<#
.SYNOPSIS
    wb_querywords 串行链（每小时巡检）：有新 input 或未完成进度才跑搜索词 batch，跑完再交回 fetch_all。

.DESCRIPTION
    背景：推荐接口每店每日仅 100 次硬配额（按店计），且 wb_querywords 与 fetch_all
    会共用同一批店铺的 Chrome profile（同目录不能双开），二者不能并行，必须串行。
    故不用 wb_querywords 常驻宿主，改由本脚本高频触发、但先做轻量预检，只有确有
    待办才真正进链，串三步：
        0) 预检 python -m wb_querywords.batch --check-due（不起浏览器、不碰 fetch_all）：
           无新日期且无未完成进度 → 直接 exit 0，本小时不打扰 fetch_all；
        1) 优雅停回退链各店的 fetch_all worker（逐个 POST /stop），等它们退出释放各自 profile；
        2) 跑 python -m wb_querywords.batch（one_shot）：多店接力——当前店跑完当天 input 或
           命中 limitExceeded 就自动切回退链下一个店从同一个词继续；全部店配额用尽或跑完当天
           即自动退出进程（退出前跑完即上传当前日期，并写 day_done 台账）；
        3) 重新拉起各店 fetch_all worker（逐个 POST /start?resume=1）继续几小时的大拉。

    为什么每小时：新 input 到达后≤ 1 小时内自动开跑；意外重启后≤ 1 小时内自动从断点续跑完。
    但仅在巡检窗口 [$WinStart, $WinEnd]（默认 13:30–19:30，下午一点半到晚上七点半）内每小时一轮，
    窗口外不进链（把早晚时段完整留给 fetch_all）。
    靠预检 + day_done 台账保证幂等（“跑过即丢弃”）：跑过的文件（done 或 quota）一律不再回来，
    只有 OSS 出现更晚日期的新文件才会再跑；仅当天崩在中途（无 day_done 且是今日文件）才原地恢复，
    往日没跑完且今天无新文件则不补（配额宁可闲着）。

    部署前提：
        - WB_FetchHost（8080 常驻宿主，AUTO_START + 巡检自愈）保持启用；
        - 不要启用 WB_QuerywordsHost（常驻宿主），否则会与 batch 抢同一 run.lock；
        - 本脚本注册为计划任务 WB_QuerywordsDailyChain，仅巡检窗口内每小时一轮：
          `schtasks /SC HOURLY /MO 1 /ST 13:30 /ET 19:30`（而非旧的每日 15:00）；脚本内亦有窗口门兜底。

    A / B 机通用：脚本只调模块，两类型差异（RECOM_TYPES）在各分支的 config.py 内，
    本脚本无需区分。请按本机实际项目根修改 $ProjectRoot。

.NOTES
    退出码：0 正常收尾（预检无待办 / 跑完 / 配额用尽 / 无 input）；1 fetch_all 未及时让出 profile。
#>

$ErrorActionPreference = "Stop"

# ==== 按本机实际情况修改 ====
$ProjectRoot    = "C:\Users\yuanbo\PycharmProjects\WelcomeScreen"  # 项目根目录
$Python         = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$FetchHostBase  = "http://127.0.0.1:8080"   # fetch_all 常驻宿主（根 app.py）
# B 机单店：batch 只起 store1 一个店的浏览器（独立 profile），故只需停下 store1 的
# fetch_all worker 释放 profile，跑完 batch 再拉起。需与 wb_querywords/config.py 的 STORE_IDS 对齐。
$Stores         = @("store1")                # B 机单店，与 config.STORE_IDS 一致
$StopTimeoutSec = 180                       # 等 fetch_all worker 优雅退出的上限（秒）
# 巡检窗口：仅在该时段内每小时进链（含端点），窗口外直接退出、连预检都不做，绝不碰 fetch_all。
# 与计划任务的 /ST /ET 双保险：任务层窗口外不 spawn，脚本层再兜一道（防有人把任务重登记成全时段）。
$WinStart       = "13:30"                    # 下午一点半
$WinEnd         = "19:30"                    # 晚上七点半
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

# ---- -1. 窗口判定：不在 [$WinStart, $WinEnd] 内直接退出（不 spawn python、不预检、不碰 fetch_all）----
$now  = Get-Date
$day0 = $now.Date   # 今日 00:00（用 [datetime].Date，PS5.1 兼容；Get-Date -Hour 是 PS6+ 才有）
$ws   = $WinStart.Split(':'); $we = $WinEnd.Split(':')
$from = $day0.AddMinutes([int]$ws[0] * 60 + [int]$ws[1])
$to   = $day0.AddMinutes([int]$we[0] * 60 + [int]$we[1])
if ($now -lt $from -or $now -gt $to) {
    Log "[SKIP] 当前 $($now.ToString('HH:mm')) 不在巡检窗口 [$WinStart, $WinEnd] 内，退出（不预检、不碰 fetch_all）"
    exit 0
}

Log "===== 巡检开始（stores=$($Stores -join ',')，窗口 [$WinStart, $WinEnd]）====="

# ---- 0. 轻量预检：无待办则不进链（不起浏览器、不碰 fetch_all）----
# 原生 python 往 stderr 写日志时，PS5.1 在 ErrorActionPreference=Stop + 2>&1 下会抛终止性
# NativeCommandError 打断脚本；此处临时放宽为 Continue，只看输出里的 WQ_CHECK_DUE 标记。
$prevEAP = $ErrorActionPreference
$ErrorActionPreference = "Continue"
$checkOut = & $Python -u -m wb_querywords.batch --check-due 2>&1
$checkExit = $LASTEXITCODE
$ErrorActionPreference = $prevEAP
$checkText = ($checkOut | Out-String)
foreach ($ln in ($checkText -split "`r?`n")) {
    if ($ln -ne "") { Log "[CHECK] $ln" }
}
if ($checkExit -ne 0 -or $checkText -notmatch "WQ_CHECK_DUE=true") {
    Log "[SKIP] 预检无待办，本轮不进链（fetch_all 不受影响），退出"
    exit 0
}
Log "[DUE] 预检有待办，进入串行链：停 fetch_all -> 跑 batch -> 起 fetch_all"

# ---- 1. 优雅停回退链各店的 fetch_all worker，释放各自 profile ----
$stopped = $true
try {
    foreach ($st in $Stores) {
        Invoke-RestMethod -Method Post -Uri "$FetchHostBase/stop?store=$st" | Out-Null
        Log "已请求 fetch_all /stop store=$st"
    }
    Log "等待所有 worker 退出（最多 ${StopTimeoutSec}s）..."
    $deadline = (Get-Date).AddSeconds($StopTimeoutSec)
    $stillAlive = $true
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 5
        $anyAlive = $false
        foreach ($st in $Stores) {
            $s = Invoke-RestMethod -Uri "$FetchHostBase/status?store=$st"
            if ([bool]$s.worker_alive) { $anyAlive = $true }
        }
        if (-not $anyAlive) { $stillAlive = $false; break }
    }
    if ($stillAlive) {
        $stopped = $false
        Log "[X] fetch_all worker 在 ${StopTimeoutSec}s 内仍未全部退出；为不抢 profile，本轮跳过 batch"
    } else {
        Log "[OK] fetch_all worker 已全部停止，profile 已释放"
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

# ---- 3. 重新拉起回退链各店的 fetch_all worker 继续大拉 ----
try {
    foreach ($st in $Stores) {
        Invoke-RestMethod -Method Post -Uri "$FetchHostBase/start?store=$st&resume=1" | Out-Null
        Log "[OK] 已请求 fetch_all /start?resume=1 store=$st 续拉"
    }
} catch {
    Log "[!] 拉起 fetch_all 失败（宿主可能没在跑，巡检会自愈）：$($_.Exception.Message)"
}

Log "===== 巡检链结束（batchExit=$batchExit）====="
if ($null -ne $batchExit) { exit $batchExit }
if (-not $stopped) { exit 1 }
exit 0
