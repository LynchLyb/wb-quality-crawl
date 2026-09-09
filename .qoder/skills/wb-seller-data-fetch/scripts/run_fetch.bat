@echo off
rem ============================================================
rem  WB 商品数据定时拉取胶水层（模板）
rem  部署：复制本文件到项目根目录，然后注册定时任务：
rem  schtasks /Create /TN "WB_FetchAll" /TR "<项目路径>\run_fetch.bat" /SC DAILY /ST 03:00 /F
rem  注意：任务属性须为"只在用户登录时运行"（Chrome GUI 无法在会话 0 启动）
rem ============================================================
setlocal
chcp 65001 >nul
cd /d "%~dp0"
set "PY=.venv\Scripts\python.exe"
set "PYTHONIOENCODING=utf-8"

rem 日志按日期命名（用 PowerShell 取日期，避免 %date% 的区域格式差异）
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set "DAY=%%i"
if not exist logs mkdir logs
set "LOG=logs\fetch_%DAY%.log"
set "LOCK=%TEMP%\wb_fetch_all.lock"

rem 防重叠：锁文件存在说明上一轮还在跑（两个实例会抢同一个 chrome_profile）
if exist "%LOCK%" (
    echo [%DATE% %TIME%] 锁文件存在，可能已有实例在运行，跳过本次 >> "%LOG%"
    exit /b 0
)
echo locked> "%LOCK%"

set "RETRY=0"
set "ARGS="

:run
set /a RETRY+=1
echo [%DATE% %TIME%] 第 %RETRY% 轮开始 (参数: %ARGS%) >> "%LOG%"
%PY% -u fetch_all.py %ARGS% >> "%LOG%" 2>&1
set "ARGS=--resume"

rem 检查最近运行目录的 state.json：finished=true 则退出码 0
%PY% -c "import json,glob,os,sys;ds=sorted(glob.glob('tableListv6_2*'));p=os.path.join(ds[-1],'state.json') if ds else '';st=json.load(open(p,encoding='utf-8')) if p and os.path.exists(p) else {};sys.exit(0 if st.get('finished') else 1)" 2>> "%LOG%"

if errorlevel 1 (
    if %RETRY% lss 4 (
        echo [%DATE% %TIME%] 未完成，60 秒后自动续传 >> "%LOG%"
        timeout /t 60 /nobreak >nul
        goto run
    )
    echo [%DATE% %TIME%] 已重试 3 轮仍未完成，需人工检查（登录态失效/持续限流）>> "%LOG%"
)

del "%LOCK%" >nul 2>&1
echo [%DATE% %TIME%] 本次执行结束 >> "%LOG%"
endlocal
