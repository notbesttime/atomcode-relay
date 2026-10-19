@echo off
setlocal enabledelayedexpansion
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================
echo   AtomCode 反代
echo ============================================
echo.
echo   Base URL : http://127.0.0.1:8787/v1
echo   API Key  : sk-proxy   (随便填)
echo.
echo   关闭本窗口 或 Ctrl+C 即停止代理
echo ============================================
echo.

rem 端口已有实例就先清掉，避免起不来
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8787" ^| findstr "LISTENING"') do (
  echo 端口 8787 被 PID=%%a 占用，清理...
  taskkill /PID %%a /F >nul 2>&1
)

python atomcode2api.py --port 8787

echo.
echo 代理已退出。
pause
