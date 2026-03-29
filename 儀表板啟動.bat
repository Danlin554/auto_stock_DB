@echo off
chcp 65001 >nul
echo ===================================
echo   盤中情緒監控系統 - 一鍵啟動
echo ===================================
echo.

:: ── Step 1：補齊近期缺漏的每日收盤資料 ──────────────────
echo [1/4] 補齊近期收盤指標資料（跳過已有的日期）...
for /f "tokens=*" %%i in ('powershell -NoProfile -Command "(Get-Date).AddDays(-14).ToString('yyyy-MM-dd')"') do set START_DATE=%%i
wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python backfill_history.py --start %START_DATE% 2>&1 | tail -5"

echo.
echo [2/4] 更新 20日新高/新低 + 均線結構...
wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python backfill_history.py --fill-rolling 2>&1 | tail -3"

echo.
:: ── Step 2：啟動盤中監控（需富邦SDK） ────────────────────
echo [3/4] 啟動盤中資料抓取 (main.py)...
start /min "盤中情緒監控 - 資料抓取" cmd /k wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python main.py"

:: ── Step 3：啟動儀表板 ───────────────────────────────────
echo [4/4] 啟動儀表板 (dashboard.py)...
start /min "盤中情緒監控 - 儀表板" cmd /k wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/streamlit run dashboard.py --server.headless true"

echo.
echo ===================================
echo   全部啟動完成！
echo ===================================
echo.
echo   收盤指標：已更新至最新
echo   盤中抓取：已最小化至工作列
echo   儀表板：http://localhost:8501
echo.
echo   此視窗將在 5 秒後自動關閉...
timeout /t 5 /nobreak >nul
