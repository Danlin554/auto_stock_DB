@echo off
chcp 65001 >nul
echo ===================================
echo   盤中情緒監控系統 - 一鍵啟動
echo ===================================
echo.

echo [1/2] 啟動資料抓取 (main.py)...
start "盤中情緒監控 - 資料抓取" cmd /k wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python main.py"

echo [2/2] 啟動儀表板 (dashboard.py)...
start "盤中情緒監控 - 儀表板" cmd /k wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/streamlit run dashboard.py --server.headless true"

echo.
echo ===================================
echo   全部啟動完成！
echo ===================================
echo.
echo   資料抓取：背景視窗執行中（13:35 自動結束）
echo   儀表板：http://localhost:8501
echo.
echo   提醒：每個交易日早上都要重新啟動一次
echo   （因為 main.py 在 13:35 收盤後會自動結束）
echo.
pause
