@echo off
chcp 65001 >nul
echo ===================================
echo   盤後資料同步
echo ===================================
echo.
echo 啟動中...
echo.
wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python postmarket_sync.py"
echo.
echo 同步完成
pause
