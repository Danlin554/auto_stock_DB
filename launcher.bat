@echo off
chcp 65001 >nul
echo ===================================
echo   盤中情緒監控系統
echo ===================================
echo.
echo 啟動中...
echo.
wsl bash -c "cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python main.py"
echo.
echo 程式已結束
pause
