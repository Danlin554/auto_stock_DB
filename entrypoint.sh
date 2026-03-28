#!/bin/bash
# Zeabur 入口腳本：根據 SERVICE_MODE 環境變數決定啟動哪個服務
# - dashboard（預設）：Streamlit 儀表板
# - scheduler：APScheduler 排程服務（main.py + postmarket_sync.py）

set -e

MODE="${SERVICE_MODE:-dashboard}"
echo "[entrypoint] SERVICE_MODE=$MODE"

case "$MODE" in
    scheduler)
        echo "[entrypoint] 啟動排程服務..."
        exec python scheduler.py
        ;;
    *)
        echo "[entrypoint] 啟動儀表板..."
        exec python -m streamlit run dashboard.py \
            --server.port="${PORT:-8501}" \
            --server.address=0.0.0.0 \
            --server.fileWatcherType=none \
            --server.headless=true
        ;;
esac
