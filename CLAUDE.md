# FB-Market 專案規則

## 專案說明
富邦證券 API 串接專案，功能：盤中每 15 秒抓取全市場快照，寫入 SQLite，並提供 Streamlit 即時監控儀表板。

## 技術環境
- Python 虛擬環境：`venv/`，執行一律用 `venv/bin/python`
- 富邦 SDK：`fubon-neo 2.2.8`（Linux 版，安裝在 venv 內）
- 資料庫：SQLite，路徑 `data/market.db`
- Dashboard：Streamlit，port 8501

## 帳號對照
- 帳號 124298 → 顯示為 **F**
- 帳號 420170 → 顯示為 **B**

## 主要檔案
- `main.py` — 主程式，盤中每 15 秒抓快照、寫 SQLite
- `dashboard.py` — Streamlit 儀表板
- `postmarket_sync.py` — 盤後同步作業
- `config/settings.json` — 監控設定（白名單股票等）
- `config/blue_chips.csv` / `tse_top20.csv` / `otc_top20.csv` — 股票清單
- `data/` — SQLite 資料庫
- `launcher.bat` / `儀表板啟動.bat` — 啟動捷徑

## 執行方式
```bash
# 盤中監控（WSL 內執行）
cd /mnt/c/Users/User/Desktop/FB-Market
venv/bin/python main.py

# 儀表板
venv/bin/python venv/bin/streamlit run dashboard.py --server.fileWatcherType poll
```

## 開發規範
- 修改任何腳本前，先說明要改什麼、為什麼，讓用戶確認
- 路徑一律使用絕對路徑（`os.path.abspath(__file__)`），避免相對路徑問題
- 不要修改 `config.py` 的內容，那是用戶的帳號資訊

## 已知問題
- 富邦 SDK 在程式結束時會出現 Segmentation fault，屬正常現象，不影響功能
