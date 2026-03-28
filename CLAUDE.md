# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 專案說明

富邦證券 API 串接專案。盤中每 15 秒抓取全市場快照，寫入 PostgreSQL，並提供 Streamlit 即時監控儀表板。已部署到 Zeabur 雲端（日本機房）。

## 技術環境

- Python 虛擬環境：`venv/`，執行一律用 `venv/bin/python`
- 富邦 SDK：`fubon_neo`（**不在 requirements.txt**，需手動安裝，Linux 版）
- 資料庫：**PostgreSQL**（已從 SQLite 遷移），透過 `DATABASE_URL` 環境變數連線
- 雲端部署：Zeabur（GitHub `master` 分支自動部署）

## 執行方式

```bash
# 本機執行前必須先設定
export DATABASE_URL='postgresql://...'

# 盤中監控（需富邦 SDK + config.py 或環境變數）
venv/bin/python main.py

# 儀表板（本機測試）
venv/bin/python -m streamlit run dashboard.py --server.fileWatcherType poll

# 盤後資料同步（手動補抓當日收盤資料）
venv/bin/python postmarket_sync.py [YYYY-MM-DD]

# 歷史資料回填（補算 daily_closing）
venv/bin/python backfill_history.py

# SQLite → PostgreSQL 資料遷移（只需執行一次）
venv/bin/python migrate_data.py
```

## 帳號對照

- 帳號 124298 → 顯示為 **F**
- 帳號 420170 → 顯示為 **B**

## 架構

```
main.py（盤中，每 15 秒）
    │  FubonSDK.login() → 抓全市場快照
    │  write_raw()      → raw_snapshots
    │  compute_stats()  → computed_stats
    └  save_daily_closing() → daily_closing

postmarket_sync.py（盤後）
    └  fetch TSE/OTC API → daily_stocks

dashboard.py（Streamlit）
    └  讀 computed_stats / raw_snapshots / daily_closing / daily_stocks

pages/
    ├  0_⚙_設定.py          → 儀表板參數設定（寫 config/settings.json）
    └  1_📈_歷史收盤指標.py  → 讀 daily_closing 的長期趨勢圖
```

## 資料庫（lib/db.py）

所有 DB 操作透過 `lib/db.py` 的統一介面，禁止直接使用 `psycopg2`：

```python
from lib.db import get_connection, init_all_tables, ensure_columns
from lib.db import read_sql, qone, qall, qexec, qmany
```

SQL 佔位符一律用 `%s`（psycopg2 pyformat），具名參數用 `%(name)s`。

**`offset` 是 PostgreSQL 保留字**：所有 SQL 中凡涉及 `daily_stocks.offset` 欄位，必須寫成 `"offset"`（加雙引號）。

## 資料表結構

| 資料表 | 用途 | 保留期限 |
|--------|------|---------|
| `raw_snapshots` | 每 15 秒全市場快照（原始） | 2 天 |
| `computed_stats` | 每 15 秒彙總統計 | 20 天 |
| `daily_closing` | 每日收盤指標（永久） | 6 年 |
| `daily_stocks` | 每日個股 OHLCV + 法人 + 融資 | 依設定 |
| `daily_summary` | 每日摘要 | — |

## 憑證與環境變數

| 變數 | 本機來源 | 雲端來源 |
|------|---------|---------|
| `DATABASE_URL` | 手動設定 | Zeabur 自動注入 |
| `FUBON_ID` | `config.py`（優先用環境變數） | Zeabur 環境變數 |
| `FUBON_PWD` | 同上 | 同上 |
| `FUBON_CERT_PATH` | 本機憑證路徑 | 不用（用 `FUBON_CERT_B64`） |
| `FUBON_CERT_B64` | — | PFX 憑證的 base64 編碼 |
| `FUBON_CERT_PWD` | `config.py` | Zeabur 環境變數 |

`config.py` 存在時自動載入；不存在（雲端）時改從環境變數讀取。**不要修改 `config.py`**。

## 開發規範

- 修改任何腳本前，先說明要改什麼、為什麼，讓用戶確認
- 路徑一律使用絕對路徑（`os.path.abspath(__file__)`）
- 富邦 SDK 在程式正常結束時會出現 Segmentation fault（exit code 139），屬正常現象

## 已知問題

- `fubon_neo` 無法透過 pip 安裝到 Zeabur，目前 `main.py`（盤中收集）仍在本機執行，連雲端 PostgreSQL
- Zeabur 部署的是 `dashboard.py`，使用 `master` 分支
