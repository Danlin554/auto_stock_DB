"""
盤中情緒監控系統 - 主程式
每 15 秒抓取全市場快照，寫入 SQLite，計算彙總數值。
執行方式：cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/python main.py
"""
import sys
import os
import time
import json
import math
import sqlite3
import logging
from datetime import datetime, timedelta

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import config
from fubon_neo.sdk import FubonSDK

# === 讀取設定檔 ===
SETTINGS_PATH = os.path.join(BASE_DIR, 'config', 'settings.json')
BLUE_CHIPS_PATH = os.path.join(BASE_DIR, 'config', 'blue_chips.csv')

with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
    SETTINGS = json.load(f)

# === 常數設定 ===
DB_PATH = os.path.join(BASE_DIR, 'data', 'market.db')
LOG_DIR = os.path.join(BASE_DIR, 'log')
FETCH_INTERVAL = 15  # 快照間隔固定 15 秒
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 0
MARKET_CLOSE_HOUR = 13
MARKET_CLOSE_MIN = 35
RELOGIN_INTERVAL = 7200      # 2 小時（秒）
RAW_KEEP_DAYS = 2
COMPUTED_KEEP_DAYS = 20
LOG_KEEP_DAYS = 30

# 從 settings.json 讀取的參數（初始值，會被 reload_settings() 更新）
VOLUME_FILTER = SETTINGS.get('volume_filter', 0)
BUCKET_TIERS = SETTINGS.get('bucket_tiers', [2.5, 5, 7.5])
LIMIT_THRESHOLD = SETTINGS.get('limit_threshold', 9.5)
INDICATOR_STRONG = SETTINGS.get('indicator_strong', 3)
INDICATOR_SUPER_STRONG = SETTINGS.get('indicator_super_strong', 7.5)
CONTINUITY_THRESHOLD = SETTINGS.get('continuity_threshold', 5)
TOP_BOTTOM_N = SETTINGS.get('top_bottom_n', 100)
ABNORMAL_THRESHOLD = 10.01  # 底層固定值，漲跌幅超過 ±10% 視為異常
BLUE_CHIPS = []


def reload_blue_chips(logger=None):
    """重新讀取權值股清單，讓 dashboard 修改可即時生效"""
    global BLUE_CHIPS
    try:
        with open(BLUE_CHIPS_PATH, 'r', encoding='utf-8') as f:
            BLUE_CHIPS = [line.strip() for line in f if line.strip()]
    except Exception as e:
        if logger:
            logger.warning(f"重新讀取 blue_chips.csv 失敗：{e}（使用上一次的清單）")


def reload_settings(logger=None):
    """重新讀取 settings.json，更新全域參數（支援 dashboard 熱更新）"""
    global VOLUME_FILTER, BUCKET_TIERS, LIMIT_THRESHOLD, INDICATOR_STRONG
    global INDICATOR_SUPER_STRONG, CONTINUITY_THRESHOLD, TOP_BOTTOM_N
    try:
        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
            s = json.load(f)
        VOLUME_FILTER = s.get('volume_filter', 0)
        BUCKET_TIERS = s.get('bucket_tiers', [2.5, 5, 7.5])
        LIMIT_THRESHOLD = s.get('limit_threshold', 9.5)
        INDICATOR_STRONG = s.get('indicator_strong', 3)
        INDICATOR_SUPER_STRONG = s.get('indicator_super_strong', 7.5)
        CONTINUITY_THRESHOLD = s.get('continuity_threshold', 5)
        TOP_BOTTOM_N = s.get('top_bottom_n', 100)
    except Exception as e:
        if logger:
            logger.warning(f"重新讀取 settings.json 失敗：{e}（使用上一次的參數）")


# ============================================================
#  工具函式
# ============================================================

def win_to_wsl(win_path):
    """Windows 路徑轉 WSL 路徑"""
    path = win_path.replace('\\', '/')
    if len(path) >= 2 and path[1] == ':':
        drive = path[0].lower()
        path = f"/mnt/{drive}" + path[2:]
    return path


def is_regular_stock(symbol):
    """判斷是否為一般股票（4碼純數字，首碼1-9）"""
    return (len(symbol) == 4 and symbol.isdigit() and symbol[0] in '123456789')


def is_trading_time(now):
    """判斷是否在交易時間內 (09:00 ~ 13:35)"""
    open_time = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN,
                            second=0, microsecond=0)
    close_time = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN,
                             second=0, microsecond=0)
    return open_time <= now < close_time


def next_fetch_mark(now):
    """計算下一個抓取時間，支援任意秒數間隔"""
    interval = max(1, int(FETCH_INTERVAL))
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    seconds_since_hour = now.minute * 60 + now.second
    next_offset = ((seconds_since_hour // interval) + 1) * interval
    return hour_start + timedelta(seconds=next_offset)


def align_fetch_time(now):
    """依目前秒數間隔，回推本次快照應對應的時間點"""
    interval = max(1, int(FETCH_INTERVAL))
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    seconds_since_hour = now.minute * 60 + now.second
    current_offset = (seconds_since_hour // interval) * interval
    return hour_start + timedelta(seconds=current_offset)


# ============================================================
#  Logging 設定
# ============================================================

def setup_logging():
    """建立 Logger，輸出到檔案與終端機"""
    os.makedirs(LOG_DIR, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(LOG_DIR, f'{today}.log')

    logger = logging.getLogger('market_monitor')
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)-7s] [%(module)-8s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ============================================================
#  資料庫
# ============================================================

def init_db():
    """建立 SQLite 資料庫與資料表"""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # /mnt/c 下的 SQLite 搭配 WAL 容易出現 shared-memory/lock 問題。
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
    -- 第一層：原始快照
    CREATE TABLE IF NOT EXISTS raw_snapshots (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_time   TEXT NOT NULL,
        market          TEXT NOT NULL,
        type            TEXT,
        symbol          TEXT NOT NULL,
        name            TEXT,
        open_price      REAL,
        high_price      REAL,
        low_price       REAL,
        close_price     REAL,
        last_price      REAL,
        change          REAL,
        change_percent  REAL,
        reference_price REAL,
        trade_volume    INTEGER,
        trade_value     REAL,
        last_updated    INTEGER,
        is_anomaly      INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_raw_time ON raw_snapshots(snapshot_time);
    CREATE INDEX IF NOT EXISTS idx_raw_symbol ON raw_snapshots(symbol);

    -- 第二層：彙總統計（每 15 秒一筆）
    CREATE TABLE IF NOT EXISTS computed_stats (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_time           TEXT NOT NULL,
        filtered_total          INTEGER,
        -- 漲跌家數
        up_count                INTEGER,
        down_count              INTEGER,
        flat_count              INTEGER,
        tse_up_count            INTEGER,
        otc_up_count            INTEGER,
        -- 量能
        total_trade_value       REAL,
        total_trade_volume      INTEGER,
        -- 核心指標
        sentiment_index         REAL,
        ad_ratio                REAL,
        volatility              REAL,
        strength_index          REAL,
        activity_rate           REAL,
        -- 分桶分布（漲）
        bucket_up_2_5           INTEGER,
        bucket_up_5             INTEGER,
        bucket_up_7_5           INTEGER,
        bucket_up_above         INTEGER,
        -- 分桶分布（跌）
        bucket_down_2_5         INTEGER,
        bucket_down_5           INTEGER,
        bucket_down_7_5         INTEGER,
        bucket_down_above       INTEGER,
        -- 強弱勢家數
        advantage_count         INTEGER,
        strong_count            INTEGER,
        super_strong_count      INTEGER,
        near_limit_up_count     INTEGER,
        disadvantage_count      INTEGER,
        weak_count              INTEGER,
        super_weak_count        INTEGER,
        near_limit_down_count   INTEGER,
        -- 強勢股延續性
        prev_strong_count       INTEGER,
        prev_strong_avg_today   REAL,
        prev_strong_positive_rate REAL,
        prev_weak_count         INTEGER,
        prev_weak_avg_today     REAL,
        prev_weak_negative_rate REAL,
        -- 百大強弱勢平均漲幅
        top_n_avg               REAL,
        bottom_n_avg            REAL,
        -- 權值股
        blue_chip_up_count      INTEGER,
        blue_chip_total         INTEGER,
        blue_chip_avg_change    REAL
    );
    CREATE INDEX IF NOT EXISTS idx_computed_time ON computed_stats(snapshot_time);

    -- 第三層：每日摘要
    CREATE TABLE IF NOT EXISTS daily_summary (
        id                          INTEGER PRIMARY KEY AUTOINCREMENT,
        date                        TEXT NOT NULL UNIQUE,
        market_open_time            TEXT,
        market_close_time           TEXT,
        total_snapshots             INTEGER,
        total_stocks                INTEGER,
        open_snapshot_time          TEXT,
        open_gap_up_count           INTEGER,
        open_gap_down_count         INTEGER,
        open_flat_count             INTEGER,
        open_valid_count            INTEGER,
        mid_30min_up_count          INTEGER,
        mid_30min_down_count        INTEGER,
        pre_close_up_count          INTEGER,
        pre_close_down_count        INTEGER,
        close_up_count              INTEGER,
        close_down_count            INTEGER,
        close_flat_count            INTEGER,
        close_tse_up                INTEGER,
        close_otc_up                INTEGER,
        max_up_count                INTEGER,
        max_up_count_time           TEXT,
        min_up_count                INTEGER,
        min_up_count_time           TEXT,
        total_amount                REAL,
        total_volume                INTEGER,
        tse_amount                  REAL,
        otc_amount                  REAL,
        prev_day_amount             REAL,
        amount_ratio                REAL,
        advance_decline_ratio       REAL,
        sentiment_label             TEXT,
        note                        TEXT
    );
    """)

    # 舊資料庫補欄位，避免既有 DB 因 schema 落後而失敗
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(computed_stats)")}
    for column_def in [
        "prev_weak_count INTEGER",
        "prev_weak_avg_today REAL",
        "prev_weak_negative_rate REAL",
    ]:
        col_name = column_def.split()[0]
        if col_name not in existing_cols:
            conn.execute(f"ALTER TABLE computed_stats ADD COLUMN {column_def}")

    conn.commit()
    return conn


def cleanup_old_data(conn, logger):
    """清除過期資料：raw 保留 2 天，computed 保留 20 天"""
    now = datetime.now()
    raw_cutoff = (now - timedelta(days=RAW_KEEP_DAYS)).strftime('%Y-%m-%d 00:00:00')
    computed_cutoff = (now - timedelta(days=COMPUTED_KEEP_DAYS)).strftime('%Y-%m-%d 00:00:00')

    cur1 = conn.execute("DELETE FROM raw_snapshots WHERE snapshot_time < ?", (raw_cutoff,))
    cur2 = conn.execute("DELETE FROM computed_stats WHERE snapshot_time < ?", (computed_cutoff,))
    conn.commit()

    if cur1.rowcount > 0 or cur2.rowcount > 0:
        logger.info(f"清理舊資料：raw {cur1.rowcount} 筆, computed {cur2.rowcount} 筆")


def cleanup_old_logs(logger):
    """清除超過 30 天的 Log 檔"""
    cutoff = datetime.now() - timedelta(days=LOG_KEEP_DAYS)
    for f in os.listdir(LOG_DIR):
        if not f.endswith('.log'):
            continue
        try:
            date_str = f.replace('.log', '')
            file_date = datetime.strptime(date_str, '%Y-%m-%d')
            if file_date < cutoff:
                os.remove(os.path.join(LOG_DIR, f))
                logger.info(f"刪除舊 Log：{f}")
        except ValueError:
            pass


# ============================================================
#  富邦 SDK 登入
# ============================================================

def sdk_login(logger):
    """登入富邦 API，回傳 sdk 物件"""
    sdk = FubonSDK()
    cert_path = win_to_wsl(config.FUBON_CERT_PATH)

    try:
        sdk.login(config.FUBON_ID, config.FUBON_PWD, cert_path, config.FUBON_CERT_PWD)
        logger.info("登入富邦 API 成功")
    except Exception as e:
        logger.error(f"登入失敗：{e}")
        raise

    sdk.init_realtime()
    logger.info("初始化即時行情完成")
    return sdk


# ============================================================
#  資料抓取
# ============================================================

def fetch_snapshots(stock_client, logger):
    """呼叫 TSE + OTC Snapshot API，回傳依 symbol 去重後的股票清單。
    注意：兩次 API 呼叫均回傳全市場資料，故去重保留第一次出現者。"""
    all_items = []

    for market in ['TSE', 'OTC']:
        try:
            data = stock_client.snapshot.quotes(market=market)
            items = data.get('data', [])
            for item in items:
                item['_market'] = market
            all_items.extend(items)
            logger.info(f"{market} 快照：{len(items)} 筆")
        except Exception as e:
            logger.error(f"{market} 快照失敗：{e}")
            raise

    # 依 symbol 去重（兩次 API 回傳相同資料集，保留第一次出現者以避免 DB 重複寫入）
    seen = set()
    deduped = []
    for item in all_items:
        sym = item.get('symbol', '')
        if sym not in seen:
            seen.add(sym)
            deduped.append(item)

    logger.info(f"去重前：{len(all_items)} 筆，去重後：{len(deduped)} 筆")
    return deduped


# ============================================================
#  異常資料檢查
# ============================================================

def check_anomaly(item):
    """檢查單筆資料是否異常，回傳 True 代表異常"""
    close = item.get('closePrice')
    high = item.get('highPrice')
    low = item.get('lowPrice')
    change_pct = item.get('changePercent')
    volume = item.get('tradeVolume')

    if close is not None and close <= 0:
        return True
    if change_pct is not None and abs(change_pct) > 30:
        return True
    if high is not None and low is not None and high < low:
        return True
    if volume is not None and volume < 0:
        return True

    return False


# ============================================================
#  過濾一般股票
# ============================================================

def filter_regular_stocks(items):
    """從 API 回傳的全部資料中篩選出一般股票（非異常、4碼數字）"""
    return [i for i in items
            if is_regular_stock(i.get('symbol', ''))
            and not check_anomaly(i)
            and i.get('changePercent') is not None]


# ============================================================
#  寫入 raw_snapshots
# ============================================================

def write_raw(conn, items, snapshot_time, logger):
    """將原始快照寫入 raw_snapshots 資料表"""
    rows = []
    anomaly_count = 0

    for item in items:
        anomaly = check_anomaly(item)
        if anomaly:
            anomaly_count += 1

        close_price = item.get('closePrice')
        change = item.get('change')
        ref_price = None
        if close_price is not None and change is not None:
            ref_price = round(close_price - change, 2)

        if anomaly:
            rows.append((
                snapshot_time, item.get('_market'), item.get('type'),
                item.get('symbol'), item.get('name'),
                None, None, None, None, None,
                None, None, None,
                None, None,
                item.get('lastUpdated'), 1
            ))
        else:
            rows.append((
                snapshot_time, item.get('_market'), item.get('type'),
                item.get('symbol'), item.get('name'),
                item.get('openPrice'), item.get('highPrice'),
                item.get('lowPrice'), close_price,
                item.get('lastPrice'),
                change, item.get('changePercent'), ref_price,
                item.get('tradeVolume'), item.get('tradeValue'),
                item.get('lastUpdated'), 0
            ))

    conn.executemany("""
        INSERT INTO raw_snapshots (
            snapshot_time, market, type, symbol, name,
            open_price, high_price, low_price, close_price, last_price,
            change, change_percent, reference_price,
            trade_volume, trade_value, last_updated, is_anomaly
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()

    if anomaly_count > 0:
        logger.warning(f"異常資料：{anomaly_count} 筆")

    return len(rows), anomaly_count


# ============================================================
#  取得昨日強勢股清單（用於延續性指標）
# ============================================================

def get_prev_strong_symbols(conn, today_str):
    """
    從 daily_stocks 取得前一交易日漲幅 > CONTINUITY_THRESHOLD 的股票代號清單。
    回傳 set of symbols，若無資料回傳空 set。
    """
    # 找前一交易日（daily_stocks 中 < today 的最大日期）
    row = conn.execute(
        "SELECT MAX(date) FROM daily_stocks WHERE date < ?", (today_str,)
    ).fetchone()

    if not row or not row[0]:
        return set()

    prev_date = row[0]

    # 計算漲幅：需要 close_price 和前一天的 close
    # daily_stocks 沒有直接的 change_percent，需要用 raw 或自己算
    # 但我們有 postmarket_sync 的 OHLCV，可以用兩天的 close 算
    prev_prev_row = conn.execute(
        "SELECT MAX(date) FROM daily_stocks WHERE date < ?", (prev_date,)
    ).fetchone()

    if not prev_prev_row or not prev_prev_row[0]:
        return set()

    prev_prev_date = prev_prev_row[0]

    # 取前一天漲幅超過門檻的股票
    rows = conn.execute("""
        SELECT d1.symbol
        FROM daily_stocks d1
        JOIN daily_stocks d0 ON d1.symbol = d0.symbol AND d0.date = ?
        WHERE d1.date = ?
          AND d0.close_price > 0
          AND ((d1.close_price - d0.close_price) / d0.close_price * 100) > ?
    """, (prev_prev_date, prev_date, CONTINUITY_THRESHOLD)).fetchall()

    return set(r[0] for r in rows)


def get_prev_weak_symbols(conn, today_str):
    """
    從 daily_stocks 取得前一交易日跌幅 < -CONTINUITY_THRESHOLD 的股票代號清單。
    回傳 set of symbols，若無資料回傳空 set。
    """
    row = conn.execute(
        "SELECT MAX(date) FROM daily_stocks WHERE date < ?", (today_str,)
    ).fetchone()

    if not row or not row[0]:
        return set()

    prev_date = row[0]

    prev_prev_row = conn.execute(
        "SELECT MAX(date) FROM daily_stocks WHERE date < ?", (prev_date,)
    ).fetchone()

    if not prev_prev_row or not prev_prev_row[0]:
        return set()

    prev_prev_date = prev_prev_row[0]

    rows = conn.execute("""
        SELECT d1.symbol
        FROM daily_stocks d1
        JOIN daily_stocks d0 ON d1.symbol = d0.symbol AND d0.date = ?
        WHERE d1.date = ?
          AND d0.close_price > 0
          AND ((d1.close_price - d0.close_price) / d0.close_price * 100) < ?
    """, (prev_prev_date, prev_date, -CONTINUITY_THRESHOLD)).fetchall()

    return set(r[0] for r in rows)


# ============================================================
#  計算 computed_stats（完整版）
# ============================================================

def compute_stats(conn, items, snapshot_time, logger, prev_strong_symbols, prev_weak_symbols):
    """對全市場資料計算所有指標，寫入 computed_stats"""
    # 先篩一般股票（不含成交量濾網），用於 raw 寫入等
    all_regular = filter_regular_stocks(items)

    # 加上成交量 >= 300 張濾網，用於指標計算
    valid = [i for i in all_regular if (i.get('tradeVolume') or 0) >= VOLUME_FILTER]
    filtered_total = len(valid)

    if filtered_total == 0:
        logger.warning("無有效股票資料，跳過計算")
        return

    # --- 基礎漲跌家數 ---
    pcts = [i['changePercent'] for i in valid]
    up = sum(1 for p in pcts if p > 0)
    down = sum(1 for p in pcts if p < 0)
    flat = sum(1 for p in pcts if p == 0)
    tse_up = sum(1 for i in valid if i['_market'] == 'TSE' and i['changePercent'] > 0)
    otc_up = sum(1 for i in valid if i['_market'] == 'OTC' and i['changePercent'] > 0)

    # --- 量能 ---
    total_value = sum(i.get('tradeValue', 0) or 0 for i in valid)
    total_volume = sum(i.get('tradeVolume', 0) or 0 for i in valid)

    # --- 核心指標 ---
    # 情緒指數：(上漲-下跌)/(上漲+下跌) × 100
    sentiment_index = round((up - down) / (up + down) * 100, 2) if (up + down) > 0 else 0
    ad_ratio = round(up / down, 4) if down > 0 else None

    # 強弱指數：≥3% 和 ≤-3% 股票漲跌幅的平均值
    extreme_pcts = [p for p in pcts if p >= INDICATOR_STRONG or p <= -INDICATOR_STRONG]
    strength_index = round(sum(extreme_pcts) / len(extreme_pcts), 4) if extreme_pcts else 0

    # 波動度：全市場漲跌幅的標準差
    mean_pct = sum(pcts) / len(pcts)
    variance = sum((p - mean_pct) ** 2 for p in pcts) / len(pcts)
    volatility = round(math.sqrt(variance), 4)

    # 活躍度：(強勢股+弱勢股) / 總家數 × 100
    strong_count = sum(1 for p in pcts if p >= INDICATOR_STRONG)
    weak_count = sum(1 for p in pcts if p <= -INDICATOR_STRONG)
    activity_rate = round((strong_count + weak_count) / filtered_total * 100, 2) if filtered_total > 0 else 0

    # --- 分桶分布 ---
    # 漲方：0~2.5%, 2.5~5%, 5~7.5%, >7.5%
    t1, t2, t3 = BUCKET_TIERS
    bucket_up_2_5 = sum(1 for p in pcts if 0 < p <= t1)
    bucket_up_5 = sum(1 for p in pcts if t1 < p <= t2)
    bucket_up_7_5 = sum(1 for p in pcts if t2 < p <= t3)
    bucket_up_above = sum(1 for p in pcts if p > t3)
    # 跌方：0~-2.5%, -2.5~-5%, -5~-7.5%, <-7.5%
    bucket_down_2_5 = sum(1 for p in pcts if -t1 <= p < 0)
    bucket_down_5 = sum(1 for p in pcts if -t2 <= p < -t1)
    bucket_down_7_5 = sum(1 for p in pcts if -t3 <= p < -t2)
    bucket_down_above = sum(1 for p in pcts if p < -t3)

    # --- 強弱勢家數（用 >= / <= 與範本一致）---
    advantage_count = up
    # strong_count 已在活躍度計算時算過
    super_strong_count = sum(1 for p in pcts if p >= INDICATOR_SUPER_STRONG)
    near_limit_up_count = sum(1 for p in pcts if p >= LIMIT_THRESHOLD)

    disadvantage_count = down
    # weak_count 已在活躍度計算時算過
    super_weak_count = sum(1 for p in pcts if p <= -INDICATOR_SUPER_STRONG)
    near_limit_down_count = sum(1 for p in pcts if p <= -LIMIT_THRESHOLD)

    # --- 強勢股延續性 ---
    prev_strong_count = len(prev_strong_symbols) if prev_strong_symbols else None
    prev_strong_avg_today = None
    prev_strong_positive_rate = None
    prev_weak_count = len(prev_weak_symbols) if prev_weak_symbols else None
    prev_weak_avg_today = None
    prev_weak_negative_rate = None

    if prev_strong_symbols:
        # 找出昨日強勢股在今天的表現
        today_pcts = []
        for i in valid:
            if i.get('symbol') in prev_strong_symbols:
                today_pcts.append(i['changePercent'])

        if today_pcts:
            prev_strong_avg_today = round(sum(today_pcts) / len(today_pcts), 4)
            positive = sum(1 for p in today_pcts if p > 0)
            prev_strong_positive_rate = round(positive / len(today_pcts) * 100, 2)

    if prev_weak_symbols:
        # 找出昨日弱勢股在今天的表現
        today_pcts = []
        for i in valid:
            if i.get('symbol') in prev_weak_symbols:
                today_pcts.append(i['changePercent'])

        if today_pcts:
            prev_weak_avg_today = round(sum(today_pcts) / len(today_pcts), 4)
            negative = sum(1 for p in today_pcts if p < 0)
            prev_weak_negative_rate = round(negative / len(today_pcts) * 100, 2)

    # --- 百大強弱勢平均漲幅（排除漲跌幅 > 10.01%）---
    normal_pcts = [p for p in pcts if abs(p) <= ABNORMAL_THRESHOLD]
    normal_pcts_sorted = sorted(normal_pcts, reverse=True)

    n = min(TOP_BOTTOM_N, len(normal_pcts_sorted))
    top_n_avg = round(sum(normal_pcts_sorted[:n]) / n, 4) if n > 0 else None
    bottom_n_avg = round(sum(normal_pcts_sorted[-n:]) / n, 4) if n > 0 else None

    # --- 權值股 ---
    blue_items = [i for i in valid if i.get('symbol') in BLUE_CHIPS]
    blue_chip_total = len(blue_items)
    blue_chip_up_count = sum(1 for i in blue_items if i['changePercent'] > 0)
    blue_pcts = [i['changePercent'] for i in blue_items]
    blue_chip_avg_change = round(sum(blue_pcts) / len(blue_pcts), 4) if blue_pcts else None

    # --- 寫入 ---
    conn.execute("""
        INSERT INTO computed_stats (
            snapshot_time, filtered_total,
            up_count, down_count, flat_count, tse_up_count, otc_up_count,
            total_trade_value, total_trade_volume,
            sentiment_index, ad_ratio, volatility, strength_index, activity_rate,
            bucket_up_2_5, bucket_up_5, bucket_up_7_5, bucket_up_above,
            bucket_down_2_5, bucket_down_5, bucket_down_7_5, bucket_down_above,
            advantage_count, strong_count, super_strong_count, near_limit_up_count,
            disadvantage_count, weak_count, super_weak_count, near_limit_down_count,
            prev_strong_count, prev_strong_avg_today, prev_strong_positive_rate,
            prev_weak_count, prev_weak_avg_today, prev_weak_negative_rate,
            top_n_avg, bottom_n_avg,
            blue_chip_up_count, blue_chip_total, blue_chip_avg_change
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot_time, filtered_total,
        up, down, flat, tse_up, otc_up,
        total_value, total_volume,
        sentiment_index, ad_ratio, volatility, strength_index, activity_rate,
        bucket_up_2_5, bucket_up_5, bucket_up_7_5, bucket_up_above,
        bucket_down_2_5, bucket_down_5, bucket_down_7_5, bucket_down_above,
        advantage_count, strong_count, super_strong_count, near_limit_up_count,
        disadvantage_count, weak_count, super_weak_count, near_limit_down_count,
        prev_strong_count, prev_strong_avg_today, prev_strong_positive_rate,
        prev_weak_count, prev_weak_avg_today, prev_weak_negative_rate,
        top_n_avg, bottom_n_avg,
        blue_chip_up_count, blue_chip_total, blue_chip_avg_change,
    ))
    conn.commit()

    logger.info(
        f"計算完成：有效 {filtered_total}  "
        f"上漲 {up} 下跌 {down} 持平 {flat}  "
        f"情緒 {sentiment_index}%  強勢 {strong_count}  弱勢 {weak_count}  "
        f"波動 {volatility}"
    )


# ============================================================
#  API 失敗處理（含重試與 Token 過期偵測）
# ============================================================

def fetch_with_retry(stock_client, sdk_state, logger):
    """
    抓取快照，失敗時自動重試或重新登入。
    sdk_state: dict，包含 sdk, stock_client, last_login_time（可變物件）
    """
    try:
        return fetch_snapshots(stock_client, logger)
    except Exception as e:
        error_msg = str(e).lower()

        if 'authentication' in error_msg or 'token' in error_msg or 'unauthenticated' in error_msg:
            logger.error(f"Token 可能過期：{e}，嘗試重新登入")
            try:
                sdk = sdk_login(logger)
                sdk_state['sdk'] = sdk
                sdk_state['stock_client'] = sdk.marketdata.rest_client.stock
                sdk_state['last_login_time'] = time.time()
                return fetch_snapshots(sdk_state['stock_client'], logger)
            except Exception as e2:
                logger.error(f"重新登入後仍失敗：{e2}")
                return None

        for attempt in range(1, 4):
            logger.error(f"API 失敗：{e}，立即重試({attempt}/3)")
            try:
                items = fetch_snapshots(stock_client, logger)
                logger.info("重試成功")
                return items
            except Exception as retry_e:
                e = retry_e

        logger.error("重試 3 次均失敗，跳過本次快照")
        return None


# ============================================================
#  主迴圈
# ============================================================

def main():
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("[SYSTEM ] 盤中情緒監控系統啟動")
    logger.info("=" * 50)

    # 週末檢查
    now = datetime.now()
    if now.weekday() >= 5:
        logger.info("[SYSTEM ] 今天是週末，程式不執行")
        return

    # 初始化資料庫
    conn = init_db()
    logger.info("資料庫初始化完成")
    reload_settings(logger)
    reload_blue_chips(logger)

    # 啟動時清理舊資料與舊 Log
    cleanup_old_data(conn, logger)
    cleanup_old_logs(logger)

    # 預先載入昨日強勢股清單（用於延續性指標）
    today_str = now.strftime('%Y-%m-%d')
    prev_strong_symbols = get_prev_strong_symbols(conn, today_str)
    prev_weak_symbols = get_prev_weak_symbols(conn, today_str)
    if prev_strong_symbols:
        logger.info(f"昨日強勢股（>{CONTINUITY_THRESHOLD}%）：{len(prev_strong_symbols)} 檔")
    else:
        logger.info("無昨日強勢股資料（可能尚未同步盤後資料）")
    if prev_weak_symbols:
        logger.info(f"昨日弱勢股（<-{CONTINUITY_THRESHOLD}%）：{len(prev_weak_symbols)} 檔")
    else:
        logger.info("無昨日弱勢股資料（可能尚未同步盤後資料）")

    # 登入富邦 API
    sdk = sdk_login(logger)

    sdk_state = {
        'sdk': sdk,
        'stock_client': sdk.marketdata.rest_client.stock,
        'last_login_time': time.time(),
    }

    snapshot_count = 0

    # === 主迴圈 ===
    while True:
        now = datetime.now()
        close_time = now.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN,
                                 second=0, microsecond=0)
        open_time = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN,
                                second=0, microsecond=0)

        if now >= close_time:
            logger.info(f"[SYSTEM ] 已過 {MARKET_CLOSE_HOUR}:{MARKET_CLOSE_MIN:02d}，程式結束")
            break

        if now < open_time:
            wait = (open_time - now).total_seconds()
            logger.info(f"[SYSTEM ] 等待開盤，還有 {wait:.0f} 秒 ({open_time.strftime('%H:%M:%S')})")
            time.sleep(min(wait, 60))
            continue

        # dashboard 更新後，下一輪排程就套用新的秒數與權值股清單
        reload_settings(logger)
        reload_blue_chips(logger)

        target = next_fetch_mark(now)
        wait = (target - datetime.now()).total_seconds()
        if wait > 0:
            time.sleep(wait)

        now = datetime.now()
        if now >= close_time:
            logger.info(f"[SYSTEM ] 已過 {MARKET_CLOSE_HOUR}:{MARKET_CLOSE_MIN:02d}，程式結束")
            break

        snapshot_time = align_fetch_time(now).strftime('%Y-%m-%d %H:%M:%S')

        t_start = time.time()

        # --- 預防性重新登入（每 2 小時）---
        if time.time() - sdk_state['last_login_time'] > RELOGIN_INTERVAL:
            logger.info("預防性重新登入（每 2 小時）...")
            try:
                sdk = sdk_login(logger)
                sdk_state['sdk'] = sdk
                sdk_state['stock_client'] = sdk.marketdata.rest_client.stock
                sdk_state['last_login_time'] = time.time()
            except Exception as e:
                logger.error(f"預防性重新登入失敗：{e}（繼續使用舊 Token）")

        # --- 抓取快照 ---
        items = fetch_with_retry(sdk_state['stock_client'], sdk_state, logger)

        if not items:
            logger.warning("本次快照無資料，跳過")
            continue

        # --- 寫入 raw_snapshots ---
        try:
            count, anomaly = write_raw(conn, items, snapshot_time, logger)
        except Exception as e:
            logger.error(f"raw_snapshots 寫入失敗：{e}")
            continue

        # --- 計算 computed_stats ---
        try:
            compute_stats(conn, items, snapshot_time, logger, prev_strong_symbols, prev_weak_symbols)
        except Exception as e:
            logger.error(f"computed_stats 計算失敗：{e}")

        snapshot_count += 1
        elapsed = time.time() - t_start
        logger.info(f"快照 #{snapshot_count} 完成：{count} 筆，耗時 {elapsed:.2f} 秒")

    # === 收盤收尾 ===
    conn.close()
    logger.info(f"[SYSTEM ] 今日共完成 {snapshot_count} 次快照")
    logger.info("[SYSTEM ] 系統正常關閉")


if __name__ == "__main__":
    main()
