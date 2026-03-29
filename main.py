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
import traceback
import logging
from datetime import datetime, timedelta

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from lib.db import get_connection, init_all_tables, ensure_columns, read_sql, qone, qall, qexec, qmany

try:
    import config as _cfg
    _FUBON_ID        = os.environ.get('FUBON_ID')        or _cfg.FUBON_ID
    _FUBON_PWD       = os.environ.get('FUBON_PWD')       or _cfg.FUBON_PWD
    _FUBON_CERT_PATH = os.environ.get('FUBON_CERT_PATH') or _cfg.FUBON_CERT_PATH
    _FUBON_CERT_PWD  = os.environ.get('FUBON_CERT_PWD')  or _cfg.FUBON_CERT_PWD
except ImportError:
    _FUBON_ID        = os.environ['FUBON_ID']
    _FUBON_PWD       = os.environ['FUBON_PWD']
    _FUBON_CERT_PATH = os.environ.get('FUBON_CERT_PATH', '')
    _FUBON_CERT_PWD  = os.environ['FUBON_CERT_PWD']

from fubon_neo.sdk import FubonSDK
from postmarket_sync import sync_date as sync_daily

# === 讀取設定檔 ===
SETTINGS_PATH = os.path.join(BASE_DIR, 'config', 'settings.json')
BLUE_CHIPS_PATH = os.path.join(BASE_DIR, 'config', 'blue_chips.csv')

try:
    with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
        SETTINGS = json.load(f)
except Exception:
    SETTINGS = {}

# === 常數設定 ===
LOG_DIR = os.path.join(BASE_DIR, 'log')
FETCH_INTERVAL = 15  # 快照間隔固定 15 秒
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 0
MARKET_CLOSE_HOUR = 13
MARKET_CLOSE_MIN = 35
RELOGIN_INTERVAL = 7200      # 2 小時（秒）
RAW_KEEP_DAYS = 2
COMPUTED_KEEP_DAYS = 20
DAILY_CLOSING_KEEP_DAYS = 2200   # 約 6 年
LOG_KEEP_DAYS = 30


def normalize_bucket_tiers(tiers):
    """確保 bucket_tiers 為 3 個正數，失敗時回退預設值"""
    default = [2.5, 5.0, 7.5]
    if not isinstance(tiers, (list, tuple)):
        return default

    parsed = []
    for value in tiers:
        try:
            fval = float(value)
        except (TypeError, ValueError):
            continue
        if fval > 0:
            parsed.append(fval)

    if len(parsed) < 3:
        return default
    return sorted(parsed)[:3]

# 從 settings.json 讀取的參數（初始值，會被 reload_settings() 更新）
VOLUME_FILTER = SETTINGS.get('volume_filter', 0)
BUCKET_TIERS = normalize_bucket_tiers(SETTINGS.get('bucket_tiers', [2.5, 5, 7.5]))
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
        BUCKET_TIERS = normalize_bucket_tiers(s.get('bucket_tiers', [2.5, 5, 7.5]))
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
    """建立 PostgreSQL 連線並初始化資料表"""
    conn = get_connection()
    init_all_tables(conn)

    # 補欄位（舊資料庫相容）
    ensure_columns(conn, 'computed_stats', [
        "prev_weak_count INTEGER",
        "prev_weak_avg_today REAL",
        "prev_weak_negative_rate REAL",
        "red_k_count INTEGER",
        "black_k_count INTEGER",
        "flat_k_count INTEGER",
        "above_5pct_count INTEGER",
    ])
    ensure_columns(conn, 'daily_closing', [
        "above_5pct_count INTEGER",
        "new_high_20d_count INTEGER",
        "new_low_20d_count INTEGER",
        "above_5ma_count INTEGER",
        "above_20ma_count INTEGER",
        "above_60ma_count INTEGER",
        "above_5ma_pct REAL",
        "above_20ma_pct REAL",
        "above_60ma_pct REAL",
        "margin_maintenance_rate REAL",
    ])
    return conn


def cleanup_old_data(conn, logger):
    """清除過期資料：raw 保留 2 天，computed 保留 20 天，daily_closing 保留 2200 天。
    raw_snapshots 資料量大（每日 ~200 萬筆），採分批刪除（每批 5000 筆）避免長時間鎖定資料庫。
    """
    _BATCH_SIZE = 5000
    now = datetime.now()
    raw_cutoff = (now - timedelta(days=RAW_KEEP_DAYS)).strftime('%Y-%m-%d 00:00:00')
    computed_cutoff = (now - timedelta(days=COMPUTED_KEEP_DAYS)).strftime('%Y-%m-%d 00:00:00')
    closing_cutoff = (now - timedelta(days=DAILY_CLOSING_KEEP_DAYS)).strftime('%Y-%m-%d')

    # computed_stats 和 daily_closing 資料量小，直接刪
    cur2 = qexec(conn, "DELETE FROM computed_stats WHERE snapshot_time < %s", (computed_cutoff,))
    cur3 = qexec(conn, "DELETE FROM daily_closing WHERE date < %s", (closing_cutoff,))
    conn.commit()
    if cur2.rowcount > 0 or cur3.rowcount > 0:
        logger.info(f"清理舊資料：computed {cur2.rowcount} 筆, closing {cur3.rowcount} 筆")

    # raw_snapshots 分批刪除，每批 commit 後釋放鎖，讓儀表板可在批次間讀取
    total_deleted = 0
    batch_num = 0
    while True:
        cur1 = qexec(conn,
            "DELETE FROM raw_snapshots WHERE id IN ("
            "  SELECT id FROM raw_snapshots WHERE snapshot_time < %s LIMIT %s"
            ")", (raw_cutoff, _BATCH_SIZE))
        conn.commit()
        total_deleted += cur1.rowcount
        batch_num += 1
        if cur1.rowcount < _BATCH_SIZE:
            break
        if batch_num % 50 == 0:
            logger.info(f"清理 raw_snapshots 進度：已刪除 {total_deleted:,} 筆...")
        time.sleep(0.1)

    if total_deleted > 0:
        logger.info(f"清理舊資料：raw_snapshots 共刪除 {total_deleted:,} 筆（{batch_num} 批）")


def _compute_today_rolling_indicators(conn, today_str, logger):
    """
    計算當日的 20 日創新高/新低家數與均線結構，並 UPDATE 回 daily_closing。
    只讀取近 80 個交易日資料（60MA 需 60 天 + 緩衝）。
    """
    try:
        import pandas as pd
        dates = qall(conn,
            "SELECT DISTINCT date FROM daily_stocks WHERE date <= %s ORDER BY date DESC LIMIT 80",
            (today_str,)
        )
        if len(dates) < 20:
            logger.warning("_compute_today_rolling_indicators: daily_stocks 不足 20 日，跳過")
            return
        earliest = dates[-1][0]
        df = read_sql(
            """SELECT symbol, date, close_price FROM daily_stocks
               WHERE date >= %s AND date <= %s AND close_price IS NOT NULL AND close_price > 0""",
            conn, params=(earliest, today_str)
        )
        df = df[df['symbol'].str.match(r'^[1-9]\d{3}$')]
        if df.empty or today_str not in df['date'].values:
            return
        pivot = df.pivot_table(index='date', columns='symbol',
                               values='close_price', aggfunc='first').sort_index()

        if today_str not in pivot.index:
            return

        rm20  = pivot.rolling(20, min_periods=20).max()
        rmin20 = pivot.rolling(20, min_periods=20).min()
        sma5   = pivot.rolling(5,  min_periods=5).mean()
        sma20  = pivot.rolling(20, min_periods=20).mean()
        sma60  = pivot.rolling(60, min_periods=60).mean()

        row    = pivot.loc[today_str]
        c_high = int(((row == rm20.loc[today_str]) & rm20.loc[today_str].notna()).sum())
        c_low  = int(((row == rmin20.loc[today_str]) & rmin20.loc[today_str].notna()).sum())
        c5     = int((row > sma5.loc[today_str]).sum())  if today_str in sma5.index  else 0
        c20    = int((row > sma20.loc[today_str]).sum()) if today_str in sma20.index else 0
        c60    = int((row > sma60.loc[today_str]).sum()) if today_str in sma60.index else 0
        v5     = int(sma5.loc[today_str].notna().sum())  if today_str in sma5.index  else 0
        v20    = int(sma20.loc[today_str].notna().sum()) if today_str in sma20.index else 0
        v60    = int(sma60.loc[today_str].notna().sum()) if today_str in sma60.index else 0

        qexec(conn, """
            UPDATE daily_closing SET
                new_high_20d_count = %s, new_low_20d_count  = %s,
                above_5ma_count    = %s, above_20ma_count   = %s, above_60ma_count = %s,
                above_5ma_pct      = %s, above_20ma_pct     = %s, above_60ma_pct   = %s
            WHERE date = %s
        """, (
            c_high, c_low, c5, c20, c60,
            round(c5  / v5  * 100, 2) if v5  > 0 else None,
            round(c20 / v20 * 100, 2) if v20 > 0 else None,
            round(c60 / v60 * 100, 2) if v60 > 0 else None,
            today_str,
        ))
        conn.commit()
        logger.info(f"rolling 指標已更新：新高 {c_high} 新低 {c_low}  "
                    f"5MA:{c5} 20MA:{c20} 60MA:{c60}")
    except Exception as e:
        logger.error(f"_compute_today_rolling_indicators 失敗：{e}")


def save_daily_closing(conn, logger):
    """將今日最後一筆 computed_stats 存入 daily_closing（收盤後呼叫）"""
    today = datetime.now().strftime('%Y-%m-%d')
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM computed_stats WHERE snapshot_time LIKE %s ORDER BY snapshot_time DESC LIMIT 1",
        (f"{today}%",)
    )
    row = cur.fetchone()
    if not row:
        logger.warning("save_daily_closing: 今日無 computed_stats 資料，略過")
        return
    cols = [d[0] for d in cur.description]
    cs = dict(zip(cols, row))
    qexec(conn, """
        INSERT INTO daily_closing (
            date, filtered_total, above_5pct_count,
            up_count, down_count, flat_count, red_k_count, black_k_count, flat_k_count,
            tse_up_count, otc_up_count,
            total_trade_value, total_trade_volume,
            sentiment_index, ad_ratio, volatility, strength_index, activity_rate,
            bucket_up_2_5, bucket_up_5, bucket_up_7_5, bucket_up_above,
            bucket_down_2_5, bucket_down_5, bucket_down_7_5, bucket_down_above,
            advantage_count, strong_count, super_strong_count, near_limit_up_count,
            disadvantage_count, weak_count, super_weak_count, near_limit_down_count,
            prev_strong_count, prev_strong_avg_today, prev_strong_positive_rate,
            prev_weak_count, prev_weak_avg_today, prev_weak_negative_rate,
            top_n_avg, bottom_n_avg,
            blue_chip_up_count, blue_chip_total, blue_chip_avg_change,
            volume_tide_up_value, volume_tide_down_value,
            volume_tide_net, volume_tide_up_pct, volume_tide_down_pct
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (date) DO UPDATE SET
            filtered_total=EXCLUDED.filtered_total, above_5pct_count=EXCLUDED.above_5pct_count,
            up_count=EXCLUDED.up_count, down_count=EXCLUDED.down_count,
            flat_count=EXCLUDED.flat_count, red_k_count=EXCLUDED.red_k_count,
            black_k_count=EXCLUDED.black_k_count, flat_k_count=EXCLUDED.flat_k_count,
            tse_up_count=EXCLUDED.tse_up_count, otc_up_count=EXCLUDED.otc_up_count,
            total_trade_value=EXCLUDED.total_trade_value,
            total_trade_volume=EXCLUDED.total_trade_volume,
            sentiment_index=EXCLUDED.sentiment_index, ad_ratio=EXCLUDED.ad_ratio,
            volatility=EXCLUDED.volatility, strength_index=EXCLUDED.strength_index,
            activity_rate=EXCLUDED.activity_rate,
            bucket_up_2_5=EXCLUDED.bucket_up_2_5, bucket_up_5=EXCLUDED.bucket_up_5,
            bucket_up_7_5=EXCLUDED.bucket_up_7_5, bucket_up_above=EXCLUDED.bucket_up_above,
            bucket_down_2_5=EXCLUDED.bucket_down_2_5, bucket_down_5=EXCLUDED.bucket_down_5,
            bucket_down_7_5=EXCLUDED.bucket_down_7_5, bucket_down_above=EXCLUDED.bucket_down_above,
            advantage_count=EXCLUDED.advantage_count, strong_count=EXCLUDED.strong_count,
            super_strong_count=EXCLUDED.super_strong_count,
            near_limit_up_count=EXCLUDED.near_limit_up_count,
            disadvantage_count=EXCLUDED.disadvantage_count, weak_count=EXCLUDED.weak_count,
            super_weak_count=EXCLUDED.super_weak_count,
            near_limit_down_count=EXCLUDED.near_limit_down_count,
            prev_strong_count=EXCLUDED.prev_strong_count,
            prev_strong_avg_today=EXCLUDED.prev_strong_avg_today,
            prev_strong_positive_rate=EXCLUDED.prev_strong_positive_rate,
            prev_weak_count=EXCLUDED.prev_weak_count,
            prev_weak_avg_today=EXCLUDED.prev_weak_avg_today,
            prev_weak_negative_rate=EXCLUDED.prev_weak_negative_rate,
            top_n_avg=EXCLUDED.top_n_avg, bottom_n_avg=EXCLUDED.bottom_n_avg,
            blue_chip_up_count=EXCLUDED.blue_chip_up_count,
            blue_chip_total=EXCLUDED.blue_chip_total,
            blue_chip_avg_change=EXCLUDED.blue_chip_avg_change,
            volume_tide_up_value=EXCLUDED.volume_tide_up_value,
            volume_tide_down_value=EXCLUDED.volume_tide_down_value,
            volume_tide_net=EXCLUDED.volume_tide_net,
            volume_tide_up_pct=EXCLUDED.volume_tide_up_pct,
            volume_tide_down_pct=EXCLUDED.volume_tide_down_pct
    """, (
        today, cs.get('filtered_total'), cs.get('above_5pct_count'),
        cs.get('up_count'), cs.get('down_count'), cs.get('flat_count'),
        cs.get('red_k_count'), cs.get('black_k_count'), cs.get('flat_k_count'),
        cs.get('tse_up_count'), cs.get('otc_up_count'),
        cs.get('total_trade_value'), cs.get('total_trade_volume'),
        cs.get('sentiment_index'), cs.get('ad_ratio'), cs.get('volatility'),
        cs.get('strength_index'), cs.get('activity_rate'),
        cs.get('bucket_up_2_5'), cs.get('bucket_up_5'),
        cs.get('bucket_up_7_5'), cs.get('bucket_up_above'),
        cs.get('bucket_down_2_5'), cs.get('bucket_down_5'),
        cs.get('bucket_down_7_5'), cs.get('bucket_down_above'),
        cs.get('advantage_count'), cs.get('strong_count'),
        cs.get('super_strong_count'), cs.get('near_limit_up_count'),
        cs.get('disadvantage_count'), cs.get('weak_count'),
        cs.get('super_weak_count'), cs.get('near_limit_down_count'),
        cs.get('prev_strong_count'), cs.get('prev_strong_avg_today'),
        cs.get('prev_strong_positive_rate'),
        cs.get('prev_weak_count'), cs.get('prev_weak_avg_today'),
        cs.get('prev_weak_negative_rate'),
        cs.get('top_n_avg'), cs.get('bottom_n_avg'),
        cs.get('blue_chip_up_count'), cs.get('blue_chip_total'),
        cs.get('blue_chip_avg_change'),
        cs.get('volume_tide_up_value'), cs.get('volume_tide_down_value'),
        cs.get('volume_tide_net'), cs.get('volume_tide_up_pct'),
        cs.get('volume_tide_down_pct'),
    ))
    conn.commit()
    logger.info(f"save_daily_closing: {today} 收盤指標已存入 daily_closing")

    # 補算 rolling 指標（20日新高/新低、均線結構）
    _compute_today_rolling_indicators(conn, today, logger)


def cleanup_old_logs(logger):
    """清除超過 30 天的 Log 檔"""
    cutoff_date = (datetime.now() - timedelta(days=LOG_KEEP_DAYS)).date()
    for f in os.listdir(LOG_DIR):
        file_date = None
        try:
            if f.endswith('.log'):
                date_str = f[:-4]
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            elif '.log.' in f:
                suffix = f.rsplit('.log.', 1)[1]
                file_date = datetime.strptime(suffix, '%Y%m%d').date()
        except ValueError:
            file_date = None

        if file_date and file_date < cutoff_date:
            try:
                os.remove(os.path.join(LOG_DIR, f))
                logger.info(f"刪除舊 Log：{f}")
            except OSError as e:
                logger.warning(f"刪除舊 Log 失敗：{f} ({e})")


# ============================================================
#  富邦 SDK 登入
# ============================================================

def sdk_login(logger):
    """登入富邦 API，回傳 sdk 物件"""
    sdk = FubonSDK()

    cert_b64 = os.environ.get('FUBON_CERT_B64')
    _tmp_cert = None
    if cert_b64:
        import base64, tempfile
        fd, cert_path = tempfile.mkstemp(suffix='.pfx')
        _tmp_cert = cert_path
        with os.fdopen(fd, 'wb') as f:
            f.write(base64.b64decode(cert_b64))
        logger.info("使用 FUBON_CERT_B64 環境變數憑證")
    else:
        cert_path = win_to_wsl(_FUBON_CERT_PATH)
        logger.info(f"使用本機憑證：{cert_path}")

    try:
        sdk.login(_FUBON_ID, _FUBON_PWD, cert_path, _FUBON_CERT_PWD)
        logger.info("登入富邦 API 成功")
    except Exception as e:
        logger.error(f"登入失敗：{e}")
        raise
    finally:
        # 立即刪除暫存憑證檔（含私鑰，不應留在 /tmp）
        if _tmp_cert:
            try:
                os.unlink(_tmp_cert)
            except Exception:
                pass

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

    qmany(conn, """
        INSERT INTO raw_snapshots (
            snapshot_time, market, type, symbol, name,
            open_price, high_price, low_price, close_price, last_price,
            change, change_percent, reference_price,
            trade_volume, trade_value, last_updated, is_anomaly
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    conn.commit()

    if anomaly_count > 0:
        logger.warning(f"異常資料：{anomaly_count} 筆")

    return len(rows), anomaly_count


# ============================================================
#  啟動時自動補齊 daily_stocks
# ============================================================

def verify_daily_stocks(conn, dt, logger):
    """檢核單日 daily_stocks 資料品質"""
    ds = dt.strftime('%Y-%m-%d')
    total = qone(conn, "SELECT COUNT(*) FROM daily_stocks WHERE date = %s", (ds,))[0]

    if total == 0:
        return

    null_close = qone(conn, "SELECT COUNT(*) FROM daily_stocks WHERE date = %s AND close_price IS NULL", (ds,))[0]

    complete_ohlc = qone(conn, """
        SELECT COUNT(*) FROM daily_stocks WHERE date = %s
        AND open_price IS NOT NULL AND high_price IS NOT NULL
        AND low_price IS NOT NULL AND close_price IS NOT NULL
    """, (ds,))[0]

    ohlc_pct = complete_ohlc / total * 100 if total > 0 else 0

    if total < 1500:
        logger.warning(f"  ⚠ {ds}: 只有 {total} 檔（預期 1800~2000）")
    else:
        logger.info(f"  {ds}: {total} 檔, OHLC 完整率 {ohlc_pct:.1f}% ✓")

    if null_close > total * 0.05:
        logger.warning(f"  ⚠ {ds}: close_price NULL {null_close} 筆（{null_close/total*100:.1f}%）")


def ensure_daily_stocks(conn, today_str, logger):
    """
    啟動時確保 daily_stocks 有最近兩個有效交易日的資料。
    從昨天開始往回找，累計找到 2 個有資料的交易日就停止。
    """
    today = datetime.strptime(today_str, '%Y-%m-%d')

    # 查目前狀態
    latest = qone(conn, "SELECT MAX(date) FROM daily_stocks")[0]
    logger.info(f"daily_stocks 最新日期: {latest or '（無資料）'}")

    # 從昨天開始往回找
    found_trading_days = 0
    needed = 2  # 強弱勢股計算需要前兩個交易日
    max_lookback = 30
    check_date = today - timedelta(days=1)

    for _ in range(max_lookback):
        if found_trading_days >= needed:
            break

        # 跳過週末
        if check_date.weekday() >= 5:
            check_date -= timedelta(days=1)
            continue

        ds = check_date.strftime('%Y-%m-%d')

        # 檢查 DB 是否已有該日且品質合格
        existing_count = qone(conn, "SELECT COUNT(*) FROM daily_stocks WHERE date = %s", (ds,))[0]

        if existing_count >= 1500:
            logger.info(f"  {ds}: 已有 {existing_count} 檔 ✓")
            found_trading_days += 1
            check_date -= timedelta(days=1)
            continue

        # 需要補抓
        logger.info(f"  {ds}: 資料不足（{existing_count} 檔），開始補同步...")
        try:
            count = sync_daily(conn, check_date, logger)
        except Exception as e:
            logger.warning(f"  {ds}: 補同步失敗 → {e}")
            logger.debug(traceback.format_exc())
            check_date -= timedelta(days=1)
            continue

        if count == 0:
            # 非交易日（國定假日等），跳過不計數
            logger.info(f"  {ds}: 非交易日，跳過")
            check_date -= timedelta(days=1)
            continue

        # 補抓成功，做檢核
        verify_daily_stocks(conn, check_date, logger)
        found_trading_days += 1
        check_date -= timedelta(days=1)

    if found_trading_days < needed:
        logger.warning(
            f"⚠ 只找到 {found_trading_days} 個有效交易日（需要 {needed}），"
            f"前日強弱勢指標可能無法計算"
        )
    else:
        logger.info(f"daily_stocks 就緒：最近 {needed} 個交易日資料完整")


# ============================================================
#  取得昨日強勢股清單（用於延續性指標）
# ============================================================

def get_prev_strong_symbols(conn, today_str):
    """
    從 daily_stocks 取得前一交易日漲幅 > CONTINUITY_THRESHOLD 的股票代號清單。
    回傳 set of symbols，若無資料回傳空 set。
    """
    # 找前一交易日（daily_stocks 中 < today 的最大日期）
    row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (today_str,))

    if not row or not row[0]:
        return set()

    prev_date = row[0]

    # 計算漲幅：需要 close_price 和前一天的 close
    # daily_stocks 沒有直接的 change_percent，需要用 raw 或自己算
    # 但我們有 postmarket_sync 的 OHLCV，可以用兩天的 close 算
    prev_prev_row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (prev_date,))

    if not prev_prev_row or not prev_prev_row[0]:
        return set()

    prev_prev_date = prev_prev_row[0]

    # 取前一天漲幅超過門檻的股票
    rows = qall(conn, """
        SELECT d1.symbol
        FROM daily_stocks d1
        JOIN daily_stocks d0 ON d1.symbol = d0.symbol AND d0.date = %s
        WHERE d1.date = %s
          AND d0.close_price > 0
          AND ((d1.close_price - d0.close_price) / d0.close_price * 100) > %s
    """, (prev_prev_date, prev_date, CONTINUITY_THRESHOLD))

    return set(r[0] for r in rows)


def get_prev_weak_symbols(conn, today_str):
    """
    從 daily_stocks 取得前一交易日跌幅 < -CONTINUITY_THRESHOLD 的股票代號清單。
    回傳 set of symbols，若無資料回傳空 set。
    """
    row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (today_str,))

    if not row or not row[0]:
        return set()

    prev_date = row[0]

    prev_prev_row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (prev_date,))

    if not prev_prev_row or not prev_prev_row[0]:
        return set()

    prev_prev_date = prev_prev_row[0]

    rows = qall(conn, """
        SELECT d1.symbol
        FROM daily_stocks d1
        JOIN daily_stocks d0 ON d1.symbol = d0.symbol AND d0.date = %s
        WHERE d1.date = %s
          AND d0.close_price > 0
          AND ((d1.close_price - d0.close_price) / d0.close_price * 100) < %s
    """, (prev_prev_date, prev_date, -CONTINUITY_THRESHOLD))

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

    # --- K棒顏色家數（收盤 vs 今日開盤）---
    red_k_count = sum(
        1 for i in valid
        if i.get('openPrice') is not None and i.get('closePrice') is not None
        and i['closePrice'] > i['openPrice']
    )
    black_k_count = sum(
        1 for i in valid
        if i.get('openPrice') is not None and i.get('closePrice') is not None
        and i['closePrice'] < i['openPrice']
    )
    flat_k_count = sum(
        1 for i in valid
        if i.get('openPrice') is not None and i.get('closePrice') is not None
        and i['closePrice'] == i['openPrice']
    )

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

    # --- 漲幅超過 5% 家數 ---
    above_5pct_count = sum(1 for p in pcts if p > 5.0)

    # --- 百大強弱勢平均漲幅（排除漲跌幅 > 10.01%）---
    normal_pcts = [p for p in pcts if abs(p) <= ABNORMAL_THRESHOLD]
    normal_pcts_sorted = sorted(normal_pcts, reverse=True)

    n = min(TOP_BOTTOM_N, len(normal_pcts_sorted))
    top_n_avg = round(sum(normal_pcts_sorted[:n]) / n, 4) if n > 0 else None
    bottom_n_avg = round(sum(normal_pcts_sorted[-n:]) / n, 4) if n > 0 else None

    # --- 量能潮汐 ---
    vt_up_value = sum(i.get('tradeValue', 0) or 0 for i in valid if i['changePercent'] > 0)
    vt_down_value = sum(i.get('tradeValue', 0) or 0 for i in valid if i['changePercent'] < 0)
    vt_total = vt_up_value + vt_down_value
    vt_net = round((vt_up_value - vt_down_value) / 1e8, 2) if vt_total > 0 else 0
    vt_up_pct = round(vt_up_value / vt_total * 100, 2) if vt_total > 0 else None
    vt_down_pct = round(vt_down_value / vt_total * 100, 2) if vt_total > 0 else None

    # --- 權值股 ---
    blue_items = [i for i in valid if i.get('symbol') in BLUE_CHIPS]
    blue_chip_total = len(blue_items)
    blue_chip_up_count = sum(1 for i in blue_items if i['changePercent'] > 0)
    blue_pcts = [i['changePercent'] for i in blue_items]
    blue_chip_avg_change = round(sum(blue_pcts) / len(blue_pcts), 4) if blue_pcts else None

    # --- 寫入 ---
    qexec(conn, """
        INSERT INTO computed_stats (
            snapshot_time, filtered_total, above_5pct_count,
            up_count, down_count, flat_count, red_k_count, black_k_count, flat_k_count,
            tse_up_count, otc_up_count,
            total_trade_value, total_trade_volume,
            sentiment_index, ad_ratio, volatility, strength_index, activity_rate,
            bucket_up_2_5, bucket_up_5, bucket_up_7_5, bucket_up_above,
            bucket_down_2_5, bucket_down_5, bucket_down_7_5, bucket_down_above,
            advantage_count, strong_count, super_strong_count, near_limit_up_count,
            disadvantage_count, weak_count, super_weak_count, near_limit_down_count,
            prev_strong_count, prev_strong_avg_today, prev_strong_positive_rate,
            prev_weak_count, prev_weak_avg_today, prev_weak_negative_rate,
            top_n_avg, bottom_n_avg,
            blue_chip_up_count, blue_chip_total, blue_chip_avg_change,
            volume_tide_up_value, volume_tide_down_value,
            volume_tide_net, volume_tide_up_pct, volume_tide_down_pct
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        snapshot_time, filtered_total, above_5pct_count,
        up, down, flat, red_k_count, black_k_count, flat_k_count, tse_up, otc_up,
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
        vt_up_value, vt_down_value, vt_net, vt_up_pct, vt_down_pct,
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

    now = datetime.now()
    is_weekend = now.weekday() >= 5

    # 初始化資料庫（週末也執行，用於維護）
    conn = init_db()
    logger.info("資料庫初始化完成")
    reload_settings(logger)
    reload_blue_chips(logger)

    # 啟動時清理舊資料與舊 Log
    cleanup_old_data(conn, logger)
    cleanup_old_logs(logger)

    # 自動補齊 daily_stocks（證交所/櫃買中心盤後資料）
    today_str = now.strftime('%Y-%m-%d')
    ensure_daily_stocks(conn, today_str, logger)

    # 載入昨日強勢/弱勢股清單（用於延續性指標）
    prev_strong_symbols = get_prev_strong_symbols(conn, today_str)
    prev_weak_symbols = get_prev_weak_symbols(conn, today_str)

    # 印出實際基準日期（不論結果是否為空）
    prev_date_row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (today_str,))
    prev_date = prev_date_row[0] if prev_date_row and prev_date_row[0] else None

    if prev_date:
        prev_prev_row = qone(conn, "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (prev_date,))
        prev_prev_date = prev_prev_row[0] if prev_prev_row and prev_prev_row[0] else None
        logger.info(f"前日強弱勢基準: {prev_date} vs {prev_prev_date}")
        logger.info(f"  昨日強勢股（>{CONTINUITY_THRESHOLD}%）：{len(prev_strong_symbols)} 檔")
        logger.info(f"  昨日弱勢股（<-{CONTINUITY_THRESHOLD}%）：{len(prev_weak_symbols)} 檔")
    else:
        logger.warning("⚠ daily_stocks 無資料，前日強弱勢指標將顯示 N/A")

    # DB 健康狀態報告
    _ds_dates = qone(conn, "SELECT COUNT(DISTINCT date) FROM daily_stocks")[0]
    _ds_latest = qone(conn, "SELECT MAX(date) FROM daily_stocks")[0] or '無'
    _cs_dates = qone(conn, "SELECT COUNT(DISTINCT substr(snapshot_time,1,10)) FROM computed_stats")[0]
    logger.info(f"DB 狀態: daily_stocks {_ds_dates} 個交易日（最新 {_ds_latest}）, computed_stats {_cs_dates} 個交易日")

    # 週末：維護完成即結束，不進入盤中監控
    if is_weekend:
        conn.close()
        logger.info("[SYSTEM ] 今天是週末，資料維護完成，不進入盤中監控")
        return

    # 登入富邦 API
    sdk = sdk_login(logger)

    sdk_state = {
        'sdk': sdk,
        'stock_client': sdk.marketdata.rest_client.stock,
        'last_login_time': time.time(),
    }

    snapshot_count = 0
    _cleanup_tick = 0        # 盤中增量清理計數器

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

        # 盤中增量清理（每 60 個快照 ≈ 15 分鐘執行一批，防止 raw_snapshots 堆積）
        _cleanup_tick += 1
        if _cleanup_tick >= 60:
            _cleanup_tick = 0
            _raw_cutoff = (datetime.now() - timedelta(days=RAW_KEEP_DAYS)).strftime('%Y-%m-%d 00:00:00')
            _cur = qexec(conn,
                "DELETE FROM raw_snapshots WHERE id IN ("
                "  SELECT id FROM raw_snapshots WHERE snapshot_time < %s LIMIT 5000"
                ")", (_raw_cutoff,))
            conn.commit()
            if _cur.rowcount > 0:
                logger.info(f"盤中增量清理 raw_snapshots：{_cur.rowcount} 筆")

    # === 收盤收尾 ===
    save_daily_closing(conn, logger)
    conn.close()
    logger.info(f"[SYSTEM ] 今日共完成 {snapshot_count} 次快照")
    logger.info("[SYSTEM ] 系統正常關閉")


if __name__ == "__main__":
    main()
