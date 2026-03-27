"""
歷史每日收盤指標回填腳本
從 TWSE/TPEX API 批量下載 OHLCV 資料，計算每日指標，寫入 daily_closing。

用法：
    venv/bin/python backfill_history.py                   # 從 2020-01-01 回填
    venv/bin/python backfill_history.py --start 2021-01-01
    venv/bin/python backfill_history.py --start 2026-03-01  # 小範圍測試

特性：
    - 支援中斷續傳（已有 daily_stocks 資料的日期自動跳過）
    - 只抓 OHLCV（不抓法人/融資融券），約 2.5~3 小時完成全量回填
    - 計算指標後自動寫入 daily_closing
"""

import os
import sys
import time
import math
import json
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta, date

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

SETTINGS_PATH = os.path.join(BASE_DIR, 'config', 'settings.json')
BLUE_CHIPS_PATH = os.path.join(BASE_DIR, 'config', 'blue_chips.csv')

# 預設回填起始日
DEFAULT_START = date(2020, 1, 1)

from lib.db import get_connection, init_all_tables, ensure_columns, read_sql, qone, qall, qexec

# 重用 postmarket_sync 的 API 函式
from postmarket_sync import (
    fetch_tse_ohlcv, fetch_otc_ohlcv, merge_and_write,
    setup_logging as ps_setup_logging,
)

# ============================================================
#  設定與輔助
# ============================================================

def load_settings():
    try:
        with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def load_blue_chips():
    try:
        with open(BLUE_CHIPS_PATH, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception:
        return set()


def is_regular_stock(symbol):
    return len(symbol) == 4 and symbol.isdigit() and symbol[0] in '123456789'


def trading_dates(start: date, end: date):
    """產生 start..end 中所有非週末日期（不驗證假日，抓 API 時若非交易日會回空）"""
    d = start
    result = []
    while d <= end:
        if d.weekday() < 5:  # Mon~Fri
            result.append(d)
        d += timedelta(days=1)
    return result


# ============================================================
#  資料庫
# ============================================================

def open_db():
    conn = get_connection()
    init_all_tables(conn)
    # 動態補欄位（舊資料庫相容）
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


def get_existing_daily_stock_dates(conn):
    """回傳 daily_stocks 中已有資料的日期集合"""
    rows = qall(conn, "SELECT DISTINCT date FROM daily_stocks")
    return {r[0] for r in rows}


def get_existing_daily_closing_dates(conn):
    """回傳 daily_closing 中已有資料的日期集合"""
    rows = qall(conn, "SELECT DISTINCT date FROM daily_closing")
    return {r[0] for r in rows}


# ============================================================
#  每日指標計算（從 daily_stocks 資料重現 compute_stats 邏輯）
# ============================================================

def compute_daily_stats(conn, date_str, settings, blue_chips, logger):
    """
    從 daily_stocks 計算日期 date_str 的所有指標，
    回傳 dict（可直接寫入 daily_closing），失敗回傳 None。
    """
    # --- 讀取參數 ---
    volume_filter = settings.get('volume_filter', 0)
    bucket_tiers_raw = settings.get('bucket_tiers', [2.5, 5.0, 7.5])
    try:
        t1, t2, t3 = sorted(float(x) for x in bucket_tiers_raw if float(x) > 0)[:3]
    except Exception:
        t1, t2, t3 = 2.5, 5.0, 7.5
    limit_thr = settings.get('limit_threshold', 9.5)
    strong_thr = settings.get('indicator_strong', 3.0)
    super_strong_thr = settings.get('indicator_super_strong', 7.5)
    cont_thr = settings.get('continuity_threshold', 5.0)
    top_n = settings.get('top_bottom_n', 100)
    abnormal_thr = 10.01

    # --- 讀取當日個股 OHLCV ---
    rows_today = qall(conn, """
        SELECT symbol, market, close_price, open_price, trade_volume, trade_value
        FROM daily_stocks
        WHERE date = %s AND close_price IS NOT NULL AND close_price > 0
    """, (date_str,))

    if not rows_today:
        return None

    # --- 讀取前一交易日收盤價（用作參考價）---
    prev_date_row = qone(conn,
        "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (date_str,)
    )
    prev_date = prev_date_row[0] if prev_date_row and prev_date_row[0] else None

    prev_close = {}
    if prev_date:
        rows_prev = qall(conn,
            "SELECT symbol, close_price FROM daily_stocks WHERE date = %s AND close_price > 0",
            (prev_date,)
        )
        prev_close = {r[0]: r[1] for r in rows_prev}

    # --- 前日強弱勢股（D-2 → D-1 的漲跌）---
    prev_strong_symbols = set()
    prev_weak_symbols = set()
    if prev_date:
        prev_prev_row = qone(conn,
            "SELECT MAX(date) FROM daily_stocks WHERE date < %s", (prev_date,)
        )
        prev_prev_date = prev_prev_row[0] if prev_prev_row and prev_prev_row[0] else None
        if prev_prev_date:
            rows_d1 = qall(conn, """
                SELECT d1.symbol,
                       (d1.close_price - d0.close_price) / d0.close_price * 100
                FROM daily_stocks d1
                JOIN daily_stocks d0 ON d1.symbol = d0.symbol AND d0.date = %s
                WHERE d1.date = %s AND d0.close_price > 0 AND d1.close_price > 0
            """, (prev_prev_date, prev_date))
            for sym, pct in rows_d1:
                if pct is not None:
                    if pct > cont_thr:
                        prev_strong_symbols.add(sym)
                    elif pct < -cont_thr:
                        prev_weak_symbols.add(sym)

    # --- 組建 valid 清單（一般股票 + 有參考價 + 有成交量）---
    valid = []
    for sym, mkt, close_p, open_p, vol, val in rows_today:
        if not is_regular_stock(sym):
            continue
        if (vol or 0) < volume_filter:
            continue
        ref_p = prev_close.get(sym)
        if ref_p is None or ref_p <= 0:
            continue
        chg_pct = round((close_p - ref_p) / ref_p * 100, 4)
        valid.append({
            'symbol': sym, 'market': mkt,
            'close_price': close_p, 'open_price': open_p,
            'trade_volume': vol or 0, 'trade_value': val or 0,
            'change_percent': chg_pct,
        })

    filtered_total = len(valid)
    if filtered_total == 0:
        return None

    # --- 指標計算（同 main.py compute_stats）---
    pcts = [v['change_percent'] for v in valid]
    above_5pct_count = sum(1 for p in pcts if p > 5.0)

    up = sum(1 for p in pcts if p > 0)
    down = sum(1 for p in pcts if p < 0)
    flat = sum(1 for p in pcts if p == 0)
    tse_up = sum(1 for v in valid if v['market'] == 'TSE' and v['change_percent'] > 0)
    otc_up = sum(1 for v in valid if v['market'] == 'OTC' and v['change_percent'] > 0)

    red_k = sum(1 for v in valid if v['open_price'] and v['close_price'] > v['open_price'])
    black_k = sum(1 for v in valid if v['open_price'] and v['close_price'] < v['open_price'])
    flat_k = sum(1 for v in valid if v['open_price'] and v['close_price'] == v['open_price'])

    total_val = sum(v['trade_value'] for v in valid)
    total_vol = sum(v['trade_volume'] for v in valid)

    sentiment = round((up - down) / (up + down) * 100, 2) if (up + down) > 0 else 0
    ad_ratio = round(up / down, 4) if down > 0 else None

    strong_count = sum(1 for p in pcts if p >= strong_thr)
    weak_count = sum(1 for p in pcts if p <= -strong_thr)
    activity_rate = round((strong_count + weak_count) / filtered_total * 100, 2)

    extreme = [p for p in pcts if p >= strong_thr or p <= -strong_thr]
    strength_index = round(sum(extreme) / len(extreme), 4) if extreme else 0

    mean_p = sum(pcts) / len(pcts)
    volatility = round(math.sqrt(sum((p - mean_p) ** 2 for p in pcts) / len(pcts)), 4)

    bucket_up_2_5 = sum(1 for p in pcts if 0 < p <= t1)
    bucket_up_5 = sum(1 for p in pcts if t1 < p <= t2)
    bucket_up_7_5 = sum(1 for p in pcts if t2 < p <= t3)
    bucket_up_above = sum(1 for p in pcts if p > t3)
    bucket_down_2_5 = sum(1 for p in pcts if -t1 <= p < 0)
    bucket_down_5 = sum(1 for p in pcts if -t2 <= p < -t1)
    bucket_down_7_5 = sum(1 for p in pcts if -t3 <= p < -t2)
    bucket_down_above = sum(1 for p in pcts if p < -t3)

    super_strong = sum(1 for p in pcts if p >= super_strong_thr)
    super_weak = sum(1 for p in pcts if p <= -super_strong_thr)
    near_up = sum(1 for p in pcts if p >= limit_thr)
    near_down = sum(1 for p in pcts if p <= -limit_thr)

    # 前日強弱勢延續性
    ps_count = len(prev_strong_symbols) or None
    ps_avg = ps_rate = None
    pw_count = len(prev_weak_symbols) or None
    pw_avg = pw_rate = None

    ps_pcts = [v['change_percent'] for v in valid if v['symbol'] in prev_strong_symbols]
    if ps_pcts:
        ps_avg = round(sum(ps_pcts) / len(ps_pcts), 4)
        ps_rate = round(sum(1 for p in ps_pcts if p > 0) / len(ps_pcts) * 100, 2)

    pw_pcts = [v['change_percent'] for v in valid if v['symbol'] in prev_weak_symbols]
    if pw_pcts:
        pw_avg = round(sum(pw_pcts) / len(pw_pcts), 4)
        pw_rate = round(sum(1 for p in pw_pcts if p < 0) / len(pw_pcts) * 100, 2)

    # 強勢百 / 弱勢百
    normal = sorted([p for p in pcts if abs(p) <= abnormal_thr], reverse=True)
    n = min(top_n, len(normal))
    top_avg = round(sum(normal[:n]) / n, 4) if n > 0 else None
    bot_avg = round(sum(normal[-n:]) / n, 4) if n > 0 else None

    # 量能潮汐
    vt_up = sum(v['trade_value'] for v in valid if v['change_percent'] > 0)
    vt_down = sum(v['trade_value'] for v in valid if v['change_percent'] < 0)
    vt_total = vt_up + vt_down
    vt_net = round((vt_up - vt_down) / 1e8, 2) if vt_total > 0 else 0
    vt_up_pct = round(vt_up / vt_total * 100, 2) if vt_total > 0 else None
    vt_down_pct = round(vt_down / vt_total * 100, 2) if vt_total > 0 else None

    # 權值股
    blue = [v for v in valid if v['symbol'] in blue_chips]
    blue_total = len(blue)
    blue_up = sum(1 for v in blue if v['change_percent'] > 0)
    blue_avg = round(sum(v['change_percent'] for v in blue) / blue_total, 4) if blue_total > 0 else None

    return {
        'date': date_str,
        'filtered_total': filtered_total,
        'above_5pct_count': above_5pct_count,
        'up_count': up, 'down_count': down, 'flat_count': flat,
        'red_k_count': red_k, 'black_k_count': black_k, 'flat_k_count': flat_k,
        'tse_up_count': tse_up, 'otc_up_count': otc_up,
        'total_trade_value': total_val, 'total_trade_volume': total_vol,
        'sentiment_index': sentiment, 'ad_ratio': ad_ratio,
        'volatility': volatility, 'strength_index': strength_index,
        'activity_rate': activity_rate,
        'bucket_up_2_5': bucket_up_2_5, 'bucket_up_5': bucket_up_5,
        'bucket_up_7_5': bucket_up_7_5, 'bucket_up_above': bucket_up_above,
        'bucket_down_2_5': bucket_down_2_5, 'bucket_down_5': bucket_down_5,
        'bucket_down_7_5': bucket_down_7_5, 'bucket_down_above': bucket_down_above,
        'advantage_count': up,
        'strong_count': strong_count, 'super_strong_count': super_strong,
        'near_limit_up_count': near_up,
        'disadvantage_count': down,
        'weak_count': weak_count, 'super_weak_count': super_weak,
        'near_limit_down_count': near_down,
        'prev_strong_count': ps_count, 'prev_strong_avg_today': ps_avg,
        'prev_strong_positive_rate': ps_rate,
        'prev_weak_count': pw_count, 'prev_weak_avg_today': pw_avg,
        'prev_weak_negative_rate': pw_rate,
        'top_n_avg': top_avg, 'bottom_n_avg': bot_avg,
        'blue_chip_up_count': blue_up, 'blue_chip_total': blue_total,
        'blue_chip_avg_change': blue_avg,
        'volume_tide_up_value': vt_up, 'volume_tide_down_value': vt_down,
        'volume_tide_net': vt_net, 'volume_tide_up_pct': vt_up_pct,
        'volume_tide_down_pct': vt_down_pct,
    }


def write_daily_closing(conn, stats):
    """將計算好的指標 dict 寫入 daily_closing"""
    with conn.cursor() as cur:
        cur.execute("""
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
                %(date)s, %(filtered_total)s, %(above_5pct_count)s,
                %(up_count)s, %(down_count)s, %(flat_count)s,
                %(red_k_count)s, %(black_k_count)s, %(flat_k_count)s,
                %(tse_up_count)s, %(otc_up_count)s,
                %(total_trade_value)s, %(total_trade_volume)s,
                %(sentiment_index)s, %(ad_ratio)s, %(volatility)s,
                %(strength_index)s, %(activity_rate)s,
                %(bucket_up_2_5)s, %(bucket_up_5)s, %(bucket_up_7_5)s, %(bucket_up_above)s,
                %(bucket_down_2_5)s, %(bucket_down_5)s, %(bucket_down_7_5)s, %(bucket_down_above)s,
                %(advantage_count)s, %(strong_count)s, %(super_strong_count)s, %(near_limit_up_count)s,
                %(disadvantage_count)s, %(weak_count)s, %(super_weak_count)s, %(near_limit_down_count)s,
                %(prev_strong_count)s, %(prev_strong_avg_today)s, %(prev_strong_positive_rate)s,
                %(prev_weak_count)s, %(prev_weak_avg_today)s, %(prev_weak_negative_rate)s,
                %(top_n_avg)s, %(bottom_n_avg)s,
                %(blue_chip_up_count)s, %(blue_chip_total)s, %(blue_chip_avg_change)s,
                %(volume_tide_up_value)s, %(volume_tide_down_value)s,
                %(volume_tide_net)s, %(volume_tide_up_pct)s, %(volume_tide_down_pct)s
            )
            ON CONFLICT (date) DO UPDATE SET
                filtered_total=EXCLUDED.filtered_total, above_5pct_count=EXCLUDED.above_5pct_count,
                up_count=EXCLUDED.up_count, down_count=EXCLUDED.down_count, flat_count=EXCLUDED.flat_count,
                red_k_count=EXCLUDED.red_k_count, black_k_count=EXCLUDED.black_k_count, flat_k_count=EXCLUDED.flat_k_count,
                tse_up_count=EXCLUDED.tse_up_count, otc_up_count=EXCLUDED.otc_up_count,
                total_trade_value=EXCLUDED.total_trade_value, total_trade_volume=EXCLUDED.total_trade_volume,
                sentiment_index=EXCLUDED.sentiment_index, ad_ratio=EXCLUDED.ad_ratio,
                volatility=EXCLUDED.volatility, strength_index=EXCLUDED.strength_index,
                activity_rate=EXCLUDED.activity_rate,
                bucket_up_2_5=EXCLUDED.bucket_up_2_5, bucket_up_5=EXCLUDED.bucket_up_5,
                bucket_up_7_5=EXCLUDED.bucket_up_7_5, bucket_up_above=EXCLUDED.bucket_up_above,
                bucket_down_2_5=EXCLUDED.bucket_down_2_5, bucket_down_5=EXCLUDED.bucket_down_5,
                bucket_down_7_5=EXCLUDED.bucket_down_7_5, bucket_down_above=EXCLUDED.bucket_down_above,
                advantage_count=EXCLUDED.advantage_count, strong_count=EXCLUDED.strong_count,
                super_strong_count=EXCLUDED.super_strong_count, near_limit_up_count=EXCLUDED.near_limit_up_count,
                disadvantage_count=EXCLUDED.disadvantage_count, weak_count=EXCLUDED.weak_count,
                super_weak_count=EXCLUDED.super_weak_count, near_limit_down_count=EXCLUDED.near_limit_down_count,
                prev_strong_count=EXCLUDED.prev_strong_count, prev_strong_avg_today=EXCLUDED.prev_strong_avg_today,
                prev_strong_positive_rate=EXCLUDED.prev_strong_positive_rate,
                prev_weak_count=EXCLUDED.prev_weak_count, prev_weak_avg_today=EXCLUDED.prev_weak_avg_today,
                prev_weak_negative_rate=EXCLUDED.prev_weak_negative_rate,
                top_n_avg=EXCLUDED.top_n_avg, bottom_n_avg=EXCLUDED.bottom_n_avg,
                blue_chip_up_count=EXCLUDED.blue_chip_up_count, blue_chip_total=EXCLUDED.blue_chip_total,
                blue_chip_avg_change=EXCLUDED.blue_chip_avg_change,
                volume_tide_up_value=EXCLUDED.volume_tide_up_value, volume_tide_down_value=EXCLUDED.volume_tide_down_value,
                volume_tide_net=EXCLUDED.volume_tide_net, volume_tide_up_pct=EXCLUDED.volume_tide_up_pct,
                volume_tide_down_pct=EXCLUDED.volume_tide_down_pct
        """, stats)
    conn.commit()


# ============================================================
#  Rolling 指標批次計算（20日新高/新低 + 均線結構）
# ============================================================

def compute_rolling_indicators(conn, logger):
    """
    批次計算所有 daily_closing 日期的：
      - 20 日創新高/新低家數
      - 站穩 5MA / 20MA / 60MA 家數與百分比
    以 pandas 向量化一次性處理，約 5-15 秒完成 1500+ 天。
    回傳 dict: {date_str: {欄位: 值, ...}}
    """
    logger.info("載入 daily_stocks 全量資料...")
    df = read_sql(
        """SELECT symbol, date, close_price FROM daily_stocks
           WHERE close_price IS NOT NULL AND close_price > 0
           ORDER BY date""",
        conn
    )
    # 只保留一般股票（4碼數字）
    df = df[df['symbol'].str.match(r'^[1-9]\d{3}$')].copy()
    logger.info(f"  共 {len(df)} 筆，{df['symbol'].nunique()} 支股票，"
                f"{df['date'].nunique()} 個交易日")

    if df.empty:
        logger.warning("daily_stocks 無資料，跳過 rolling 指標計算")
        return {}

    # 建構 pivot（行=日期, 列=股票, 值=收盤價）
    logger.info("建構 pivot 表 + 計算 rolling...")
    pivot = df.pivot_table(index='date', columns='symbol',
                           values='close_price', aggfunc='first')
    pivot = pivot.sort_index()

    # 20 日最高/最低
    roll_max20 = pivot.rolling(window=20, min_periods=20).max()
    roll_min20 = pivot.rolling(window=20, min_periods=20).min()
    is_new_high = (pivot == roll_max20) & roll_max20.notna()
    is_new_low  = (pivot == roll_min20) & roll_min20.notna()

    # 均線
    sma5  = pivot.rolling(window=5,  min_periods=5).mean()
    sma20 = pivot.rolling(window=20, min_periods=20).mean()
    sma60 = pivot.rolling(window=60, min_periods=60).mean()
    above_5ma  = (pivot > sma5)  & sma5.notna()
    above_20ma = (pivot > sma20) & sma20.notna()
    above_60ma = (pivot > sma60) & sma60.notna()
    valid_5  = sma5.notna()
    valid_20 = sma20.notna()
    valid_60 = sma60.notna()

    # 取出 daily_closing 中已有的日期清單
    closing_dates = {r[0] for r in qall(conn, "SELECT date FROM daily_closing")}
    all_pivot_dates = set(pivot.index)
    target_dates = sorted(closing_dates & all_pivot_dates)

    logger.info(f"  計算 {len(target_dates)} 個交易日的 rolling 指標...")
    result = {}
    for d in target_dates:
        c_high = int(is_new_high.loc[d].sum())
        c_low  = int(is_new_low.loc[d].sum())
        c5  = int(above_5ma.loc[d].sum())
        c20 = int(above_20ma.loc[d].sum())
        c60 = int(above_60ma.loc[d].sum())
        v5  = int(valid_5.loc[d].sum())
        v20 = int(valid_20.loc[d].sum())
        v60 = int(valid_60.loc[d].sum())
        result[d] = {
            'new_high_20d_count': c_high,
            'new_low_20d_count':  c_low,
            'above_5ma_count':    c5,
            'above_20ma_count':   c20,
            'above_60ma_count':   c60,
            'above_5ma_pct':  round(c5  / v5  * 100, 2) if v5  > 0 else None,
            'above_20ma_pct': round(c20 / v20 * 100, 2) if v20 > 0 else None,
            'above_60ma_pct': round(c60 / v60 * 100, 2) if v60 > 0 else None,
        }
    return result


# ============================================================
#  主程式
# ============================================================

def setup_logger():
    logger = logging.getLogger('backfill')
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%H:%M:%S')
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


def main():
    parser = argparse.ArgumentParser(description='歷史每日收盤指標批量回填')
    parser.add_argument('--start', default=DEFAULT_START.strftime('%Y-%m-%d'),
                        help='回填起始日期 YYYY-MM-DD，預設 2020-01-01')
    parser.add_argument('--stats-only', action='store_true',
                        help='跳過 API 下載，僅重新計算 daily_closing（daily_stocks 已有資料時使用）')
    parser.add_argument('--fill-rolling', action='store_true',
                        help='批次計算 rolling 指標（20日新高/新低、均線結構）並填入 daily_closing')
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
    except ValueError:
        print(f"日期格式錯誤：{args.start}，請用 YYYY-MM-DD")
        sys.exit(1)

    end_date = date.today()
    logger = setup_logger()
    settings = load_settings()
    blue_chips = load_blue_chips()

    logger.info(f"回填範圍：{start_date} ~ {end_date}")
    logger.info(f"設定：volume_filter={settings.get('volume_filter',0)}, "
                f"strong_thr={settings.get('indicator_strong',3)}")

    conn = open_db()
    existing_stocks = get_existing_daily_stock_dates(conn)
    existing_closing = get_existing_daily_closing_dates(conn)

    all_dates = trading_dates(start_date, end_date)
    logger.info(f"候選交易日：{len(all_dates)} 天（含週末已剔除，假日會回空跳過）")

    # 計算需要補的天數
    need_fetch = [d for d in all_dates if d.strftime('%Y-%m-%d') not in existing_stocks]
    need_stats = [d for d in all_dates
                  if d.strftime('%Y-%m-%d') in existing_stocks
                  and d.strftime('%Y-%m-%d') not in existing_closing]

    if args.stats_only:
        need_fetch = []
        need_stats = [d for d in all_dates if d.strftime('%Y-%m-%d') not in existing_closing]

    if args.fill_rolling:
        need_fetch = []   # --fill-rolling 不需要下載 OHLCV
        need_stats = []   # 也不需要重算基礎指標

    logger.info(f"需要下載 OHLCV：{len(need_fetch)} 天")
    logger.info(f"已有 OHLCV，需要計算指標：{len(need_stats)} 天")

    # ---- Phase 1: 下載 OHLCV ----
    if need_fetch:
        logger.info("=" * 50)
        logger.info("Phase 1：下載 OHLCV")
        logger.info("=" * 50)
        fetch_ok = 0
        fetch_skip = 0
        for i, d in enumerate(need_fetch):
            d_str = d.strftime('%Y-%m-%d')
            pct = (i + 1) / len(need_fetch) * 100
            print(f"\r  [{i+1:4d}/{len(need_fetch)}] {d_str}  ({pct:.1f}%)  ", end='', flush=True)

            tse = fetch_tse_ohlcv(d, logger)
            time.sleep(3)
            otc = fetch_otc_ohlcv(d, logger)
            time.sleep(3)

            ohlcv = {**tse, **otc}
            if not ohlcv:
                fetch_skip += 1
                continue

            merge_and_write(conn, d, ohlcv, {}, {}, logger)
            existing_stocks.add(d_str)
            fetch_ok += 1

        print()
        logger.info(f"Phase 1 完成：成功 {fetch_ok} 天，跳過（非交易日）{fetch_skip} 天")

    # ---- Phase 2: 計算指標 ----
    all_to_compute = sorted(set(
        d for d in all_dates
        if d.strftime('%Y-%m-%d') in existing_stocks
        and d.strftime('%Y-%m-%d') not in existing_closing
    ))

    if all_to_compute:
        logger.info("=" * 50)
        logger.info("Phase 2：計算每日指標")
        logger.info("=" * 50)
        calc_ok = calc_skip = 0
        for i, d in enumerate(all_to_compute):
            d_str = d.strftime('%Y-%m-%d')
            pct = (i + 1) / len(all_to_compute) * 100
            print(f"\r  [{i+1:4d}/{len(all_to_compute)}] {d_str}  ({pct:.1f}%)  ", end='', flush=True)

            stats = compute_daily_stats(conn, d_str, settings, blue_chips, logger)
            if stats is None:
                calc_skip += 1
                continue
            write_daily_closing(conn, stats)
            calc_ok += 1

        print()
        logger.info(f"Phase 2 完成：成功 {calc_ok} 天，無資料跳過 {calc_skip} 天")

    # ---- Phase 3: Rolling 指標 ----
    if args.fill_rolling:
        logger.info("=" * 50)
        logger.info("Phase 3：計算 rolling 指標（20日新高/新低、均線結構）")
        logger.info("=" * 50)

        # 先用 SQL 從現有 bucket 欄位補算 above_5pct_count（效率最高）
        updated_5pct = qexec(conn, """
            UPDATE daily_closing
            SET above_5pct_count = bucket_up_7_5 + bucket_up_above
            WHERE above_5pct_count IS NULL
              AND bucket_up_7_5 IS NOT NULL
              AND bucket_up_above IS NOT NULL
        """).rowcount
        conn.commit()
        if updated_5pct > 0:
            logger.info(f"  已補算 above_5pct_count：{updated_5pct} 筆")

        # 批次計算 20日新高/新低 + 均線結構
        rolling_data = compute_rolling_indicators(conn, logger)

        if rolling_data:
            logger.info(f"  寫入 {len(rolling_data)} 天的 rolling 指標...")
            with conn.cursor() as cur:
                for d_str, vals in rolling_data.items():
                    cur.execute("""
                        UPDATE daily_closing SET
                            new_high_20d_count = %(new_high_20d_count)s,
                            new_low_20d_count  = %(new_low_20d_count)s,
                            above_5ma_count    = %(above_5ma_count)s,
                            above_20ma_count   = %(above_20ma_count)s,
                            above_60ma_count   = %(above_60ma_count)s,
                            above_5ma_pct      = %(above_5ma_pct)s,
                            above_20ma_pct     = %(above_20ma_pct)s,
                            above_60ma_pct     = %(above_60ma_pct)s
                        WHERE date = %(date)s
                    """, {**vals, 'date': d_str})
            conn.commit()
            logger.info("Phase 3 完成")

    # ---- 最終統計 ----
    total_closing = qone(conn, "SELECT COUNT(*) FROM daily_closing")[0]
    date_range = qone(conn, "SELECT MIN(date), MAX(date) FROM daily_closing")
    logger.info(f"daily_closing 目前共 {total_closing} 天，範圍：{date_range[0]} ~ {date_range[1]}")
    conn.close()
    logger.info("回填完成")


if __name__ == '__main__':
    main()
