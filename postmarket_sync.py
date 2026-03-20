"""
盤後資料同步 - 從證交所/櫃買中心抓取每日收盤行情、三大法人、融資融券
寫入 SQLite 的 daily_stocks 表
執行方式：cd /mnt/c/Users/User/Desktop/FB-API && venv/bin/python market-metadata/postmarket_sync.py
可帶參數指定日期：venv/bin/python market-metadata/postmarket_sync.py 2026-03-06
"""
import sys
import os
import time
import sqlite3
import json
import logging
import urllib.request
from datetime import datetime, timedelta

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'market.db')
LOG_DIR = os.path.join(BASE_DIR, 'log')

# === 常數 ===
REQUEST_DELAY = 3  # 每次 API 呼叫間隔（秒），避免被擋
REQUEST_TIMEOUT = 20
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
DAILY_STOCKS_KEEP_DAYS = 120  # 保留 4 個月


# ============================================================
#  Logging
# ============================================================

def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = os.path.join(LOG_DIR, f'{today}.log')

    logger = logging.getLogger('postmarket_sync')
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
#  工具函式
# ============================================================

def fetch_json(url, logger):
    """發送 HTTP GET 請求，回傳 JSON"""
    req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
    try:
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
        raw = resp.read()
        return json.loads(raw)
    except Exception as e:
        logger.error(f"HTTP 請求失敗: {url} → {e}")
        raise


def parse_number(s):
    """把 '1,234,567' 這種字串轉成數字，失敗回傳 None"""
    if s is None:
        return None
    s = str(s).strip().replace(',', '')
    if s == '' or s == '--' or s == '---' or s == 'N/A':
        return None
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except ValueError:
        return None


def to_roc_date(dt):
    """西元日期轉民國格式 115/03/06"""
    roc_year = dt.year - 1911
    return f"{roc_year}/{dt.month:02d}/{dt.day:02d}"


def to_ad_date_str(dt):
    """日期轉 20260306 格式"""
    return dt.strftime('%Y%m%d')


def is_regular_stock(symbol):
    """判斷是否為一般股票（4碼純數字，首碼1-9）"""
    return (len(symbol) == 4 and symbol.isdigit() and symbol[0] in '123456789')


# ============================================================
#  資料庫
# ============================================================

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    # /mnt/c 下的 SQLite 搭配 WAL 容易出現 shared-memory/lock 問題。
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.executescript("""
    CREATE TABLE IF NOT EXISTS daily_stocks (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT NOT NULL,
        market          TEXT NOT NULL,
        symbol          TEXT NOT NULL,
        name            TEXT,
        -- OHLCV
        open_price      REAL,
        high_price      REAL,
        low_price       REAL,
        close_price     REAL,
        trade_volume    INTEGER,
        trade_value     REAL,
        trade_count     INTEGER,
        -- 三大法人買賣超（股數）
        foreign_buy     INTEGER,
        foreign_sell    INTEGER,
        foreign_net     INTEGER,
        trust_buy       INTEGER,
        trust_sell      INTEGER,
        trust_net       INTEGER,
        dealer_net      INTEGER,
        inst_total_net  INTEGER,
        -- 融資融券（張數）
        margin_buy      INTEGER,
        margin_sell     INTEGER,
        margin_redeem   INTEGER,
        margin_balance  INTEGER,
        short_sell      INTEGER,
        short_buy       INTEGER,
        short_redeem    INTEGER,
        short_balance   INTEGER,
        offset          INTEGER,
        -- 唯一約束
        UNIQUE(date, symbol)
    );
    CREATE INDEX IF NOT EXISTS idx_daily_stocks_date ON daily_stocks(date);
    CREATE INDEX IF NOT EXISTS idx_daily_stocks_symbol ON daily_stocks(symbol);
    """)
    conn.commit()
    return conn


def cleanup_old_daily(conn, logger):
    cutoff = (datetime.now() - timedelta(days=DAILY_STOCKS_KEEP_DAYS)).strftime('%Y-%m-%d')
    cur = conn.execute("DELETE FROM daily_stocks WHERE date < ?", (cutoff,))
    conn.commit()
    if cur.rowcount > 0:
        logger.info(f"清理舊 daily_stocks：{cur.rowcount} 筆")


# ============================================================
#  抓取：每日收盤行情
# ============================================================

def fetch_tse_ohlcv(dt, logger):
    """抓取上市每日收盤行情，回傳 {symbol: {...}} 字典"""
    date_str = to_ad_date_str(dt)
    url = f'https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALLBUT0999'
    logger.info(f"[TSE OHLCV] 抓取 {date_str}")
    data = fetch_json(url, logger)

    if data.get('stat') != 'OK':
        logger.warning(f"[TSE OHLCV] stat={data.get('stat')}，可能非交易日")
        return {}

    # 找到每日收盤行情的 table（欄位包含 '證券代號'）
    target = None
    for t in data.get('tables', []):
        fields = t.get('fields', [])
        if fields and fields[0] == '證券代號':
            target = t
            break

    if not target:
        logger.warning("[TSE OHLCV] 找不到收盤行情表")
        return {}

    result = {}
    for row in target.get('data', []):
        symbol = row[0].strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'market': 'TSE',
            'name': row[1].strip(),
            'trade_volume': parse_number(row[2]),
            'trade_count': parse_number(row[3]),
            'trade_value': parse_number(row[4]),
            'open_price': parse_number(row[5]),
            'high_price': parse_number(row[6]),
            'low_price': parse_number(row[7]),
            'close_price': parse_number(row[8]),
        }

    logger.info(f"[TSE OHLCV] 取得 {len(result)} 檔一般股票")
    return result


def fetch_otc_ohlcv(dt, logger):
    """抓取上櫃每日收盤行情"""
    roc_date = to_roc_date(dt)
    url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={roc_date}&se=EW'
    logger.info(f"[OTC OHLCV] 抓取 {roc_date}")
    data = fetch_json(url, logger)

    target = None
    for t in data.get('tables', []):
        if t.get('data') and len(t['data']) > 0:
            target = t
            break

    if not target:
        logger.warning("[OTC OHLCV] 找不到收盤行情表")
        return {}

    # 欄位順序：代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 成交股數, 成交金額, 成交筆數, ...
    result = {}
    for row in target.get('data', []):
        symbol = str(row[0]).strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'market': 'OTC',
            'name': str(row[1]).strip(),
            'close_price': parse_number(row[2]),
            'open_price': parse_number(row[4]),
            'high_price': parse_number(row[5]),
            'low_price': parse_number(row[6]),
            'trade_volume': parse_number(row[7]),
            'trade_value': parse_number(row[8]),
            'trade_count': parse_number(row[9]),
        }

    logger.info(f"[OTC OHLCV] 取得 {len(result)} 檔一般股票")
    return result


# ============================================================
#  抓取：三大法人
# ============================================================

def fetch_tse_institutional(dt, logger):
    """抓取上市三大法人買賣超"""
    date_str = to_ad_date_str(dt)
    url = f'https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999'
    logger.info(f"[TSE 法人] 抓取 {date_str}")
    data = fetch_json(url, logger)

    if data.get('stat') != 'OK':
        logger.warning(f"[TSE 法人] stat={data.get('stat')}")
        return {}

    # fields: 證券代號, 證券名稱, 外資買, 外資賣, 外資淨,
    #         外資自營商買, 外資自營商賣, 外資自營商淨,
    #         投信買, 投信賣, 投信淨,
    #         自營商淨, 自營商買(自行), 自營商賣(自行), 自營商淨(自行),
    #         自營商買(避險), 自營商賣(避險), 自營商淨(避險),
    #         三大法人買賣超
    result = {}
    for row in data.get('data', []):
        symbol = row[0].strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'foreign_buy': parse_number(row[2]),
            'foreign_sell': parse_number(row[3]),
            'foreign_net': parse_number(row[4]),
            'trust_buy': parse_number(row[8]),
            'trust_sell': parse_number(row[9]),
            'trust_net': parse_number(row[10]),
            'dealer_net': parse_number(row[11]),
            'inst_total_net': parse_number(row[18]),
        }

    logger.info(f"[TSE 法人] 取得 {len(result)} 檔")
    return result


def fetch_otc_institutional(dt, logger):
    """抓取上櫃三大法人買賣超"""
    roc_date = to_roc_date(dt)
    url = f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&d={roc_date}&se=EW&t=D'
    logger.info(f"[OTC 法人] 抓取 {roc_date}")
    data = fetch_json(url, logger)

    target = None
    for t in data.get('tables', []):
        if t.get('data') and len(t['data']) > 0:
            target = t
            break

    if not target:
        logger.warning("[OTC 法人] 找不到資料表")
        return {}

    # 欄位順序（含避險）：
    # 代號, 名稱,
    # 外資及陸資(不含外資自營商): 買, 賣, 淨
    # 外資自營商: 買, 賣, 淨
    # 投信: 買, 賣, 淨
    # 自營商(自行): 買, 賣, 淨
    # 自營商(避險): 買, 賣, 淨
    # 自營商: 買, 賣, 淨
    # 三大法人合計
    result = {}
    for row in target.get('data', []):
        symbol = str(row[0]).strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'foreign_buy': parse_number(row[2]),
            'foreign_sell': parse_number(row[3]),
            'foreign_net': parse_number(row[4]),
            'trust_buy': parse_number(row[8]),
            'trust_sell': parse_number(row[9]),
            'trust_net': parse_number(row[10]),
            'dealer_net': parse_number(row[17]),
            'inst_total_net': parse_number(row[23]),
        }

    logger.info(f"[OTC 法人] 取得 {len(result)} 檔")
    return result


# ============================================================
#  抓取：融資融券
# ============================================================

def fetch_tse_margin(dt, logger):
    """抓取上市融資融券"""
    date_str = to_ad_date_str(dt)
    url = f'https://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date={date_str}&selectType=STOCK'
    logger.info(f"[TSE 融資融券] 抓取 {date_str}")
    data = fetch_json(url, logger)

    if data.get('stat') != 'OK':
        logger.warning(f"[TSE 融資融券] stat={data.get('stat')}")
        return {}

    # 找 table[1]（table[0] 通常是空的）
    target = None
    for t in data.get('tables', []):
        if t.get('data') and len(t['data']) > 0:
            target = t
            break

    if not target:
        logger.warning("[TSE 融資融券] 找不到資料表")
        return {}

    # fields: 代號, 名稱,
    # 融資: 買進, 賣出, 現金償還, 前日餘額, 今日餘額, 限額
    # 融券: 買進, 賣出, 現券償還, 前日餘額, 今日餘額, 限額
    # 資券互抵, 註記
    result = {}
    for row in target.get('data', []):
        symbol = str(row[0]).strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'margin_buy': parse_number(row[2]),
            'margin_sell': parse_number(row[3]),
            'margin_redeem': parse_number(row[4]),
            'margin_balance': parse_number(row[6]),
            'short_sell': parse_number(row[9]),
            'short_buy': parse_number(row[10]),
            'short_redeem': parse_number(row[11]),
            'short_balance': parse_number(row[12]),
            'offset': parse_number(row[14]),
        }

    logger.info(f"[TSE 融資融券] 取得 {len(result)} 檔")
    return result


def fetch_otc_margin(dt, logger):
    """抓取上櫃融資融券"""
    roc_date = to_roc_date(dt)
    url = f'https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&d={roc_date}&se=EW'
    logger.info(f"[OTC 融資融券] 抓取 {roc_date}")
    data = fetch_json(url, logger)

    target = None
    for t in data.get('tables', []):
        if t.get('data') and len(t['data']) > 0:
            target = t
            break

    if not target:
        logger.warning("[OTC 融資融券] 找不到資料表")
        return {}

    # fields: 代號, 名稱,
    # 前資餘額, 資買, 資賣, 現償, 資餘額, 資屬證金, 資使用率, 資限額,
    # 前券餘額, 券賣, 券買, 券償, 券餘額, 券屬證金, 券使用率, 券限額,
    # 資券相抵, 備註
    result = {}
    for row in target.get('data', []):
        symbol = str(row[0]).strip()
        if not is_regular_stock(symbol):
            continue
        result[symbol] = {
            'margin_buy': parse_number(row[3]),
            'margin_sell': parse_number(row[4]),
            'margin_redeem': parse_number(row[5]),
            'margin_balance': parse_number(row[6]),
            'short_sell': parse_number(row[11]),
            'short_buy': parse_number(row[12]),
            'short_redeem': parse_number(row[13]),
            'short_balance': parse_number(row[14]),
            'offset': parse_number(row[18]),
        }

    logger.info(f"[OTC 融資融券] 取得 {len(result)} 檔")
    return result


# ============================================================
#  合併寫入
# ============================================================

def merge_and_write(conn, dt, ohlcv, institutional, margin, logger):
    """合併三類資料，寫入 daily_stocks"""
    date_str = dt.strftime('%Y-%m-%d')

    # 以 OHLCV 為主鍵，合併其他資料
    all_symbols = set(ohlcv.keys())
    rows = []

    for symbol in all_symbols:
        o = ohlcv.get(symbol, {})
        inst = institutional.get(symbol, {})
        mg = margin.get(symbol, {})

        rows.append((
            date_str,
            o.get('market', ''),
            symbol,
            o.get('name'),
            o.get('open_price'),
            o.get('high_price'),
            o.get('low_price'),
            o.get('close_price'),
            o.get('trade_volume'),
            o.get('trade_value'),
            o.get('trade_count'),
            inst.get('foreign_buy'),
            inst.get('foreign_sell'),
            inst.get('foreign_net'),
            inst.get('trust_buy'),
            inst.get('trust_sell'),
            inst.get('trust_net'),
            inst.get('dealer_net'),
            inst.get('inst_total_net'),
            mg.get('margin_buy'),
            mg.get('margin_sell'),
            mg.get('margin_redeem'),
            mg.get('margin_balance'),
            mg.get('short_sell'),
            mg.get('short_buy'),
            mg.get('short_redeem'),
            mg.get('short_balance'),
            mg.get('offset'),
        ))

    # UPSERT：有衝突時更新全部欄位
    conn.executemany("""
        INSERT INTO daily_stocks (
            date, market, symbol, name,
            open_price, high_price, low_price, close_price,
            trade_volume, trade_value, trade_count,
            foreign_buy, foreign_sell, foreign_net,
            trust_buy, trust_sell, trust_net,
            dealer_net, inst_total_net,
            margin_buy, margin_sell, margin_redeem, margin_balance,
            short_sell, short_buy, short_redeem, short_balance,
            offset
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(date, symbol) DO UPDATE SET
            market=excluded.market, name=excluded.name,
            open_price=excluded.open_price, high_price=excluded.high_price,
            low_price=excluded.low_price, close_price=excluded.close_price,
            trade_volume=excluded.trade_volume, trade_value=excluded.trade_value,
            trade_count=excluded.trade_count,
            foreign_buy=excluded.foreign_buy, foreign_sell=excluded.foreign_sell,
            foreign_net=excluded.foreign_net,
            trust_buy=excluded.trust_buy, trust_sell=excluded.trust_sell,
            trust_net=excluded.trust_net,
            dealer_net=excluded.dealer_net, inst_total_net=excluded.inst_total_net,
            margin_buy=excluded.margin_buy, margin_sell=excluded.margin_sell,
            margin_redeem=excluded.margin_redeem, margin_balance=excluded.margin_balance,
            short_sell=excluded.short_sell, short_buy=excluded.short_buy,
            short_redeem=excluded.short_redeem, short_balance=excluded.short_balance,
            offset=excluded.offset
    """, rows)
    conn.commit()

    logger.info(f"寫入 daily_stocks：{len(rows)} 筆（{date_str}）")
    return len(rows)


# ============================================================
#  主程式
# ============================================================

def sync_date(conn, dt, logger):
    """同步指定日期的所有盤後資料"""
    logger.info(f"===== 開始同步 {dt.strftime('%Y-%m-%d')} =====")
    t_start = time.time()

    # 1. 收盤行情（OHLCV）
    tse_ohlcv = fetch_tse_ohlcv(dt, logger)
    time.sleep(REQUEST_DELAY)
    otc_ohlcv = fetch_otc_ohlcv(dt, logger)
    time.sleep(REQUEST_DELAY)

    ohlcv = {**tse_ohlcv, **otc_ohlcv}
    if not ohlcv:
        logger.warning("無收盤行情資料，可能非交易日，跳過")
        return 0

    # 2. 三大法人
    tse_inst = fetch_tse_institutional(dt, logger)
    time.sleep(REQUEST_DELAY)
    otc_inst = fetch_otc_institutional(dt, logger)
    time.sleep(REQUEST_DELAY)

    institutional = {**tse_inst, **otc_inst}

    # 3. 融資融券
    tse_margin = fetch_tse_margin(dt, logger)
    time.sleep(REQUEST_DELAY)
    otc_margin = fetch_otc_margin(dt, logger)

    margin = {**tse_margin, **otc_margin}

    # 4. 合併寫入
    count = merge_and_write(conn, dt, ohlcv, institutional, margin, logger)

    elapsed = time.time() - t_start
    logger.info(f"===== 同步完成：{count} 檔，耗時 {elapsed:.1f} 秒 =====")
    return count


def main():
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info("[SYSTEM ] 盤後資料同步啟動")
    logger.info("=" * 50)

    # 決定要同步的日期
    if len(sys.argv) > 1:
        # 手動指定日期
        try:
            dt = datetime.strptime(sys.argv[1], '%Y-%m-%d')
            logger.info(f"手動指定日期：{sys.argv[1]}")
        except ValueError:
            logger.error(f"日期格式錯誤：{sys.argv[1]}（請用 YYYY-MM-DD）")
            sys.exit(1)
    else:
        # 預設：今天
        dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        # 週末自動跳過
        if dt.weekday() >= 5:
            logger.info("[SYSTEM ] 今天是週末，不執行")
            return

    conn = init_db()
    cleanup_old_daily(conn, logger)
    sync_date(conn, dt, logger)
    conn.close()
    logger.info("[SYSTEM ] 盤後同步結束")


if __name__ == "__main__":
    main()
