"""
PostgreSQL 共用資料庫模組
本機執行：需設定 DATABASE_URL 環境變數
Zeabur 部署：DATABASE_URL 由 Zeabur 自動注入
"""
import os
import psycopg2
import psycopg2.extras
import pandas as pd


def get_connection():
    """取得 PostgreSQL 連線，並設定時區為台北

    依序嘗試以下環境變數（支援 Zeabur 自動注入的各種名稱）：
      DATABASE_URL / POSTGRES_URI / POSTGRESQL_URL /
      ZEABUR_POSTGRESQL_CONNECTION_STRING
    若以上皆無，嘗試用個別欄位（PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD）組合。
    """
    # ── 1. 嘗試 connection string 形式的環境變數 ──────────
    _URL_VARS = [
        'DATABASE_URL',
        'POSTGRES_URI',
        'POSTGRESQL_URL',
        'ZEABUR_POSTGRESQL_CONNECTION_STRING',
    ]
    url = None
    for var in _URL_VARS:
        url = os.environ.get(var)
        if url:
            break

    # ── 2. 嘗試個別欄位組合（Zeabur 有時用 PGHOST 等注入）─
    if not url:
        host = os.environ.get('PGHOST') or os.environ.get('POSTGRES_HOST')
        port = os.environ.get('PGPORT') or os.environ.get('POSTGRES_PORT') or '5432'
        db   = os.environ.get('PGDATABASE') or os.environ.get('POSTGRES_DB') or os.environ.get('POSTGRES_DATABASE')
        user = os.environ.get('PGUSER') or os.environ.get('POSTGRES_USER') or os.environ.get('POSTGRES_USERNAME')
        pwd  = os.environ.get('PGPASSWORD') or os.environ.get('POSTGRES_PASSWORD')
        if host and db and user:
            url = f"postgresql://{user}:{pwd}@{host}:{port}/{db}"

    if not url:
        checked = ', '.join(_URL_VARS) + ', PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD'
        raise RuntimeError(
            "無法連線資料庫：找不到 PostgreSQL 連線設定。\n"
            f"已檢查環境變數：{checked}\n"
            "本機請先設定：export DATABASE_URL='postgresql://user:pass@host:5432/dbname'"
        )

    conn = psycopg2.connect(url)
    with conn.cursor() as cur:
        cur.execute("SET timezone = 'Asia/Taipei'")
    conn.commit()
    return conn


def ensure_columns(conn, table_name, column_defs):
    """
    確保資料表有指定欄位，不存在則新增（替代 SQLite PRAGMA table_info）。
    column_defs: list of "column_name TYPE" strings
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, (table_name,))
        existing = {row[0] for row in cur.fetchall()}
    with conn.cursor() as cur:
        for col_def in column_defs:
            col_name = col_def.split()[0]
            if col_name not in existing:
                cur.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_def}"
                )
    conn.commit()


def read_sql(sql, conn, params=None):
    """
    pd.read_sql_query 的替代函式（避免 pandas DBAPI2 棄用警告）。
    SQL 佔位符請使用 %s（psycopg2 pyformat 格式）。
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return pd.DataFrame()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def qone(conn, sql, params=None):
    """Execute SQL，回傳第一筆 row（tuple）"""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def qall(conn, sql, params=None):
    """Execute SQL，回傳所有 rows"""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def qexec(conn, sql, params=None):
    """Execute SQL，回傳 cursor（可讀 .rowcount）"""
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur


def qmany(conn, sql, rows):
    """executemany，回傳 cursor"""
    cur = conn.cursor()
    cur.executemany(sql, rows)
    return cur


def init_all_tables(conn):
    """建立所有 PostgreSQL 資料表（幂等，已存在不影響）"""
    ddl_list = [
        # ── 第一層：原始快照 ──────────────────────────
        """CREATE TABLE IF NOT EXISTS raw_snapshots (
            id              SERIAL PRIMARY KEY,
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
        )""",
        "CREATE INDEX IF NOT EXISTS idx_raw_time   ON raw_snapshots(snapshot_time)",
        "CREATE INDEX IF NOT EXISTS idx_raw_symbol ON raw_snapshots(symbol)",

        # ── 第二層：彙總統計（每 15 秒一筆）─────────
        """CREATE TABLE IF NOT EXISTS computed_stats (
            id                        SERIAL PRIMARY KEY,
            snapshot_time             TEXT NOT NULL,
            filtered_total            INTEGER,
            up_count                  INTEGER,
            down_count                INTEGER,
            flat_count                INTEGER,
            red_k_count               INTEGER,
            black_k_count             INTEGER,
            flat_k_count              INTEGER,
            above_5pct_count          INTEGER,
            tse_up_count              INTEGER,
            otc_up_count              INTEGER,
            total_trade_value         REAL,
            total_trade_volume        BIGINT,
            sentiment_index           REAL,
            ad_ratio                  REAL,
            volatility                REAL,
            strength_index            REAL,
            activity_rate             REAL,
            bucket_up_2_5             INTEGER,
            bucket_up_5               INTEGER,
            bucket_up_7_5             INTEGER,
            bucket_up_above           INTEGER,
            bucket_down_2_5           INTEGER,
            bucket_down_5             INTEGER,
            bucket_down_7_5           INTEGER,
            bucket_down_above         INTEGER,
            advantage_count           INTEGER,
            strong_count              INTEGER,
            super_strong_count        INTEGER,
            near_limit_up_count       INTEGER,
            disadvantage_count        INTEGER,
            weak_count                INTEGER,
            super_weak_count          INTEGER,
            near_limit_down_count     INTEGER,
            prev_strong_count         INTEGER,
            prev_strong_avg_today     REAL,
            prev_strong_positive_rate REAL,
            prev_weak_count           INTEGER,
            prev_weak_avg_today       REAL,
            prev_weak_negative_rate   REAL,
            top_n_avg                 REAL,
            bottom_n_avg              REAL,
            blue_chip_up_count        INTEGER,
            blue_chip_total           INTEGER,
            blue_chip_avg_change      REAL,
            volume_tide_up_value      REAL,
            volume_tide_down_value    REAL,
            volume_tide_net           REAL,
            volume_tide_up_pct        REAL,
            volume_tide_down_pct      REAL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_computed_time ON computed_stats(snapshot_time)",

        # ── 第三層：每日摘要 ──────────────────────────
        """CREATE TABLE IF NOT EXISTS daily_summary (
            id                    SERIAL PRIMARY KEY,
            date                  TEXT NOT NULL UNIQUE,
            market_open_time      TEXT,
            market_close_time     TEXT,
            total_snapshots       INTEGER,
            total_stocks          INTEGER,
            open_snapshot_time    TEXT,
            open_gap_up_count     INTEGER,
            open_gap_down_count   INTEGER,
            open_flat_count       INTEGER,
            open_valid_count      INTEGER,
            mid_30min_up_count    INTEGER,
            mid_30min_down_count  INTEGER,
            pre_close_up_count    INTEGER,
            pre_close_down_count  INTEGER,
            close_up_count        INTEGER,
            close_down_count      INTEGER,
            close_flat_count      INTEGER,
            close_tse_up          INTEGER,
            close_otc_up          INTEGER,
            max_up_count          INTEGER,
            max_up_count_time     TEXT,
            min_up_count          INTEGER,
            min_up_count_time     TEXT,
            total_amount          REAL,
            total_volume          BIGINT,
            tse_amount            REAL,
            otc_amount            REAL,
            prev_day_amount       REAL,
            amount_ratio          REAL,
            advance_decline_ratio REAL,
            sentiment_label       TEXT,
            note                  TEXT
        )""",

        # ── 每日收盤指標（永久保存）───────────────────
        """CREATE TABLE IF NOT EXISTS daily_closing (
            id                        SERIAL PRIMARY KEY,
            date                      TEXT NOT NULL UNIQUE,
            filtered_total            INTEGER,
            up_count                  INTEGER,
            down_count                INTEGER,
            flat_count                INTEGER,
            red_k_count               INTEGER,
            black_k_count             INTEGER,
            flat_k_count              INTEGER,
            tse_up_count              INTEGER,
            otc_up_count              INTEGER,
            total_trade_value         REAL,
            total_trade_volume        BIGINT,
            sentiment_index           REAL,
            ad_ratio                  REAL,
            volatility                REAL,
            strength_index            REAL,
            activity_rate             REAL,
            bucket_up_2_5             INTEGER,
            bucket_up_5               INTEGER,
            bucket_up_7_5             INTEGER,
            bucket_up_above           INTEGER,
            bucket_down_2_5           INTEGER,
            bucket_down_5             INTEGER,
            bucket_down_7_5           INTEGER,
            bucket_down_above         INTEGER,
            advantage_count           INTEGER,
            strong_count              INTEGER,
            super_strong_count        INTEGER,
            near_limit_up_count       INTEGER,
            disadvantage_count        INTEGER,
            weak_count                INTEGER,
            super_weak_count          INTEGER,
            near_limit_down_count     INTEGER,
            prev_strong_count         INTEGER,
            prev_strong_avg_today     REAL,
            prev_strong_positive_rate REAL,
            prev_weak_count           INTEGER,
            prev_weak_avg_today       REAL,
            prev_weak_negative_rate   REAL,
            top_n_avg                 REAL,
            bottom_n_avg              REAL,
            blue_chip_up_count        INTEGER,
            blue_chip_total           INTEGER,
            blue_chip_avg_change      REAL,
            volume_tide_up_value      REAL,
            volume_tide_down_value    REAL,
            volume_tide_net           REAL,
            volume_tide_up_pct        REAL,
            volume_tide_down_pct      REAL,
            above_5pct_count          INTEGER,
            new_high_20d_count        INTEGER,
            new_low_20d_count         INTEGER,
            above_5ma_count           INTEGER,
            above_20ma_count          INTEGER,
            above_60ma_count          INTEGER,
            above_5ma_pct             REAL,
            above_20ma_pct            REAL,
            above_60ma_pct            REAL,
            margin_maintenance_rate   REAL
        )""",
        "CREATE INDEX IF NOT EXISTS idx_daily_closing_date ON daily_closing(date)",

        # ── 盤後每日個股資料 ──────────────────────────
        """CREATE TABLE IF NOT EXISTS daily_stocks (
            id              SERIAL PRIMARY KEY,
            date            TEXT NOT NULL,
            market          TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            name            TEXT,
            open_price      REAL,
            high_price      REAL,
            low_price       REAL,
            close_price     REAL,
            trade_volume    INTEGER,
            trade_value     REAL,
            trade_count     INTEGER,
            foreign_buy     INTEGER,
            foreign_sell    INTEGER,
            foreign_net     INTEGER,
            trust_buy       INTEGER,
            trust_sell      INTEGER,
            trust_net       INTEGER,
            dealer_net      INTEGER,
            inst_total_net  INTEGER,
            margin_buy      INTEGER,
            margin_sell     INTEGER,
            margin_redeem   INTEGER,
            margin_balance  INTEGER,
            short_sell      INTEGER,
            short_buy       INTEGER,
            short_redeem    INTEGER,
            short_balance   INTEGER,
            "offset"        INTEGER,
            UNIQUE(date, symbol)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_daily_stocks_date   ON daily_stocks(date)",
        "CREATE INDEX IF NOT EXISTS idx_daily_stocks_symbol ON daily_stocks(symbol)",
    ]

    with conn.cursor() as cur:
        for stmt in ddl_list:
            cur.execute(stmt)
    conn.commit()
