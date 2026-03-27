"""
資料遷移腳本：SQLite → PostgreSQL
從本機 data/market.db 讀取歷史資料，寫入 Zeabur PostgreSQL。

使用方式：
    export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
    venv/bin/python migrate_data.py

遷移目標：
    - daily_closing  （全部，約 2,000+ 天）
    - daily_stocks   （全部，約 440 萬筆，需時較長）

略過（暫時性資料，雲端重新收集）：
    - raw_snapshots
    - computed_stats
    - daily_summary
"""

import os
import sys
import sqlite3
import time
import psycopg2
import psycopg2.extras
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(BASE_DIR, 'data', 'market.db')

sys.path.insert(0, BASE_DIR)
from lib.db import get_connection, init_all_tables


# PostgreSQL 保留字清單（欄位名稱衝突時需加引號）
_PG_RESERVED = {'offset', 'order', 'limit', 'group', 'table', 'index', 'type'}

def _qcol(c):
    """如果欄位名稱是保留字，加雙引號"""
    return f'"{c}"' if c.lower() in _PG_RESERVED else c


# ── 工具 ─────────────────────────────────────────────────────

def progress(current, total, label=''):
    pct = current / total * 100
    bar = '█' * int(pct / 2) + '░' * (50 - int(pct / 2))
    print(f'\r  [{bar}] {pct:5.1f}%  {current:,}/{total:,}  {label}', end='', flush=True)


def batch_insert(pg_conn, table, rows, cols, on_conflict):
    """用 psycopg2.extras.execute_values 批次寫入（速度最快）"""
    col_str = ', '.join(cols)
    sql = f"INSERT INTO {table} ({col_str}) VALUES %s {on_conflict}"
    with pg_conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    pg_conn.commit()


# ── 各表遷移 ─────────────────────────────────────────────────

def migrate_daily_closing(sqlite_conn, pg_conn):
    print('\n[1/2] 遷移 daily_closing...')
    df = pd.read_sql_query("SELECT * FROM daily_closing ORDER BY date", sqlite_conn)
    if df.empty:
        print('  daily_closing 無資料，跳過')
        return 0

    # 確認欄位（以 SQLite 實際欄位為準，id 欄位不遷移）
    cols = [c for c in df.columns if c != 'id']
    # int64 超過 INTEGER 上限的欄位轉 float（避免 psycopg2 整數溢出）
    _INT_MAX = 2_147_483_647
    for col in cols:
        if str(df[col].dtype) == 'int64' and df[col].abs().max() > _INT_MAX:
            df[col] = df[col].astype(float)
    # NaN → None（psycopg2 只接受 None 作為 NULL）
    rows = [
        tuple(None if (v != v) else v for v in row)
        for row in df[cols].itertuples(index=False)
    ]

    col_str = ', '.join(_qcol(c) for c in cols)
    update_set = ', '.join(f"{_qcol(c)}=EXCLUDED.{_qcol(c)}" for c in cols if c != 'date')
    on_conflict = f"ON CONFLICT (date) DO UPDATE SET {update_set}"

    BATCH = 500
    total = len(rows)
    done = 0
    for i in range(0, total, BATCH):
        batch = rows[i:i+BATCH]
        sql = f"INSERT INTO daily_closing ({col_str}) VALUES %s {on_conflict}"
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, batch, page_size=BATCH)
        pg_conn.commit()
        done += len(batch)
        progress(done, total)

    print(f'\n  完成：{total:,} 筆')
    return total


def migrate_daily_stocks(sqlite_conn, pg_conn):
    print('\n[2/2] 遷移 daily_stocks（資料量大，請耐心等候）...')

    # 先取得 SQLite 的總筆數
    total = sqlite_conn.execute("SELECT COUNT(*) FROM daily_stocks").fetchone()[0]
    if total == 0:
        print('  daily_stocks 無資料，跳過')
        return 0

    print(f'  總計 {total:,} 筆，分批讀取寫入...')

    # 取得 PostgreSQL 已有的日期（支援續傳）
    with pg_conn.cursor() as cur:
        cur.execute("SELECT DISTINCT date FROM daily_stocks ORDER BY date")
        pg_dates = {r[0] for r in cur.fetchall()}

    if pg_dates:
        print(f'  PostgreSQL 已有 {len(pg_dates)} 個日期，將跳過已存在的日期（續傳模式）')

    # 取所有 SQLite 日期
    all_dates = [r[0] for r in sqlite_conn.execute(
        "SELECT DISTINCT date FROM daily_stocks ORDER BY date"
    ).fetchall()]

    need_dates = [d for d in all_dates if d not in pg_dates]
    if not need_dates:
        print('  所有日期已存在於 PostgreSQL，跳過')
        return 0

    print(f'  需要遷移 {len(need_dates)} 個日期（{need_dates[0]} ~ {need_dates[-1]}）')

    # 取得欄位名稱
    sample = pd.read_sql_query(
        "SELECT * FROM daily_stocks WHERE date = ? LIMIT 1",
        sqlite_conn, params=(need_dates[0],)
    )
    cols = [c for c in sample.columns if c != 'id']
    col_str = ', '.join(_qcol(c) for c in cols)
    update_set = ', '.join(
        f"{_qcol(c)}=EXCLUDED.{_qcol(c)}" for c in cols if c not in ('date', 'symbol')
    )
    on_conflict = f"ON CONFLICT (date, symbol) DO UPDATE SET {update_set}"
    sql = f"INSERT INTO daily_stocks ({col_str}) VALUES %s {on_conflict}"

    done = 0
    t_start = time.time()
    for i, d in enumerate(need_dates):
        df = pd.read_sql_query(
            "SELECT * FROM daily_stocks WHERE date = ?",
            sqlite_conn, params=(d,)
        )
        df = df[[c for c in df.columns if c != 'id']]
        rows = [
            tuple(None if (v != v) else v for v in row)
            for row in df.itertuples(index=False)
        ]
        if rows:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
            pg_conn.commit()
        done += len(rows)
        elapsed = time.time() - t_start
        speed = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / speed if speed > 0 else 0
        progress(i+1, len(need_dates),
                 f'{d}  {speed:,.0f}筆/s  ETA {eta/60:.0f}分')

    print(f'\n  完成：{done:,} 筆')
    return done


# ── 主程式 ───────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('FB-Market 資料遷移：SQLite → PostgreSQL')
    print('=' * 60)

    if not os.path.exists(SQLITE_PATH):
        print(f'錯誤：找不到 SQLite 資料庫：{SQLITE_PATH}')
        sys.exit(1)

    if not os.environ.get('DATABASE_URL'):
        print('錯誤：請先設定 DATABASE_URL 環境變數')
        print("  export DATABASE_URL='postgresql://user:pass@host:5432/dbname'")
        sys.exit(1)

    print(f'\nSQLite：{SQLITE_PATH}')
    print(f'PostgreSQL：{os.environ["DATABASE_URL"][:40]}...')

    # 開啟連線
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    pg_conn = get_connection()

    # 先刪除空資料表（讓 BIGINT DDL 修正生效），再重建
    print('\n清除並重建 PostgreSQL 資料表...')
    with pg_conn.cursor() as cur:
        for tbl in ['daily_closing', 'daily_stocks', 'computed_stats',
                    'raw_snapshots', 'daily_summary']:
            cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
    pg_conn.commit()
    init_all_tables(pg_conn)
    print('  完成')

    t0 = time.time()
    n1 = migrate_daily_closing(sqlite_conn, pg_conn)
    n2 = migrate_daily_stocks(sqlite_conn, pg_conn)

    elapsed = time.time() - t0
    print(f'\n{"=" * 60}')
    print(f'遷移完成！耗時 {elapsed/60:.1f} 分鐘')
    print(f'  daily_closing : {n1:,} 筆')
    print(f'  daily_stocks  : {n2:,} 筆')
    print(f'{"=" * 60}')

    sqlite_conn.close()
    pg_conn.close()


if __name__ == '__main__':
    main()
