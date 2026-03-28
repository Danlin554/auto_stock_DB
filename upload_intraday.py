"""
上傳盤中資料腳本：本機 SQLite → 雲端 PostgreSQL
讀取指定日期的 computed_stats 和 raw_snapshots，寫入 Zeabur PostgreSQL。

使用方式：
    export DATABASE_URL='postgresql://user:pass@host:5432/dbname'
    venv/bin/python upload_intraday.py [YYYY-MM-DD]

若不指定日期，自動使用 SQLite 內最新的一天。
"""

import os
import sys
import sqlite3
import time
import psycopg2.extras
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(BASE_DIR, 'data', 'market.db')

sys.path.insert(0, BASE_DIR)
from lib.db import get_connection, init_all_tables, qone


_PG_RESERVED = {'offset', 'order', 'limit', 'group', 'table', 'index', 'type'}

def _qcol(c):
    return f'"{c}"' if c.lower() in _PG_RESERVED else c


def progress(current, total, label=''):
    pct = current / total * 100
    bar = '█' * int(pct / 2) + '░' * (50 - int(pct / 2))
    print(f'\r  [{bar}] {pct:5.1f}%  {current:,}/{total:,}  {label}', end='', flush=True)


def to_rows(df, cols):
    """DataFrame → list of tuples，NaN 轉 None"""
    return [
        tuple(None if (v != v) else v for v in row)
        for row in df[cols].itertuples(index=False)
    ]


# ── computed_stats ────────────────────────────────────────────

def upload_computed_stats(sqlite_conn, pg_conn, date_str):
    print(f'\n[1/2] 上傳 computed_stats（{date_str}）...')

    # 先確認雲端有無資料
    existing = qone(pg_conn,
        "SELECT COUNT(*) FROM computed_stats WHERE snapshot_time LIKE %s",
        (f"{date_str}%",)
    )[0]
    if existing > 0:
        print(f'  雲端已有 {existing:,} 筆，先清除再重新上傳...')
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM computed_stats WHERE snapshot_time LIKE %s", (f"{date_str}%",))
        pg_conn.commit()

    df = pd.read_sql_query(
        "SELECT * FROM computed_stats WHERE snapshot_time LIKE ? ORDER BY snapshot_time",
        sqlite_conn, params=(f"{date_str}%",)
    )
    if df.empty:
        print('  本機無資料，跳過')
        return 0

    cols = [c for c in df.columns if c != 'id']
    col_str = ', '.join(_qcol(c) for c in cols)
    sql = f"INSERT INTO computed_stats ({col_str}) VALUES %s"

    rows = to_rows(df, cols)
    total = len(rows)
    BATCH = 200
    done = 0
    for i in range(0, total, BATCH):
        batch = rows[i:i+BATCH]
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, batch, page_size=BATCH)
        pg_conn.commit()
        done += len(batch)
        progress(done, total)

    print(f'\n  完成：{total:,} 筆')
    return total


# ── raw_snapshots ─────────────────────────────────────────────

def upload_raw_snapshots(sqlite_conn, pg_conn, date_str):
    print(f'\n[2/2] 上傳 raw_snapshots（{date_str}）...')

    # 先確認雲端有無資料
    existing = qone(pg_conn,
        "SELECT COUNT(*) FROM raw_snapshots WHERE snapshot_time LIKE %s",
        (f"{date_str}%",)
    )[0]
    if existing > 0:
        print(f'  雲端已有 {existing:,} 筆，先清除再重新上傳...')
        with pg_conn.cursor() as cur:
            cur.execute("DELETE FROM raw_snapshots WHERE snapshot_time LIKE %s", (f"{date_str}%",))
        pg_conn.commit()

    total = sqlite_conn.execute(
        "SELECT COUNT(*) FROM raw_snapshots WHERE snapshot_time LIKE ?",
        (f"{date_str}%",)
    ).fetchone()[0]

    if total == 0:
        print('  本機無資料，跳過')
        return 0

    print(f'  總計 {total:,} 筆，分批上傳中...')

    # 取欄位名稱
    cur = sqlite_conn.cursor()
    cur.execute("SELECT * FROM raw_snapshots WHERE snapshot_time LIKE ? LIMIT 1", (f"{date_str}%",))
    col_names = [d[0] for d in cur.description]
    cols = [c for c in col_names if c != 'id']

    col_str = ', '.join(_qcol(c) for c in cols)
    sql = f"INSERT INTO raw_snapshots ({col_str}) VALUES %s"

    BATCH = 2000
    done = 0
    t_start = time.time()

    sqlite_cur = sqlite_conn.cursor()
    sqlite_cur.execute(
        f"SELECT {', '.join(cols)} FROM raw_snapshots WHERE snapshot_time LIKE ? ORDER BY snapshot_time",
        (f"{date_str}%",)
    )

    while True:
        chunk = sqlite_cur.fetchmany(BATCH)
        if not chunk:
            break
        rows = [tuple(None if (v != v) else v for v in row) for row in chunk]
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH)
        pg_conn.commit()
        done += len(rows)
        elapsed = time.time() - t_start
        speed = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / speed if speed > 0 else 0
        progress(done, total, f'{speed:,.0f}筆/s  ETA {eta/60:.1f}分')

    print(f'\n  完成：{done:,} 筆')
    return done


# ── 主程式 ─────────────────────────────────────────────────────

def main():
    print('=' * 60)
    print('FB-Market 盤中資料上傳：SQLite → PostgreSQL')
    print('=' * 60)

    if not os.path.exists(SQLITE_PATH):
        print(f'錯誤：找不到 SQLite：{SQLITE_PATH}')
        sys.exit(1)

    if not os.environ.get('DATABASE_URL'):
        for v in ('POSTGRES_URI', 'POSTGRESQL_URL'):
            if os.environ.get(v):
                os.environ['DATABASE_URL'] = os.environ[v]
                break
        else:
            print('錯誤：請先設定 DATABASE_URL 環境變數')
            sys.exit(1)

    sqlite_conn = sqlite3.connect(SQLITE_PATH)

    # 決定日期
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        row = sqlite_conn.execute(
            "SELECT substr(MAX(snapshot_time),1,10) FROM computed_stats"
        ).fetchone()
        date_str = row[0] if row and row[0] else None
        if not date_str:
            print('錯誤：SQLite computed_stats 無資料')
            sys.exit(1)
        print(f'自動偵測最新日期：{date_str}')

    print(f'目標日期：{date_str}')
    print(f'PostgreSQL：{os.environ["DATABASE_URL"][:50]}...')

    pg_conn = get_connection()
    init_all_tables(pg_conn)

    t0 = time.time()
    n1 = upload_computed_stats(sqlite_conn, pg_conn, date_str)
    n2 = upload_raw_snapshots(sqlite_conn, pg_conn, date_str)

    elapsed = time.time() - t0
    print(f'\n{"=" * 60}')
    print(f'上傳完成！耗時 {elapsed/60:.1f} 分鐘')
    print(f'  computed_stats : {n1:,} 筆')
    print(f'  raw_snapshots  : {n2:,} 筆')
    print(f'{"=" * 60}')

    sqlite_conn.close()
    pg_conn.close()


if __name__ == '__main__':
    main()
