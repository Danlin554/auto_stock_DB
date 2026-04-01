"""
FB-Market 雲端排程服務
使用 APScheduler 在指定時間自動執行 main.py 和 postmarket_sync.py。

排程：
    - main.py:             週一~五 08:50（盤中收集，跑到 13:35 自動結束）
    - postmarket_sync.py:  週一~五 21:00（盤後個股資料，幾分鐘完成）
    - backfill_history.py: 緊接 postmarket_sync 後自動執行，回填缺漏的 daily_closing

部署到 Zeabur 後持續運行，APScheduler 會在正確時間自動啟動腳本。
"""

import os
import sys
import signal
import subprocess
import logging
import threading
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ── 日誌設定 ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout,
)
logger = logging.getLogger('scheduler')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ── 執行腳本 ──────────────────────────────────────────────────

def run_script(script_name, args=None, allow_segfault=False):
    """
    以子程序執行指定 Python 腳本。
    allow_segfault: 若為 True，exit code 139（SDK segfault）視為正常。
    """
    cmd = [sys.executable, os.path.join(BASE_DIR, script_name)]
    if args:
        cmd.extend(args)

    logger.info(f"啟動 {script_name}...")
    try:
        result = subprocess.run(cmd, cwd=BASE_DIR)
        code = result.returncode

        if code == 0:
            logger.info(f"{script_name} 正常結束 (exit 0)")
        elif allow_segfault and code in (-11, 139):
            # SDK 正常結束時會 segfault，exit code 139 或 -11
            logger.info(f"{script_name} 結束 (exit {code}, SDK segfault 屬正常現象)")
        else:
            logger.error(f"{script_name} 異常結束 (exit {code})")
    except Exception as e:
        logger.error(f"執行 {script_name} 發生例外：{e}")


def check_and_recover():
    """
    服務啟動時的補救機制：
    若目前處於開盤時間（08:50~13:35）且今天 DB 尚無快照資料，立即補跑 main.py。
    以獨立執行緒執行，不阻塞排程器啟動。
    """
    now = datetime.now()

    # 只在週一到週五
    if now.weekday() >= 5:
        logger.info("[補救] 今天是週末，略過補救檢查")
        return

    # 只在開盤時間內
    market_start = now.replace(hour=8, minute=50, second=0, microsecond=0)
    market_end   = now.replace(hour=13, minute=35, second=0, microsecond=0)
    if not (market_start <= now <= market_end):
        logger.info(f"[補救] 目前時間 {now.strftime('%H:%M')} 不在開盤時段，略過補救檢查")
        return

    # 檢查今天是否已有快照資料
    try:
        sys.path.insert(0, BASE_DIR)
        from lib.db import get_connection, qone
        conn = get_connection()
        today_str = now.strftime('%Y-%m-%d')
        row = qone(conn, "SELECT COUNT(*) FROM raw_snapshots WHERE snapshot_time >= %s", (today_str,))
        conn.close()
        count = row[0] if row else 0
        if count > 0:
            logger.info(f"[補救] 今天已有 {count} 筆快照資料，無需補跑")
            return
    except Exception as e:
        logger.error(f"[補救] 檢查今日快照資料失敗：{e}，跳過補救")
        return

    logger.info("[補救] 開盤中但今天無資料，立即補跑 main.py...")
    run_script('main.py', allow_segfault=True)


def job_main():
    """盤中資料收集（08:50 啟動，跑到 13:35 自動結束）"""
    today = datetime.now()
    if today.weekday() >= 5:
        logger.info("今天是週末，跳過 main.py")
        return
    run_script('main.py', allow_segfault=True)


def job_postmarket():
    """盤後個股資料同步 + 自動回填 daily_closing"""
    today = datetime.now()
    if today.weekday() >= 5:
        logger.info("今天是週末，跳過 postmarket_sync.py")
        return
    run_script('postmarket_sync.py')
    # postmarket_sync 寫入 daily_stocks 後，自動回填缺漏的 daily_closing
    logger.info("開始回填 daily_closing（從 daily_stocks 計算）...")
    run_script('backfill_history.py', args=['--stats-only', '--fill-rolling'])


# ── 主程式 ────────────────────────────────────────────────────

def main():
    logger.info("=" * 50)
    logger.info("FB-Market 雲端排程服務啟動")
    logger.info(f"時區：{os.environ.get('TZ', 'system default')}")
    logger.info(f"目前時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    scheduler = BlockingScheduler(timezone='Asia/Taipei')

    # 盤中收集：週一~五 08:50
    scheduler.add_job(
        job_main,
        CronTrigger(day_of_week='mon-fri', hour=8, minute=50, timezone='Asia/Taipei'),
        id='main_intraday',
        name='盤中資料收集',
        misfire_grace_time=300,  # 允許延遲 5 分鐘內仍執行
    )

    # 盤後同步：週一~五 21:00
    scheduler.add_job(
        job_postmarket,
        CronTrigger(day_of_week='mon-fri', hour=21, minute=0, timezone='Asia/Taipei'),
        id='postmarket_sync',
        name='盤後個股資料同步',
        misfire_grace_time=300,
    )

    # 列出排程
    jobs = scheduler.get_jobs()
    for job in jobs:
        logger.info(f"排程任務：{job.name} (id={job.id})")

    logger.info("排程服務已啟動，等待任務觸發...")

    # 啟動時補救：若在開盤時間且今天無資料，立即補跑
    t = threading.Thread(target=check_and_recover, daemon=True)
    t.start()

    # 優雅關閉
    def shutdown(signum, frame):
        logger.info("收到關閉信號，正在停止排程...")
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("排程服務已停止")


if __name__ == '__main__':
    main()
