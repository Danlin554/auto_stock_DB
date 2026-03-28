"""
FB-Market 雲端排程服務
使用 APScheduler 在指定時間自動執行 main.py 和 postmarket_sync.py。

排程：
    - main.py:             週一~五 08:50（盤中收集，跑到 13:35 自動結束）
    - postmarket_sync.py:  週一~五 15:05（盤後個股資料，幾分鐘完成）

部署到 Zeabur 後持續運行，APScheduler 會在正確時間自動啟動腳本。
"""

import os
import sys
import signal
import subprocess
import logging
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ── 日誌設定 ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
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


def job_main():
    """盤中資料收集（08:50 啟動，跑到 13:35 自動結束）"""
    today = datetime.now()
    if today.weekday() >= 5:
        logger.info("今天是週末，跳過 main.py")
        return
    run_script('main.py', allow_segfault=True)


def job_postmarket():
    """盤後個股資料同步"""
    today = datetime.now()
    if today.weekday() >= 5:
        logger.info("今天是週末，跳過 postmarket_sync.py")
        return
    run_script('postmarket_sync.py')


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

    # 盤後同步：週一~五 15:05
    scheduler.add_job(
        job_postmarket,
        CronTrigger(day_of_week='mon-fri', hour=15, minute=5, timezone='Asia/Taipei'),
        id='postmarket_sync',
        name='盤後個股資料同步',
        misfire_grace_time=300,
    )

    # 列出排程
    jobs = scheduler.get_jobs()
    for job in jobs:
        logger.info(f"排程任務：{job.name} (id={job.id})")

    logger.info("排程服務已啟動，等待任務觸發...")

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
