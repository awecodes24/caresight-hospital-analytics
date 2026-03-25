
"""
job_scheduler.py

Automatically runs the ETL pipeline on a schedule.
"""

from apscheduler.schedulers.blocking import BlockingScheduler
from logging_monitoring.logger import setup_logger
from main import main

logger = setup_logger("scheduler")

scheduler = BlockingScheduler(timezone="UTC")

# ============================================================
#  JOB — What to run
# ============================================================

def run_etl():
    logger.info("=== ETL job starting ===")
    try:
        main()
        logger.info("=== ETL job complete ===")
    except Exception as e:
        logger.error(f"ETL job failed: {e}")

# ============================================================
#  SCHEDULE — When to run it
#  Cron format: minute hour day month day_of_week
#  "0 9 * * *" = every day at 9:00 AM UTC
# ============================================================

scheduler.add_job(
    func=run_etl,
    trigger="cron",
    hour=9,
    minute=0,
    id="etl_pipeline",
)

# ============================================================
#  START
# ============================================================

if __name__ == "__main__":
    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")
        scheduler.shutdown()