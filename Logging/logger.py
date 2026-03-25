"""
logging_monitoring/logger.py

A simple logger that works for all 3 ETL stages:
    - Extract
    - Transform
    - Load

Each stage gets its own logger so you always know
WHERE in the pipeline a message came from.
"""

import logging
import os


# ─────────────────────────────────────
# Log format — what each line looks like
#
# Output example:
#   2024-03-15 09:32:11 | INFO     | [EXTRACT]   | Starting extraction...
#   2024-03-15 09:32:45 | ERROR    | [LOAD]      | Connection timed out!
# ─────────────────────────────────────

LOG_FORMAT  = "%(asctime)s | %(levelname)-8s | [%(name)-12s] | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logger(stage):
    """
    Call this at the top of each ETL file to get a logger for that stage.

    Args:
        stage (str): name of the ETL stage — "extract", "transform", or "load"

    Returns:
        A logger tagged with the stage name.

    How to use:
        logger = setup_logger("extract")
        logger.info("Extraction started")
        logger.warning("Missing value in column")
        logger.error("Could not connect!")
    """

    # create the logs/ folder if it doesn't already exist
    os.makedirs("logs", exist_ok=True)

    # create a logger named after the stage e.g. "EXTRACT", "TRANSFORM", "LOAD"
    logger = logging.getLogger(stage.upper())
    logger.setLevel(logging.DEBUG)

    # if this stage's logger is already set up, just return it
    # (prevents duplicate log lines if called more than once)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # Handler 1 — show logs in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2 — save logs to logs/etl.log
    file_handler = logging.FileHandler("logs/etl.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ─────────────────────────────────────
# QUICK CHEAT SHEET
#
#   logger.debug("Detailed info")     → for debugging only
#   logger.info("All good")           → normal progress messages
#   logger.warning("Something odd")   → unexpected but not breaking
#   logger.error("Something broke")   → actual error
#   logger.critical("Total failure")  → pipeline cannot continue
# ─────────────────────────────────────


# ─────────────────────────────────────
# HOW TO USE IN YOUR ETL FILES
# ─────────────────────────────────────

# ── In extract.py ──
# from logging_monitoring.logger import setup_logger
# logger = setup_logger("extract")
# logger.info("Connecting to source...")
# logger.info("Pulled 5000 rows")

# ── In transform.py ──
# from logging_monitoring.logger import setup_logger
# logger = setup_logger("transform")
# logger.info("Cleaning data...")
# logger.warning("Found nulls, filling with 0")

# ── In load.py ──
# from logging_monitoring.logger import setup_logger
# logger = setup_logger("load")
# logger.info("Loading into database...")
# logger.error("Connection timed out!")


# ─────────────────────────────────────
# DEMO — run this file to see it work:
#   python logger.py
# ─────────────────────────────────────

if __name__ == "__main__":

    # each stage gets its own logger
    extract_logger   = setup_logger("extract")
    transform_logger = setup_logger("transform")
    load_logger      = setup_logger("load")

    # EXTRACT stage logs
    extract_logger.info("Connecting to source database...")
    extract_logger.info("Pulled 5000 rows from orders table")
    extract_logger.warning("Column 'phone' is empty for 30 rows")

    # TRANSFORM stage logs
    transform_logger.info("Starting data cleaning...")
    transform_logger.warning("Found 10 null values — filling with 0")
    transform_logger.info("Transformation complete — 5000 rows ready")

    # LOAD stage logs
    load_logger.info("Loading data into warehouse...")
    load_logger.error("Connection timed out — retrying...")
    load_logger.info("Retry successful")
    load_logger.info("Load complete — 4990 rows inserted")