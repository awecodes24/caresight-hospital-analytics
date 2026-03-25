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
from datetime import datetime

# ─────────────────────────────────────
# Log format — what each line looks like
#
# Output example:
#   2024-03-15 09:32:11 | INFO     | [EXTRACT]   | Starting extraction...
#   2024-03-15 09:32:45 | ERROR    | [LOAD]      | Connection timed out!
# ─────────────────────────────────────

LOG_FORMAT  = "%(asctime)s | %(levelname)-8s | [%(name)-12s] | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def log_run_start(logger):
    logger.info("\n" + "="*60)
    logger.info(f"NEW PIPELINE RUN STARTED | {datetime.now()}")
    logger.info("="*60)


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
    run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = f"etl_{run_id}.log"

    # create a logger named after the stage e.g. "EXTRACT", "TRANSFORM", "LOAD"
    logger = logging.getLogger(stage.upper())
    logger.setLevel(logging.DEBUG)

    # if this stage's logger is already set up, just return it
    # (prevents duplicate log lines if called more than once)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    #show logs in the terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    #save logs to logs/etl.log
    file_handler = logging.FileHandler(f"logs/{log_file}", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# just name  formatter for logged ETL
def log_section(logger, title):
    logger.info("=" * 50)
    logger.info(f"[{title.upper()}]")
    logger.info("=" * 50)
    
