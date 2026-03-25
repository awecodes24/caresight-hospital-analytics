

import logging
import os
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger(name: str, level: str = None) -> logging.Logger:
    level_str = level or os.getenv("LOG_LEVEL", "INFO")
    level_int = getattr(logging, level_str.upper(), logging.INFO)
    
    logging = logging.getLogger