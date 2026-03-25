from sqlalchemy import create_engine
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "caresight.db"

def get_connection():
    engine = create_engine(f"sqlite:///{DB_PATH}")
    return engine