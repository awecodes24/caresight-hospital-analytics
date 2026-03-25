from sqlalchemy import create_engine
from config.settings import DatabaseConfig

def get_connection():
    config = DatabaseConfig()
    engine = create_engine(
        config.url,
        connect_args={"host": "localhost"}  # force TCP on Windows
    )
    return engine