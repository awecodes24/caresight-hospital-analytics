
import psycopg2
from config.settings import DatabaseConfig

def get_connection():
    config = DatabaseConfig()
    
    conn = psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.name,
        user=config.user,
        password=config.password
    )
    
    return conn