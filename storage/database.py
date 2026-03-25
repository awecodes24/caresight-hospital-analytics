from sqlalchemy import create_engine
from config.settings import DatabaseConfig

config = DatabaseConfig()

engine = create_engine(config.url)

with engine.connect() as conn:
    result = conn.execute("SELECT * FROM patients LIMIT 5;")
    
    for row in result:
        print(row)