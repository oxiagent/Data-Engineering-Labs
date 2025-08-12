import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

def get_engine() -> Engine:
    user = os.getenv("MYSQL_USER", "root")
    pwd = os.getenv("MYSQL_PASSWORD", "root")
    host = os.getenv("MYSQL_HOST", "mysql")
    port = os.getenv("MYSQL_PORT", "3306")
    db   = os.getenv("MYSQL_DATABASE", "analytics_db")
    url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)
