from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DATABASE_URL = "postgresql://trader:trader@db:5432/tradingcloud"

def get_engine() -> Engine:
    return create_engine(DATABASE_URL)