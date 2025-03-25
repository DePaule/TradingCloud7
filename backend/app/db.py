from sqlalchemy import create_engine

DATABASE_URL = "postgresql://trader:trader@db:5432/tradingcloud"

def get_engine():
    return create_engine(DATABASE_URL, future=True)
