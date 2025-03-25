# TradingCloud7/backend/app/db.py
import os
import datetime
from sqlalchemy import create_engine, MetaData, Table, Column, Float, String, DateTime, text
from sqlalchemy.engine import Engine

# DATABASE_URL z. B. "postgresql://trader:trader@db:5432/tradingcloud"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

_engine = None

def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL)
    return _engine

def create_table_if_not_exists(engine: Engine, table_name: str):
    metadata = MetaData()
    # Tabellenstruktur für die aggregierten Candles (plus Platzhalterfelder)
    Table(
        table_name, metadata,
        Column("instrument", String),
        Column("timestamp", DateTime, primary_key=True),
        Column("bid_open", Float),
        Column("bid_high", Float),
        Column("bid_low", Float),
        Column("bid_close", Float),
        Column("ask_open", Float),
        Column("ask_high", Float),
        Column("ask_low", Float),
        Column("ask_close", Float),
        Column("bid_volume", Float),
        Column("ask_volume", Float),
        Column("avg_spread", Float),
        Column("first_bid_1", Float),
        Column("first_bid_2", Float),
        Column("first_bid_3", Float),
        Column("first_ask_1", Float),
        Column("first_ask_2", Float),
        Column("first_ask_3", Float),
    )
    metadata.create_all(engine, checkfirst=True)

def fetch_existing_data(engine: Engine, table_name: str, start_date: datetime.date, end_date: datetime.date):
    with engine.connect() as conn:
        query = text(f"""
            SELECT * FROM {table_name}
            WHERE timestamp >= :start AND timestamp < :end
            ORDER BY timestamp ASC
        """)
        start_dt = datetime.datetime.combine(start_date, datetime.time.min)
        end_dt = datetime.datetime.combine(end_date, datetime.time.max)
        result = conn.execute(query, {"start": start_dt, "end": end_dt})
        columns = result.keys()
        data = [dict(zip(columns, row)) for row in result.fetchall()]
    return data

def insert_data(engine: Engine, table_name: str, ticks: list, asset: str) -> int:
    """
    Fügt die heruntergeladenen Tickdaten in die angegebene Tabelle ein.
    Jeder Tick ist ein Tupel: (timestamp, bid, ask, bid_volume, ask_volume)
    Es wird ON CONFLICT DO NOTHING genutzt, um Duplikate zu vermeiden.
    """
    insert_sql = text(f"""
    INSERT INTO {table_name} (
        instrument, timestamp, bid_open, bid_high, bid_low, bid_close,
        ask_open, ask_high, ask_low, ask_close, bid_volume, ask_volume,
        avg_spread, first_bid_1, first_bid_2, first_bid_3, first_ask_1, first_ask_2, first_ask_3
    ) VALUES (
        :instrument, :timestamp, :bid_open, :bid_high, :bid_low, :bid_close,
        :ask_open, :ask_high, :ask_low, :ask_close, :bid_volume, :ask_volume,
        :avg_spread, :first_bid_1, :first_bid_2, :first_bid_3, :first_ask_1, :first_ask_2, :first_ask_3
    )
    ON CONFLICT (timestamp) DO NOTHING
    """)
    batch_data = []
    for t, bid, ask, bid_vol, ask_vol in ticks:
        row = {
            "instrument": asset,
            "timestamp": t,
            "bid_open": bid,
            "bid_high": bid,
            "bid_low": bid,
            "bid_close": bid,
            "ask_open": ask,
            "ask_high": ask,
            "ask_low": ask,
            "ask_close": ask,
            "bid_volume": bid_vol,
            "ask_volume": ask_vol,
            "avg_spread": 0.0,  # Platzhalter, später über Aggregation berechnet
            "first_bid_1": bid,
            "first_bid_2": None,
            "first_bid_3": None,
            "first_ask_1": ask,
            "first_ask_2": None,
            "first_ask_3": None,
        }
        batch_data.append(row)
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(insert_sql, batch_data)
    return len(batch_data)
