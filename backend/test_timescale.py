#!/usr/bin/env python3
import os
from sqlalchemy import create_engine, MetaData, Table, Column, Float, DateTime, String, text
from sqlalchemy.exc import SQLAlchemyError

# Verwende hier deine Verbindungs-URL oder setze sie per Umgebungsvariable DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

def create_table_if_not_exists(engine, table_name: str):
    metadata = MetaData()
    table = Table(
        table_name, metadata,
        Column("timestamp", DateTime(timezone=True), primary_key=True),
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
        Column("instrument", String)
    )
    
    # Tabelle erstellen, falls sie nicht existiert
    metadata.create_all(engine, checkfirst=True)
    print(f"Tabelle '{table_name}' erstellt (oder existiert bereits).")
    
    # Hypertable-Erstellung separat durchführen (TimescaleDB-spezifisch)
    with engine.connect() as conn:
        try:
            conn.execute(text(
                f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);"
            ))
            conn.commit()
            print(f"Hypertable '{table_name}' erstellt oder bereits vorhanden.")
        except SQLAlchemyError as e:
            print(f"Fehler bei der Hypertable-Erstellung: {e}")

def main():
    # Setze den client_encoding explizit auf UTF-8
    print("Using DSN:", DATABASE_URL)

    engine = create_engine(DATABASE_URL, connect_args={"client_encoding": "utf8"})
    table_name = "test_table"
    create_table_if_not_exists(engine, table_name)

if __name__ == "__main__":
    main()