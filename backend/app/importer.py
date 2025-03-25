# TradingCloud7/backend/app/importer.py
import datetime
from typing import Tuple, List
from app.db import get_engine, create_table_if_not_exists, fetch_existing_data, insert_data
from app.datasources.dukascopy import fetch_ticks_for_date_range
from app.aggregation import aggregate_ticks

def import_and_get_data(asset: str, start: str, end: str, timeframe: str) -> Tuple[List[dict], int]:
    """
    Prüft, ob aggregierte Tickdaten für das Asset im Zeitraum bereits in Timescale vorliegen.
    Falls nicht, werden die Tickdaten von Dukascopy abgerufen, in einer automatisch angelegten Tabelle gespeichert
    und dann aggregiert.
    
    Rückgabe:
      aggregated_data: Liste von Candle-Dictionaries (sortiert nach timestamp)
      inserted_count: Anzahl der neu eingefügten Zeilen (0, falls bereits alle Daten vorhanden waren)
    """
    engine = get_engine()
    table_name = f"{asset.lower()}_{timeframe.lower()}"
    create_table_if_not_exists(engine, table_name)

    start_date = datetime.datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d").date()

    existing_data = fetch_existing_data(engine, table_name, start_date, end_date)
    inserted_count = 0
    if not existing_data:
        ticks = fetch_ticks_for_date_range(asset, start_date, end_date)
        if not ticks:
            raise Exception("Keine Tickdaten von Dukascopy erhalten.")
        inserted_count = insert_data(engine, table_name, ticks, asset)
        existing_data = fetch_existing_data(engine, table_name, start_date, end_date)
    
    aggregated_data = aggregate_ticks(existing_data, timeframe)
    return aggregated_data, inserted_count
