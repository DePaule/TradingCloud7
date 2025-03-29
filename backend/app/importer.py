import os
import psycopg2
from datetime import datetime, timedelta
from .datasources.dukascopy import fetch_tick_data, parse_ticks

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

def create_tick_table_if_not_exists(asset: str) -> str:
    table_name = f"{asset.lower()}_tick"
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        bid REAL,
        ask REAL,
        bid_volume REAL,
        ask_volume REAL
    );
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(create_table_sql)
        cur.execute(f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);")
    conn.close()
    return table_name

def get_existing_hours(table_name: str, start: datetime, end: datetime):
    """
    Returns a set of hourly timestamps (UTC truncated to the hour) that already have data.
    """
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        # date_trunc('hour', timestamp) gives us the hour block
        query = f"""
        SELECT date_trunc('hour', timestamp) AS hour_block
        FROM {table_name}
        WHERE timestamp >= %s AND timestamp <= %s
        GROUP BY hour_block
        """
        cur.execute(query, (start, end))
        rows = cur.fetchall()
    conn.close()

    # Convert rows to a set of datetime objects
    existing = set()
    for row in rows:
        # row[0] is a datetime with minutes/seconds = 0
        existing.add(row[0])
    return existing

def import_tick_data_range(asset: str, start: datetime, end: datetime) -> int:
    table_name = create_tick_table_if_not_exists(asset)
    total_inserted = 0

    # Aligniere Start/End-Zeit auf volle Stunden
    start_aligned = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        # Prüfe, ob für den gesamten Zeitraum bereits Daten vorhanden sind
        cur.execute(
            f"SELECT MIN(timestamp), MAX(timestamp) FROM {table_name} WHERE timestamp >= %s AND timestamp <= %s",
            (start_aligned, end_aligned)
        )
        min_max = cur.fetchone()
    
    # Falls sowohl ein Minimum als auch ein Maximum vorhanden sind und diese den gesamten Zeitraum abdecken,
    # gehen wir davon aus, dass alle relevanten Stunden in der DB liegen.
    if min_max and min_max[0] is not None and min_max[1] is not None:
        if min_max[0] <= start_aligned and min_max[1] >= end_aligned:
            print("[INFO] Vollständige Daten im DB vorhanden. Kein Import nötig.")
            conn.close()
            return 0

    # Andernfalls: Erstelle Liste aller Stunden im Zeitraum (bei Nicht-Krypto werden Wochenenden übersprungen)
    hours_to_fetch = []
    current = start_aligned
    while current <= end_aligned:
        if asset.lower() == "crypto" or current.weekday() not in [5, 6]:
            hours_to_fetch.append(current)
        current += timedelta(hours=1)
    
    # Ermittle, welche Stunden in der DB bereits vorhanden sind
    existing_hours = get_existing_hours(table_name, start_aligned, end_aligned)
    missing_hours = [hour for hour in hours_to_fetch if hour not in existing_hours]

    with conn.cursor() as cur:
        for hour in missing_hours:
            print(f"[INFO] Fehlende Stunde in DB: {hour}, hole Daten von Dukascopy...")
            try:
                raw_data = fetch_tick_data(asset, hour.year, hour.month, hour.day, hour.hour)
                ticks = parse_ticks(raw_data, hour)
                if ticks:
                    args_str = ",".join(
                        cur.mogrify("(%s, %s, %s, %s, %s)", tick).decode("utf-8")
                        for tick in ticks
                    )
                    insert_query = (
                        f"INSERT INTO {table_name} (timestamp, bid, ask, bid_volume, ask_volume) VALUES " +
                        args_str +
                        " ON CONFLICT (timestamp) DO NOTHING;"
                    )
                    cur.execute(insert_query)
                    inserted = cur.rowcount
                    total_inserted += inserted
                    print(f"[INFO] {inserted} Tick(s) für {hour} eingefügt.")
                else:
                    print(f"[WARN] Für {hour} wurden keine Ticks gefunden.")
            except Exception as e:
                print(f"[ERROR] Fehler beim Laden von Daten für {hour}: {e}")
    conn.close()
    return total_inserted