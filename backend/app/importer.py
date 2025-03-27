import os
import psycopg2
from datetime import datetime, timedelta
from app.datasources.dukascopy import fetch_tick_data, parse_ticks

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

def create_tick_table_if_not_exists(asset: str):
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

def import_tick_data_range(asset: str, start: datetime, end: datetime) -> int:
    """
    Downloads tick data for every hour between start and end.
    Skips download if data for a specific hour already exists.
    For non-crypto assets, skips Saturdays.
    """
    table_name = create_tick_table_if_not_exists(asset)
    total_inserted = 0
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        current_time = start.replace(minute=0, second=0, microsecond=0)
        while current_time <= end:
            # Skip Saturdays for non-crypto assets.
            if asset.lower() != "crypto" and current_time.weekday() == 5:
                print(f"Skipping {current_time} (Saturday, no data for non-crypto assets).")
                current_time += timedelta(hours=1)
                continue

            # Check if data for the current hour already exists.
            next_hour = current_time + timedelta(hours=1)
            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE timestamp >= %s AND timestamp < %s",
                (current_time, next_hour)
            )
            count = cur.fetchone()[0]
            if count > 0:
                print(f"Data already exists for {current_time}. Skipping download.")
                current_time += timedelta(hours=1)
                continue

            year = current_time.year
            month = current_time.month
            day = current_time.day
            hour = current_time.hour
            print(f"Fetching tick data for {asset} at {current_time}...")
            try:
                raw_data = fetch_tick_data(asset, year, month, day, hour)
                ticks = parse_ticks(raw_data, current_time)
                if ticks:
                    args_str = ",".join(
                        cur.mogrify("(%s, %s, %s, %s, %s)", tick).decode("utf-8")
                        for tick in ticks
                    )
                    insert_query = (
                        f"INSERT INTO {table_name} (timestamp, bid, ask, bid_volume, ask_volume) VALUES "
                        + args_str +
                        " ON CONFLICT (timestamp) DO NOTHING;"
                    )
                    cur.execute(insert_query)
                    inserted = cur.rowcount
                    total_inserted += inserted
                    print(f"Inserted {inserted} ticks for {current_time}.")
                else:
                    print(f"No ticks found for {current_time}.")
            except Exception as e:
                print(f"Error processing {current_time}: {e}")
            current_time += timedelta(hours=1)
    conn.close()
    return total_inserted
