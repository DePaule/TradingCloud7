import os
import psycopg2
from datetime import datetime, timedelta
from app.datasources.dukascopy import fetch_tick_data, parse_ticks

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
    """
    Downloads tick data for every hour between start and end.
    Skips the entire weekend (Saturday/Sunday) for non-crypto assets.
    Only fetches data for hours that are missing in the DB.
    """
    table_name = create_tick_table_if_not_exists(asset)
    total_inserted = 0

    # Align start/end to the hour
    current_time = start.replace(minute=0, second=0, microsecond=0)
    end_time = end.replace(minute=0, second=0, microsecond=0)

    # Get a set of all existing hours in the DB for this range
    existing_hours = get_existing_hours(table_name, current_time, end_time)

    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        while current_time <= end_time:
            # Skip weekend for non-crypto assets
            # weekday() == 5 -> Samstag, == 6 -> Sonntag
            if asset.lower() != "crypto" and current_time.weekday() in [5, 6]:
                current_time += timedelta(hours=1)
                continue

            # Check if this hour is already in DB
            if current_time in existing_hours:
                # Already has data, skip
                current_time += timedelta(hours=1)
                continue

            print(f"[INFO] Hour missing in DB: {current_time}, fetching from Dukascopy...")

            try:
                raw_data = fetch_tick_data(
                    asset,
                    current_time.year,
                    current_time.month,
                    current_time.day,
                    current_time.hour
                )
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
                    print(f"[INFO] Inserted {inserted} ticks for {current_time}.")
                else:
                    print(f"[WARN] No ticks found for {current_time}.")
            except Exception as e:
                print(f"[ERROR] Failed to fetch or insert data for {current_time}: {e}")

            current_time += timedelta(hours=1)
    conn.close()
    return total_inserted