import os
import asyncio
from datetime import datetime, timedelta
import asyncpg
from .datasources.dukascopy import fetch_tick_data, parse_ticks
import logging
import requests  # Only if needed for HTTPError

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")
BATCH_SIZE = 5000  # Maximum ticks per batch to avoid exceeding query parameter limits

async def create_tick_table_if_not_exists(asset: str, pool: asyncpg.Pool) -> str:
    """
    Creates the tick data table for the given asset if it does not exist
    and converts it to a hypertable.
    """
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
    create_hypertable_sql = f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);"
    async with pool.acquire() as conn:
        await conn.execute(create_table_sql)
        try:
            await conn.execute(create_hypertable_sql)
        except asyncpg.PostgresError as e:
            logger.warning("Could not create hypertable for %s: %s", table_name, e)
    return table_name

async def get_existing_hours(table_name: str, start: datetime, end: datetime, pool: asyncpg.Pool) -> set:
    """
    Returns a set of hours (truncated to the hour) that already have data.
    """
    query = f"""
    SELECT date_trunc('hour', timestamp) AS hour_block
    FROM {table_name}
    WHERE timestamp >= $1 AND timestamp <= $2
    GROUP BY hour_block;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, start, end)
    return {row["hour_block"] for row in rows}

async def hour_has_data(table_name: str, hour: datetime, pool: asyncpg.Pool) -> bool:
    """
    Checks if any tick data exist in the given hour.
    """
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM {table_name}
    WHERE timestamp >= $1 AND timestamp < $2;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query, hour, hour + timedelta(hours=1))
    return row and row["cnt"] > 0

def build_bulk_insert_query(table_name: str, ticks: list) -> (str, list):
    """
    Builds a bulk insert query with placeholders for multiple ticks.
    Each tick is a tuple: (timestamp, bid, ask, bid_volume, ask_volume)
    """
    values_clause = []
    params = []
    param_index = 1
    for tick in ticks:
        placeholders = []
        for _ in range(5):
            placeholders.append(f"${param_index}")
            param_index += 1
        values_clause.append("(" + ", ".join(placeholders) + ")")
        params.extend(tick)
    query = (
        f"INSERT INTO {table_name} (timestamp, bid, ask, bid_volume, ask_volume) VALUES "
        + ", ".join(values_clause)
        + " ON CONFLICT (timestamp) DO NOTHING;"
    )
    return query, params

async def process_missing_hour(hour: datetime, asset: str, table_name: str, pool: asyncpg.Pool) -> int:
    """
    Processes one missing hour:
      1. Skips fetching if the current hour already has data.
      2. For Friday, Saturday, and Sunday, checks the reference hour (one week ago).
         - If the reference hour has data, also checks the hour before and after.
         - If those surrounding hours exist, proceeds with fetching.
         - If the reference hour has no data, it still attempts to fetch the current hour.
      3. Fetches tick data from Dukascopy and inserts it in batches.
    Returns the number of inserted ticks.
    """
    # Skip fetch if current hour already has data.
    if await hour_has_data(table_name, hour, pool):
        logger.info("Data already exists for hour %s; skipping fetch.", hour)
        return 0

    # For Friday, Saturday, Sunday, apply reference check.
    if hour.weekday() in {4, 5, 6}:  # Friday=4, Saturday=5, Sunday=6
        ref_hour = hour - timedelta(days=7)
        # First, check if reference hour has any data.
        if await hour_has_data(table_name, ref_hour, pool):
            # If reference data exist, also check the hour before and after.
            exists_before = await hour_has_data(table_name, ref_hour - timedelta(hours=1), pool)
            exists_after = await hour_has_data(table_name, ref_hour + timedelta(hours=1), pool)
            if not (exists_before and exists_after):
                logger.info("Insufficient surrounding reference data for ref hour %s; skipping fetch for hour %s.",
                            ref_hour, hour)
                return 0
        else:
            logger.info("No reference data for ref hour %s; attempting fetch for hour %s.",
                        ref_hour, hour)

    logger.info("Fetching data for missing hour: %s", hour)
    try:
        # Fetch tick data in a thread to avoid blocking.
        raw_data = await asyncio.to_thread(fetch_tick_data, asset, hour.year, hour.month, hour.day, hour.hour)
        ticks = await asyncio.to_thread(parse_ticks, raw_data, hour)
        if ticks:
            total_inserted = 0
            # Insert ticks in batches.
            for i in range(0, len(ticks), BATCH_SIZE):
                batch = ticks[i:i+BATCH_SIZE]
                query, params = build_bulk_insert_query(table_name, batch)
                async with pool.acquire() as conn:
                    await conn.execute(query, *params)
                total_inserted += len(batch)
            logger.info("%d ticks inserted for %s.", total_inserted, hour)
            return total_inserted
        else:
            logger.warning("No ticks found for %s.", hour)
            return 0
    except requests.exceptions.HTTPError as he:
        if he.response.status_code == 404:
            logger.warning("404 Not Found for hour %s; skipping.", hour)
            return 0
        else:
            logger.exception("HTTP error processing hour %s: %s", hour, he)
            return 0
    except Exception as e:
        logger.exception("Error processing hour %s: %s", hour, e)
        return 0

async def import_tick_data_range(asset: str, start: datetime, end: datetime, pool: asyncpg.Pool) -> int:
    """
    Imports tick data for the specified asset between start and end datetimes.
    The start and end times are aligned to full hours.
    Only missing intervals outside the existing [db_min, db_max] are fetched.
    Returns the total number of inserted tick records.
    """
    table_name = await create_tick_table_if_not_exists(asset, pool)
    total_inserted = 0

    # Align start and end to full hours.
    start_aligned = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)

    # Query the DB for the min and max timestamp in the requested range.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM {table_name} WHERE timestamp >= $1 AND timestamp <= $2",
            start_aligned, end_aligned
        )
    
    missing_intervals = []
    if not row or row["min_ts"] is None or row["max_ts"] is None:
        missing_intervals.append((start_aligned, end_aligned))
    else:
        db_min = row["min_ts"].replace(tzinfo=None)
        db_max = row["max_ts"].replace(tzinfo=None)
        if db_min <= start_aligned and db_max >= end_aligned:
            logger.info("Complete data already exists in DB for %s in the range %s - %s. No import necessary.",
                        asset, start_aligned, end_aligned)
            return 0
        if start_aligned < db_min:
            missing_intervals.append((start_aligned, db_min))
        if db_max < end_aligned:
            missing_intervals.append((db_max, end_aligned))
    
    logger.info("Missing intervals for %s: %s", asset, missing_intervals)

    # Generate list of hours to fetch from missing intervals.
    hours_to_fetch = []
    for interval in missing_intervals:
        interval_start, interval_end = interval
        current = interval_start
        while current <= interval_end:
            hours_to_fetch.append(current)
            current += timedelta(hours=1)
    
    logger.info("Total missing hours to fetch: %d", len(hours_to_fetch))

    # Process missing hours concurrently with limited concurrency.
    semaphore = asyncio.Semaphore(6)
    async def process_with_semaphore(hour):
        async with semaphore:
            return await process_missing_hour(hour, asset, table_name, pool)
    tasks = [process_with_semaphore(hour) for hour in hours_to_fetch]
    results = await asyncio.gather(*tasks)
    total_inserted = sum(results)
    logger.info("Total ticks inserted for asset %s: %d", asset, total_inserted)
    return total_inserted
