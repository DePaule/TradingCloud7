import os
import asyncio
from datetime import datetime, timedelta
import asyncpg
from .datasources.dukascopy import fetch_tick_data, parse_ticks
import logging
import requests

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")
BATCH_SIZE = 5000  # Maximum ticks per batch

async def create_tick_table_if_not_exists(asset: str, pool: asyncpg.Pool) -> str:
    """
    Creates the tick data table for the given asset if it does not exist,
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
      1. Skips if data for the hour already exist.
      2. Attempts to fetch tick data from Dukascopy.
      3. If a 404 error occurs on a Friday (indicating weekend start), returns -1.
      4. Otherwise, inserts fetched ticks in batches.
    Returns the number of inserted ticks, or -1 if a weekend gap is detected.
    """
    if await hour_has_data(table_name, hour, pool):
        logger.info("Data already exists for hour %s; skipping fetch.", hour)
        return 0

    try:
        logger.info("Fetching data for missing hour: %s", hour)
        raw_data = await asyncio.to_thread(fetch_tick_data, asset, hour.year, hour.month, hour.day, hour.hour)
        ticks = await asyncio.to_thread(parse_ticks, raw_data, hour)
    except requests.exceptions.HTTPError as he:
        if he.response.status_code == 404:
            logger.warning("404 Not Found for hour %s; skipping.", hour)
            if hour.weekday() == 4:  # Friday
                return -1
            return 0
        else:
            logger.exception("HTTP error processing hour %s: %s", hour, he)
            return 0
    except Exception as e:
        logger.exception("Error processing hour %s: %s", hour, e)
        return 0

    if ticks:
        total_inserted = 0
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

async def import_tick_data_range(asset: str, start: datetime, end: datetime, pool: asyncpg.Pool) -> int:
    """
    Imports tick data for the specified asset between start and end.
    Times are aligned to full hours; missing intervals (based on existing data) are determined and processed.
    If a 404 occurs on a Friday, the next 44 hours are skipped.
    Returns the total number of inserted tick records.
    """
    table_name = await create_tick_table_if_not_exists(asset, pool)
    total_inserted = 0

    start_aligned = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)

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

    hours_to_fetch = []
    for interval in missing_intervals:
        interval_start, interval_end = interval
        current = interval_start
        while current <= interval_end:
            hours_to_fetch.append(current)
            current += timedelta(hours=1)
    
    logger.info("Total missing hours to fetch: %d", len(hours_to_fetch))

    skip_until = None
    for hour in sorted(hours_to_fetch):
        if skip_until and hour < skip_until:
            logger.info("Skipping hour %s due to weekend skip until %s.", hour, skip_until)
            continue
        result = await process_missing_hour(hour, asset, table_name, pool)
        if result == -1:
            skip_until = hour + timedelta(hours=44)
            logger.info("Weekend detected at hour %s; skipping to %s.", hour, skip_until)
        else:
            total_inserted += result

    logger.info("Total ticks inserted for asset %s: %d", asset, total_inserted)
    return total_inserted
