import os
import asyncio
from datetime import datetime, timedelta
import asyncpg
from .datasources.dukascopy import fetch_tick_data, parse_ticks
import logging

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

async def create_tick_table_if_not_exists(asset: str, pool: asyncpg.Pool) -> str:
    """
    Creates the tick data table for the given asset if it does not exist,
    and converts it to a hypertable if possible.
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
    Returns a set of hourly timestamps (truncated to the hour) that already have data.
    """
    query = f"""
    SELECT date_trunc('hour', timestamp) AS hour_block
    FROM {table_name}
    WHERE timestamp >= $1 AND timestamp <= $2
    GROUP BY hour_block;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, start, end)
    existing = {row["hour_block"] for row in rows}
    return existing

def build_bulk_insert_query(table_name: str, ticks: list) -> (str, list):
    """
    Builds a bulk insert query for asyncpg with multiple rows.
    Returns the query string and a list of parameters.
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
    Processes one missing hour: fetches tick data from Dukascopy, builds the bulk insert,
    and executes it. Returns the number of inserted ticks.
    """
    try:
        logger.info("Fetching data for missing hour: %s", hour)
        # Fetch tick data using a thread to avoid blocking.
        raw_data = await asyncio.to_thread(fetch_tick_data, asset, hour.year, hour.month, hour.day, hour.hour)
        ticks = await asyncio.to_thread(parse_ticks, raw_data, hour)
        if ticks:
            query, params = build_bulk_insert_query(table_name, ticks)
            async with pool.acquire() as conn:
                await conn.execute(query, *params)
            inserted = len(ticks)
            logger.info("%d ticks inserted for %s.", inserted, hour)
            return inserted
        else:
            logger.warning("No ticks found for %s.", hour)
            return 0
    except Exception as e:
        logger.exception("Error processing hour %s: %s", hour, e)
        return 0

async def import_tick_data_range(asset: str, start: datetime, end: datetime, pool: asyncpg.Pool) -> int:
    """
    Imports tick data for the specified asset between start and end datetimes.
    The start and end times are aligned to full hours.
    For non-crypto assets, weekends are skipped.
    If no data exist in the requested range, data are downloaded from Dukascopy.
    Returns the total number of inserted tick records.
    """
    table_name = await create_tick_table_if_not_exists(asset, pool)
    total_inserted = 0

    # Align start and end to full hours.
    start_aligned = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)

    # Check if the table contains data for the requested range.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM {table_name} WHERE timestamp >= $1 AND timestamp <= $2",
            start_aligned, end_aligned
        )
    if row and row["min_ts"] is not None and row["max_ts"] is not None:
        db_min = row["min_ts"].replace(tzinfo=None)
        db_max = row["max_ts"].replace(tzinfo=None)
        if db_min <= start_aligned and db_max >= end_aligned:
            logger.info("Complete data already exists in DB for %s in the range %s - %s. No import necessary.",
                        asset, start_aligned, end_aligned)
            return 0
    else:
        logger.info("No data in DB for %s in the range %s - %s. Importing...", asset, start_aligned, end_aligned)

    # Generate list of hours to fetch (skip weekends for non-crypto).
    hours_to_fetch = []
    current = start_aligned
    while current <= end_aligned:
        if asset.lower() == "crypto" or current.weekday() not in [5, 6]:
            hours_to_fetch.append(current)
        current += timedelta(hours=1)
    
    # Determine which hours are missing in the DB.
    existing_hours = await get_existing_hours(table_name, start_aligned, end_aligned, pool)
    missing_hours = [hour for hour in hours_to_fetch if hour not in existing_hours]
    logger.info("Missing hours: %s", missing_hours)

    # Limit concurrent processing to avoid overload.
    semaphore = asyncio.Semaphore(5)  # max 5 concurrent tasks

    async def process_with_semaphore(hour):
        async with semaphore:
            return await process_missing_hour(hour, asset, table_name, pool)

    tasks = [process_with_semaphore(hour) for hour in missing_hours]
    results = await asyncio.gather(*tasks)
    total_inserted = sum(results)
    logger.info("Total ticks inserted: %d", total_inserted)
    return total_inserted
