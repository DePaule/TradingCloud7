import os
import asyncio
from datetime import datetime, timedelta
import asyncpg
from .datasources.dukascopy import fetch_tick_data, parse_ticks
import logging

logger = logging.getLogger(__name__)

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

async def import_tick_data_range(asset: str, start: datetime, end: datetime, pool: asyncpg.Pool) -> int:
    """
    Imports tick data for the specified asset between start and end datetimes.
    The start and end times are aligned to full hours.
    For non-crypto assets, weekends are skipped.
    Returns the total number of inserted tick records.
    """
    table_name = await create_tick_table_if_not_exists(asset, pool)
    total_inserted = 0

    # Align start and end to full hours.
    start_aligned = start.replace(minute=0, second=0, microsecond=0)
    end_aligned = end.replace(minute=0, second=0, microsecond=0)

    # Check if the table already contains data for the full range.
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT MIN(timestamp) AS min_ts, MAX(timestamp) AS max_ts FROM {table_name} WHERE timestamp >= $1 AND timestamp <= $2",
            start_aligned, end_aligned
        )
    if row and row["min_ts"] and row["max_ts"]:
        db_min = row["min_ts"].replace(tzinfo=None)
        db_max = row["max_ts"].replace(tzinfo=None)
        if db_min <= start_aligned and db_max >= end_aligned:
            logger.info("Complete data already exists in DB for %s. No import necessary.", asset)
            return 0

    # Generate list of hours to fetch.
    hours_to_fetch = []
    current = start_aligned
    while current <= end_aligned:
        if asset.lower() == "crypto" or current.weekday() not in [5, 6]:
            hours_to_fetch.append(current)
        current += timedelta(hours=1)

    # Determine which hours are missing in the DB.
    existing_hours = await get_existing_hours(table_name, start_aligned, end_aligned, pool)
    missing_hours = [hour for hour in hours_to_fetch if hour not in existing_hours]

    async with pool.acquire() as conn:
        for hour in missing_hours:
            logger.info("Missing hour in DB: %s, fetching data from Dukascopy...", hour)
            try:
                # Fetch tick data in a thread to avoid blocking the event loop.
                raw_data = await asyncio.to_thread(fetch_tick_data, asset, hour.year, hour.month, hour.day, hour.hour)
                ticks = await asyncio.to_thread(parse_ticks, raw_data, hour)
                if ticks:
                    # Prepare bulk insert.
                    values = [tick for tick in ticks]
                    insert_query = f"""
                    INSERT INTO {table_name} (timestamp, bid, ask, bid_volume, ask_volume)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (timestamp) DO NOTHING;
                    """
                    # Insert each tick individually.
                    for tick in values:
                        await conn.execute(insert_query, *tick)
                    inserted = len(values)
                    total_inserted += inserted
                    logger.info("%d ticks inserted for %s.", inserted, hour)
                else:
                    logger.warning("No ticks found for %s.", hour)
            except Exception as e:
                logger.exception("Error fetching data for %s: %s", hour, e)
    return total_inserted
