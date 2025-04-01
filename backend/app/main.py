import os
import logging
from datetime import datetime, time
from fastapi import FastAPI, HTTPException, Query, Depends
from pydantic import BaseModel, Field
import asyncpg
import psycopg2
import asyncio

# Import the blocking tick data importer function.
from .importer import import_tick_data_range

# Read the database URL from environment variables.
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@db:5432/tradingcloud")

# Configure logging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TradingCloud API (Improved)")

# ----- Asynchronous DB Pool (for endpoints using asyncpg) -----
@app.on_event("startup")
async def startup():
    try:
        app.state.db_pool = await asyncpg.create_pool(DATABASE_URL)
        logger.info("Database pool created successfully.")
    except Exception as e:
        logger.exception("Error creating database pool: %s", e)
        raise

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()
    logger.info("Database pool closed.")

# Dependency: acquire a DB connection from the asyncpg pool.
async def get_db():
    async with app.state.db_pool.acquire() as connection:
        yield connection

# Synchronous connection for endpoints using psycopg2 (candle endpoint).
def get_connection():
    return psycopg2.connect(DATABASE_URL)

def adjust_start_end(start: datetime, end: datetime):
    """
    Adjusts the provided start and end datetimes so that start is set to 00:00 
    and end is set to 23:59 of their respective days.
    """
    new_start = datetime.combine(start.date(), time(0, 0))
    new_end = datetime.combine(end.date(), time(23, 59))
    return new_start, new_end

# ----- Pydantic Model -----
class FetchDataRequest(BaseModel):
    asset: str = Field(..., description="Asset symbol, e.g., EURUSD")
    start: datetime = Field(..., description="Start datetime in ISO format")
    end: datetime = Field(..., description="End datetime in ISO format")

# ----- Endpoints -----

@app.post("/fetch-data")
async def fetch_data(request: FetchDataRequest):
    """
    POST endpoint to import tick data for the specified asset and time range.
    The time range is adjusted internally to cover 00:00 to 23:59.
    """
    adjusted_start, adjusted_end = adjust_start_end(request.start, request.end)
    try:
        # Run the blocking importer in a separate thread.
        inserted_count = await asyncio.to_thread(
            import_tick_data_range, request.asset, adjusted_start, adjusted_end, app.state.db_pool
        )
        return {"message": "Tick data imported successfully", "inserted_rows": inserted_count}
    except Exception as e:
        logger.exception("Error importing tick data")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/candles")
async def get_candles(
    asset: str,
    resolution: str = Query(..., description="Resolution, e.g., M10, 10s, H4, H8, D1"),
    start: datetime = Query(...),
    end: datetime = Query(...)
):
    """
    GET endpoint to retrieve aggregated candlestick data for a given asset and time range.
    The time range is adjusted to 00:00 (start) and 23:59 (end).
    Before aggregation, the endpoint checks if the tick data table exists.
    If it does not, the table is created and filled using the Dukascopy importer.
    This endpoint uses a synchronous psycopg2 connection for candle fetching.
    """
    start, end = adjust_start_end(start, end)
    table_name = f"{asset.lower()}_tick"

    # Check table existence using information_schema
    try:
            await import_tick_data_range(asset, start, end, app.state.db_pool)
    except Exception as e:
        logger.warning("Error checking table existence for %s: %s", table_name, e)

    # Parse resolution string
    if resolution.endswith("s"):
        try:
            seconds = int(resolution[:-1])
            interval_str = f"{seconds} seconds"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid resolution format")
    elif resolution.startswith("M"):
        try:
            minutes = int(resolution[1:])
            interval_str = f"{minutes} minutes"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid resolution format")
    elif resolution.startswith("H"):
        try:
            hours = int(resolution[1:])
            interval_str = f"{hours} hours"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid resolution format")
    elif resolution.startswith("D"):
        try:
            days = int(resolution[1:])
            interval_str = f"{days} days"
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid resolution format")
    else:
        raise HTTPException(status_code=400, detail="Resolution must end with 's' or start with 'M', 'H' or 'D'")

    # Now fetch the candle data using the original synchronous logic.
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            query = f"""
            SELECT 
                time_bucket('{interval_str}', timestamp) AS bucket,
                first(bid, timestamp) AS open,
                max(bid) AS high,
                min(bid) AS low,
                last(bid, timestamp) AS close,
                sum(bid_volume + ask_volume) AS volume
            FROM {table_name}
            WHERE timestamp >= %s AND timestamp <= %s
            GROUP BY bucket
            ORDER BY bucket;
            """
            cur.execute(query, (start, end))
            rows = cur.fetchall()
        conn.close()
        candles = []
        for row in rows:
            candles.append({
                "bucket": row[0].isoformat(),
                "open": row[1],
                "high": row[2],
                "low": row[3],
                "close": row[4],
                "volume": row[5]
            })
        return {"candles": candles}
    except Exception as e:
        logger.exception("Error fetching candlestick data")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/instrument-groups")
async def get_instrument_groups(db=Depends(get_db)):
    """
    GET endpoint to retrieve distinct instrument groups from the data_provider_instruments table.
    """
    try:
        rows = await db.fetch("""
            SELECT DISTINCT unnest(group_ids) AS group_id 
            FROM data_provider_instruments 
            WHERE group_ids IS NOT NULL
            ORDER BY group_id;
        """)
        groups = [row["group_id"] for row in rows]
        return {"groups": groups}
    except Exception as e:
        logger.exception("Error fetching instrument groups")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instruments")
async def get_instruments(
    group: str = Query("fx_majors", description="Group ID, e.g., fx_majors"),
    db=Depends(get_db)
):
    """
    GET endpoint to retrieve all instruments for a given group.
    """
    try:
        rows = await db.fetch("""
            SELECT instrument_id, instrument_name, description, decimal_factor, 
                   start_hour_for_ticks, start_day_for_minute_candles, 
                   start_month_for_hourly_candles, start_year_for_daily_candles, group_ids
            FROM data_provider_instruments
            WHERE $1 = ANY(group_ids)
            ORDER BY instrument_name;
        """, group)
        instruments = []
        for row in rows:
            instruments.append({
                "instrument_id": row["instrument_id"],
                "instrument_name": row["instrument_name"],
                "description": row["description"],
                "decimal_factor": row["decimal_factor"],
                "start_hour_for_ticks": row["start_hour_for_ticks"].isoformat() if row["start_hour_for_ticks"] else None,
                "start_day_for_minute_candles": row["start_day_for_minute_candles"].isoformat() if row["start_day_for_minute_candles"] else None,
                "start_month_for_hourly_candles": row["start_month_for_hourly_candles"].isoformat() if row["start_month_for_hourly_candles"] else None,
                "start_year_for_daily_candles": row["start_year_for_daily_candles"].isoformat() if row["start_year_for_daily_candles"] else None,
                "group_ids": row["group_ids"],
            })
        return {"instruments": instruments}
    except Exception as e:
        logger.exception("Error fetching instruments")
        raise HTTPException(status_code=500, detail=str(e))