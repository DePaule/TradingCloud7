from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime, time
import psycopg2
import os

# Import the function that imports tick data (creates the table if it doesn't exist)
from .importer import import_tick_data_range

# Retrieve the database URL from environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@db:5432/tradingcloud")

app = FastAPI(title="TradingCloud API")

def get_connection():
    """
    Returns a new PostgreSQL connection using DATABASE_URL.
    """
    return psycopg2.connect(DATABASE_URL)

def adjust_start_end(start: datetime, end: datetime):
    """
    Adjust the provided start and end datetimes so that the start time is 00:00 and
    the end time is 23:59 of their respective dates.
    """
    new_start = datetime.combine(start.date(), time(0, 0))
    new_end = datetime.combine(end.date(), time(23, 59))
    return new_start, new_end

# ---------------------------
# Existing endpoints
# ---------------------------

class FetchDataRequest(BaseModel):
    asset: str = Field(..., description="Asset symbol, e.g., EURUSD")
    start: datetime = Field(..., description="Start datetime in ISO format")
    end: datetime = Field(..., description="End datetime in ISO format")

@app.post("/fetch-data")
async def fetch_data(request: FetchDataRequest):
    """
    POST endpoint to import tick data for a given asset and time range.
    The time range is automatically adjusted to start at 00:00 and end at 23:59.
    """
    adjusted_start, adjusted_end = adjust_start_end(request.start, request.end)
    try:
        inserted_count = import_tick_data_range(request.asset, adjusted_start, adjusted_end)
        return {"message": "Tick data imported successfully", "inserted_rows": inserted_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ticks")
async def get_ticks(asset: str, start: datetime, end: datetime):
    """
    GET endpoint to retrieve raw tick data for a given asset and time range.
    """
    start, end = adjust_start_end(start, end)
    table_name = f"{asset.lower()}_tick"
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table_name} WHERE timestamp >= %s AND timestamp <= %s ORDER BY timestamp",
                (start, end)
            )
            rows = cur.fetchall()
        conn.close()
        return {"ticks": rows}
    except Exception as e:
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
    Before aggregation, missing tick data is imported via import_tick_data_range.
    """
    start, end = adjust_start_end(start, end)

    # Import missing tick data before aggregation
    try:
        _ = import_tick_data_range(asset, start, end)
    except Exception as e:
        # Log or ignore errors from import_tick_data_range if needed.
        pass

    # Parse the resolution input
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

    table_name = f"{asset.lower()}_tick"
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            query = f"""
            SELECT 
                time_bucket(%s, timestamp) AS bucket,
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
            cur.execute(query, (interval_str, start, end))
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
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------
# New endpoints for instrument groups and instruments
# ---------------------------

@app.get("/api/instrument-groups")
async def get_instrument_groups():
    """
    GET endpoint to retrieve distinct instrument groups from the data_provider_instruments table.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT unnest(group_ids) AS group_id 
                FROM data_provider_instruments 
                WHERE group_ids IS NOT NULL
                ORDER BY group_id;
            """)
            rows = cur.fetchall()
        conn.close()
        groups = [row[0] for row in rows]
        return {"groups": groups}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/instruments")
async def get_instruments(
    group: str = Query("fx_majors", description="Group ID, e.g., fx_majors")
):
    """
    GET endpoint to retrieve all instruments for a given group.
    Default group is 'fx_majors'.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT instrument_id, instrument_name, description, decimal_factor, 
                       start_hour_for_ticks, start_day_for_minute_candles, 
                       start_month_for_hourly_candles, start_year_for_daily_candles, group_ids
                FROM data_provider_instruments
                WHERE %s = ANY(group_ids)
                ORDER BY instrument_name;
            """, (group,))
            rows = cur.fetchall()
        conn.close()
        instruments = []
        for row in rows:
            instruments.append({
                "instrument_id": row[0],
                "instrument_name": row[1],
                "description": row[2],
                "decimal_factor": row[3],
                "start_hour_for_ticks": row[4].isoformat() if row[4] else None,
                "start_day_for_minute_candles": row[5].isoformat() if row[5] else None,
                "start_month_for_hourly_candles": row[6].isoformat() if row[6] else None,
                "start_year_for_daily_candles": row[7].isoformat() if row[7] else None,
                "group_ids": row[8],
            })
        return {"instruments": instruments}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
