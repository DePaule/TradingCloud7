from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from datetime import datetime
from app.importer import import_tick_data_range
import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

app = FastAPI(title="TradingCloud Tick Data Importer and Aggregator")

# Endpoint to import tick data
class FetchDataRequest(BaseModel):
    asset: str = Field(..., description="Asset symbol, e.g., EURUSD")
    start: datetime = Field(..., description="Start datetime in ISO format")
    end: datetime = Field(..., description="End datetime in ISO format")

@app.post("/fetch-data")
async def fetch_data(request: FetchDataRequest):
    try:
        inserted_count = import_tick_data_range(request.asset, request.start, request.end)
        return {"message": "Tick data imported successfully", "inserted_rows": inserted_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to retrieve raw tick data (optional)
@app.get("/api/ticks")
async def get_ticks(asset: str, start: datetime, end: datetime):
    table_name = f"{asset.lower()}_tick"
    try:
        conn = psycopg2.connect(DATABASE_URL)
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

# Endpoint to retrieve aggregated candlestick data
@app.get("/api/candles")
async def get_candles(
    asset: str,
    resolution: str = Query(..., description="Resolution e.g., M10 for 10 minutes"),
    start: datetime = Query(...),
    end: datetime = Query(...)
):
    inserted_count = import_tick_data_range(asset, start, end)
    # Parse resolution, e.g., "M10" -> 10 minutes
    if resolution.startswith("M"):
        try:
            minutes = int(resolution[1:])
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid resolution format")
    else:
        raise HTTPException(status_code=400, detail="Resolution must start with 'M'")
    
    table_name = f"{asset.lower()}_tick"
    try:
        conn = psycopg2.connect(DATABASE_URL)
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
            interval_str = f'{minutes} minutes'
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

# Serve the web interface from the frontend folder
#from fastapi.staticfiles import StaticFiles
#app.mount("/", StaticFiles(directory="frontend", html=True), name="static")
