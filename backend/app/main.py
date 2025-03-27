from fastapi import FastAPI, HTTPException, Query
from datetime import datetime
import psycopg2
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

app = FastAPI(title="TradingCloud API")

@app.get("/api/candles")
async def get_candles(
    asset: str,
    resolution: str = Query(..., description="Resolution e.g., M10 for 10 minutes"),
    start: datetime = Query(...),
    end: datetime = Query(...)
):
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
