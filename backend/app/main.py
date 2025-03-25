# TradingCloud7/backend/app/main.py
import debugpy

# Starte den Debug-Listener auf Port 5678
debugpy.listen(("0.0.0.0", 5678))
print("Waiting for debugger to attach...", flush=True)
# Warten, bis der Debugger angehängt ist – entferne dies, wenn du nicht warten möchtest:
debugpy.wait_for_client()

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
from app.importer import import_and_get_data

app = FastAPI(title="TradingCloud Data Import")

class DataRequest(BaseModel):
    asset: str
    start: str
    end: str
    timeframe: str

@app.get("/fetch-data")
def fetch_data(asset: str, start: str, end: str, timeframe: str):
    try:
        datetime.strptime(start, "%Y-%m-%d")
        datetime.strptime(end, "%Y-%m-%d")
    except Exception:
        raise HTTPException(status_code=400, detail="Ungültiges Datumsformat. Nutze YYYY-MM-DD")
    try:
        aggregated_data, inserted_count = import_and_get_data(asset, start, end, timeframe)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return {"status": "ok", "inserted": inserted_count, "data": aggregated_data}

if __name__ == "__main__":
    import uvicorn
    # Ohne --reload, damit debugpy korrekt funktioniert:
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
