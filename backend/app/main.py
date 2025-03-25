from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from app.importer import import_ticks_for_range

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DataRequest(BaseModel):
    asset: str        # e.g. "EURUSD"
    start: str        # "2023-01-01"
    end: str          # "2023-01-03"
    timeframe: str    # e.g. "M1", "M5", "H1"

@app.post("/fetch-data")
def fetch_data(req: DataRequest):
    count = import_ticks_for_range(
        asset=req.asset,
        start=req.start,
        end=req.end,
        timeframe=req.timeframe
    )
    return {"status": "ok", "inserted": count}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
