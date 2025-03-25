from datetime import datetime, timedelta
from sqlalchemy import text
from app.db import get_engine
from app.datasources import dukascopy

TIMEFRAME_TO_SECONDS = {
    "S10": 10, "M1": 60, "M5": 300, "M10": 600,
    "M15": 900, "M30": 1800, "H1": 3600, "H4": 14400, "D1": 86400
}

def floor_dt(dt: datetime, interval: int):
    ts = int(dt.timestamp())
    return datetime.utcfromtimestamp(ts - (ts % interval)).replace(tzinfo=dt.tzinfo)

def aggregate_ticks(ticks, interval_sec):
    candles = {}
    for t, bid, ask, bid_vol, ask_vol in ticks:
        bucket = floor_dt(t, interval_sec)
        if bucket not in candles:
            candles[bucket] = {
                'bid_open': bid, 'bid_high': bid, 'bid_low': bid, 'bid_close': bid,
                'ask_open': ask, 'ask_high': ask, 'ask_low': ask, 'ask_close': ask,
                'bid_volume': bid_vol, 'ask_volume': ask_vol
            }
        else:
            c = candles[bucket]
            c['bid_high'] = max(c['bid_high'], bid)
            c['bid_low'] = min(c['bid_low'], bid)
            c['bid_close'] = bid
            c['ask_high'] = max(c['ask_high'], ask)
            c['ask_low'] = min(c['ask_low'], ask)
            c['ask_close'] = ask
            c['bid_volume'] += bid_vol
            c['ask_volume'] += ask_vol
    return candles

def create_table_if_not_exists(engine, table_name):
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMPTZ PRIMARY KEY,
        bid_open REAL,
        bid_high REAL,
        bid_low REAL,
        bid_close REAL,
        ask_open REAL,
        ask_high REAL,
        ask_low REAL,
        ask_close REAL,
        bid_volume REAL,
        ask_volume REAL
    );
    SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);
    """
    with engine.connect() as conn:
        for stmt in create_sql.strip().split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
        conn.commit()

def import_ticks_for_range(asset: str, start: str, end: str, timeframe: str):
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    interval = TIMEFRAME_TO_SECONDS[timeframe.upper()]
    table_name = f"{asset.lower()}_{timeframe.lower()}"

    engine = get_engine()
    create_table_if_not_exists(engine, table_name)

    total_inserted = 0
    for i in range((end_date - start_date).days + 1):
        day = start_date + timedelta(days=i)
        ticks = dukascopy.fetch_ticks(asset, day)
        candles = aggregate_ticks(ticks, interval)

        if not candles:
            continue

        with engine.connect() as conn:
            for ts, c in candles.items():
                conn.execute(text(f"""
                    INSERT INTO {table_name} VALUES (
                        :ts, :bo, :bh, :bl, :bc,
                        :ao, :ah, :al, :ac,
                        :bv, :av
                    ) ON CONFLICT DO NOTHING
                """), {
                    "ts": ts,
                    "bo": c['bid_open'], "bh": c['bid_high'], "bl": c['bid_low'], "bc": c['bid_close'],
                    "ao": c['ask_open'], "ah": c['ask_high'], "al": c['ask_low'], "ac": c['ask_close'],
                    "bv": c['bid_volume'], "av": c['ask_volume']
                })
            conn.commit()
            total_inserted += len(candles)
    return total_inserted
