import os
import asyncpg
import requests
from datetime import datetime

# Database URL from environment; default uses container name "db"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@db:5432/tradingcloud")

# URLs for instrument meta data and instrument groups (example from dukascopy-node)
META_DATA_URL = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/refs/heads/master/src/utils/instrument-meta-data/generated/instrument-meta-data.json"
GROUPS_URL = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/refs/heads/master/src/utils/instrument-meta-data/generated/instrument-groups.json"

async def get_db_pool():
    return await asyncpg.create_pool(DATABASE_URL)

async def create_tables(pool: asyncpg.Pool):
    """
    Creates the tables 'data_provider' and 'data_provider_instruments' (with additional columns) if they do not exist.
    """
    create_provider_table = """
    CREATE TABLE IF NOT EXISTS data_provider (
        pk SERIAL PRIMARY KEY,
        name_provider TEXT NOT NULL,
        source TEXT NOT NULL,
        comment TEXT
    );
    """
    create_instruments_table = """
    CREATE TABLE IF NOT EXISTS data_provider_instruments (
        pk SERIAL PRIMARY KEY,
        pk_data_provider INTEGER NOT NULL REFERENCES data_provider(pk),
        instrument_id TEXT NOT NULL,
        instrument_name TEXT NOT NULL,
        description TEXT,
        decimal_factor INTEGER,
        start_hour_for_ticks TIMESTAMPTZ,
        start_day_for_minute_candles TIMESTAMPTZ,
        start_month_for_hourly_candles TIMESTAMPTZ,
        start_year_for_daily_candles TIMESTAMPTZ,
        group_ids TEXT[],
        comment TEXT
    );
    """
    async with pool.acquire() as conn:
        await conn.execute(create_provider_table)
        await conn.execute(create_instruments_table)

async def get_or_create_data_provider(pool: asyncpg.Pool, name_provider: str, source: str, comment: str = "") -> int:
    """
    Searches for an existing provider or creates a new entry.
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT pk FROM data_provider WHERE name_provider = $1 AND source = $2",
            name_provider, source
        )
        if row:
            return row["pk"]
        row = await conn.fetchrow(
            "INSERT INTO data_provider (name_provider, source, comment) VALUES ($1, $2, $3) RETURNING pk",
            name_provider, source, comment
        )
        return row["pk"]

def fetch_json(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] Error fetching {url}: {e}")
        return None

async def insert_instruments(pool: asyncpg.Pool, provider_id: int, meta_data: dict, groups: list):
    """
    Inserts all instrument meta data into the database.
    For each instrument:
      - 'instrument_id' is the JSON key (e.g., "eurusd")
      - 'instrument_name' is the value of "name" (e.g., "EUR/USD")
      - 'description' is the value of "description" (e.g., "Euro vs US Dollar")
      - Additional columns are also inserted.
      - 'group_ids' is determined from the groups in which the instrument appears.
    """
    group_mapping = {}
    if groups:
        for group in groups:
            group_id = group.get("id")
            instruments = group.get("instruments", [])
            for inst_id in instruments:
                group_mapping.setdefault(inst_id, []).append(group_id)

    inserted_count = 0

    def parse_ts(ts_str):
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return None

    async with pool.acquire() as conn:
        for inst_id, inst_info in meta_data.items():
            instrument_name = inst_info.get("name", "")
            description = inst_info.get("description", "")
            decimal_factor = inst_info.get("decimalFactor")
            start_hour = parse_ts(inst_info.get("startHourForTicks", ""))
            start_day = parse_ts(inst_info.get("startDayForMinuteCandles", ""))
            start_month = parse_ts(inst_info.get("startMonthForHourlyCandles", ""))
            start_year = parse_ts(inst_info.get("startYearForDailyCandles", ""))
            groups_for_inst = group_mapping.get(inst_id, [])

            query = """
                INSERT INTO data_provider_instruments 
                (pk_data_provider, instrument_id, instrument_name, description, decimal_factor, 
                 start_hour_for_ticks, start_day_for_minute_candles, start_month_for_hourly_candles, 
                 start_year_for_daily_candles, group_ids, comment)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT DO NOTHING;
            """
            await conn.execute(query, provider_id, inst_id, instrument_name, description, decimal_factor,
                               start_hour, start_day, start_month, start_year, groups_for_inst, "Imported via script")
            inserted_count += 1
    print(f"[INFO] Inserted {inserted_count} instrument(s).")

async def main():
    pool = await get_db_pool()
    await create_tables(pool)
    provider_name = "dukascopy"
    provider_source = META_DATA_URL
    provider_comment = "Imported from dukascopy-node meta data"
    provider_id = await get_or_create_data_provider(pool, provider_name, provider_source, provider_comment)
    print(f"[INFO] Provider ID: {provider_id}")

    meta_data = fetch_json(META_DATA_URL)
    if meta_data is None:
        return
    groups = fetch_json(GROUPS_URL)
    if groups is None:
        groups = []
    print(f"[INFO] Retrieved {len(meta_data)} instrument(s) from meta data.")
    print(f"[INFO] Retrieved {len(groups)} group(s).")

    await insert_instruments(pool, provider_id, meta_data, groups)
    print("[INFO] Import completed.")
    await pool.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())