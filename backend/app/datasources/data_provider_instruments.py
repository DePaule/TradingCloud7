import os
import psycopg2
import requests
import json
from datetime import datetime

# Datenbank-URL aus der Umgebung, Standard: Container-Name "db" nutzen
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@db:5432/tradingcloud")

# URLs für die Instrument-Meta-Daten und die Gruppen (Beispiele aus dukascopy-node)
META_DATA_URL = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/refs/heads/master/src/utils/instrument-meta-data/generated/instrument-meta-data.json"
GROUPS_URL = "https://raw.githubusercontent.com/Leo4815162342/dukascopy-node/refs/heads/master/src/utils/instrument-meta-data/generated/instrument-groups.json"

def get_connection():
    return psycopg2.connect(DATABASE_URL)

def create_tables():
    """
    Erstellt die Tabellen data_provider und data_provider_instruments (mit zusätzlichen Spalten) falls sie noch nicht existieren.
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
        instrument_id TEXT NOT NULL,        -- z.B. "eurusd", "0941hkhkd"
        instrument_name TEXT NOT NULL,      -- z.B. "EUR/USD", "0941.HK/HKD"
        description TEXT,                   -- z.B. "Euro vs US Dollar", "China Mobile"
        decimal_factor INTEGER,
        start_hour_for_ticks TIMESTAMPTZ,
        start_day_for_minute_candles TIMESTAMPTZ,
        start_month_for_hourly_candles TIMESTAMPTZ,
        start_year_for_daily_candles TIMESTAMPTZ,
        group_ids TEXT[],
        comment TEXT
    );
    """
    conn = get_connection()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(create_provider_table)
        cur.execute(create_instruments_table)
    conn.close()

def get_or_create_data_provider(name_provider: str, source: str, comment: str = "") -> int:
    """
    Sucht nach einem bestehenden Provider oder erstellt einen neuen Eintrag.
    """
    conn = get_connection()
    conn.autocommit = True
    provider_id = None
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pk FROM data_provider 
            WHERE name_provider = %s AND source = %s
        """, (name_provider, source))
        result = cur.fetchone()
        if result:
            provider_id = result[0]
        else:
            cur.execute("""
                INSERT INTO data_provider (name_provider, source, comment)
                VALUES (%s, %s, %s)
                RETURNING pk
            """, (name_provider, source, comment))
            provider_id = cur.fetchone()[0]
    conn.close()
    return provider_id

def fetch_json(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[ERROR] Fehler beim Abrufen von {url}: {e}")
        return None

def insert_instruments(provider_id: int, meta_data: dict, groups: list):
    """
    Fügt alle Instrument-Meta-Daten in die Datenbank ein.
    Für jedes Instrument:
      - instrument_id ist der JSON-Key (z.B. "eurusd")
      - instrument_name ist der Wert von "name" (z.B. "EUR/USD")
      - description ist der Wert von "description" (z.B. "Euro vs US Dollar")
      - decimal_factor und Zeitstempel werden ebenfalls übernommen.
      - group_ids wird aus den Gruppen ermittelt, in denen das Instrument vorkommt.
    """
    # group_mapping: instrument_id -> Liste von Gruppennamen
    group_mapping = {}
    if groups:
        for group in groups:
            group_id = group.get("id")
            instruments = group.get("instruments", [])
            for inst_id in instruments:
                group_mapping.setdefault(inst_id, []).append(group_id)

    conn = get_connection()
    conn.autocommit = True
    inserted_count = 0

    def parse_ts(ts_str):
        try:
            # Ersetze "Z" durch "+00:00" für ISO-konforme Datumsverarbeitung
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return None

    with conn.cursor() as cur:
        # meta_data ist ein Dict mit Key=instrument_id, Value=Infos
        for inst_id, inst_info in meta_data.items():
            # z.B. inst_id = "eurusd"
            # z.B. inst_info = { "name": "EUR/USD", "description": "Euro vs US Dollar", ... }
            instrument_name = inst_info.get("name", "")  # z.B. "EUR/USD"
            description = inst_info.get("description", "")
            decimal_factor = inst_info.get("decimalFactor")
            start_hour = parse_ts(inst_info.get("startHourForTicks", ""))
            start_day = parse_ts(inst_info.get("startDayForMinuteCandles", ""))
            start_month = parse_ts(inst_info.get("startMonthForHourlyCandles", ""))
            start_year = parse_ts(inst_info.get("startYearForDailyCandles", ""))

            # Finde Gruppen, denen dieses Instrument zugeordnet ist
            groups_for_inst = group_mapping.get(inst_id, [])

            cur.execute("""
                INSERT INTO data_provider_instruments 
                (pk_data_provider, instrument_id, instrument_name, description, decimal_factor, 
                 start_hour_for_ticks, start_day_for_minute_candles, start_month_for_hourly_candles, 
                 start_year_for_daily_candles, group_ids, comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
            """, (provider_id,
                  inst_id,             # "eurusd"
                  instrument_name,     # "EUR/USD"
                  description,         # "Euro vs US Dollar"
                  decimal_factor,
                  start_hour,
                  start_day,
                  start_month,
                  start_year,
                  groups_for_inst,
                  "Imported via script"))
            inserted_count += cur.rowcount
    conn.close()
    print(f"[INFO] {inserted_count} Instrument(e) eingefügt.")

def main():
    create_tables()
    provider_name = "dukascopy"
    provider_source = META_DATA_URL
    provider_comment = "Imported from dukascopy-node meta data"
    provider_id = get_or_create_data_provider(provider_name, provider_source, provider_comment)
    print(f"[INFO] Provider-ID: {provider_id}")

    meta_data = fetch_json(META_DATA_URL)
    if meta_data is None:
        return
    groups = fetch_json(GROUPS_URL)
    if groups is None:
        groups = []
    print(f"[INFO] {len(meta_data)} Instrument(e) aus Meta-Daten abgerufen.")
    print(f"[INFO] {len(groups)} Gruppe(n) abgerufen.")

    insert_instruments(provider_id, meta_data, groups)
    print("[INFO] Import abgeschlossen.")

if __name__ == "__main__":
    main()
