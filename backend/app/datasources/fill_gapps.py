import os
import psycopg2
from datetime import datetime, timedelta
from dukascopy import fetch_tick_data, parse_ticks

# Datenbank-URL (anpassen, falls nötig)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://trader:trader@localhost:5432/tradingcloud")

def find_gaps(table_name: str, start: datetime, end: datetime, gap_threshold=timedelta(minutes=10)):
    """
    Sucht in der Tabelle nach Zeitlücken, in denen der Abstand zwischen aufeinanderfolgenden Ticks
    größer als das gap_threshold ist.
    Gibt eine Liste von Tupeln (prev_timestamp, current_timestamp) zurück.
    """
    conn = psycopg2.connect(DATABASE_URL)
    with conn.cursor() as cur:
        query = f"""
        WITH cte AS (
            SELECT timestamp,
                   LAG(timestamp) OVER (ORDER BY timestamp) AS prev_ts
            FROM {table_name}
            WHERE timestamp BETWEEN %s AND %s
        )
        SELECT prev_ts, timestamp
        FROM cte
        WHERE (timestamp - prev_ts) > %s;
        """
        cur.execute(query, (start, end, gap_threshold))
        gaps = cur.fetchall()
    conn.close()
    return gaps

def adjust_gap_interval(gap_start: datetime, gap_end: datetime) -> (datetime, datetime):
    """
    Passt den zu importierenden Intervall an, wenn der Gap ein Wochenend-Zeitraum zugeordnet ist.
    Beispiel: Wenn gap_start an einem Freitag (weekday 4) und gap_end an einem Montag (weekday 0)
    liegt, dann wird nur ein kleiner Zeitraum (z. B. 1 Stunde nach gap_start bis 1 Stunde vor gap_end)
    zurückgegeben.
    """
    if gap_start.weekday() == 4 and gap_end.weekday() == 0:
        # Wochenend-Gap: Nur einen kleinen Zeitraum abfragen
        fetch_start = gap_start + timedelta(hours=1)  # z. B. 1 Stunde nach gap_start
        fetch_end = gap_end - timedelta(hours=1)      # z. B. 1 Stunde vor gap_end
        return fetch_start, fetch_end
    else:
        # Andernfalls kompletten Gap abfragen
        return gap_start, gap_end

def bulk_insert_ticks(table_name: str, ticks: list, conn) -> int:
    """
    Fügt die übergebenen Tick-Daten als Bulk-Insert in die Tabelle ein.
    Jeder Tick ist ein Tupel: (timestamp, bid, ask, bid_volume, ask_volume)
    """
    if not ticks:
        return 0
    # Baue den INSERT-String mit Platzhaltern
    values_clause = []
    params = []
    for tick in ticks:
        values_clause.append("(%s, %s, %s, %s, %s)")
        params.extend(tick)
    query = (
        f"INSERT INTO {table_name} (timestamp, bid, ask, bid_volume, ask_volume) VALUES " +
        ", ".join(values_clause) +
        " ON CONFLICT (timestamp) DO NOTHING;"
    )
    with conn.cursor() as cur:
        cur.execute(query, params)
        inserted = cur.rowcount
    conn.commit()
    return inserted

def close_gap_for_interval(table_name: str, gap_start: datetime, gap_end: datetime) -> int:
    """
    Schließt eine einzelne Lücke: Passt das Intervall mit adjust_gap_interval an,
    und für jede volle Stunde im angepassten Intervall werden Tick-Daten von Dukascopy abgeholt
    und per Bulk-Insert eingefügt.
    """
    fetch_start, fetch_end = adjust_gap_interval(gap_start, gap_end)
    print(f"Adjusted fetch interval: {fetch_start} bis {fetch_end}")
    total_inserted = 0
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    current = fetch_start.replace(minute=0, second=0, microsecond=0)
    while current <= fetch_end:
        try:
            print(f"Fetching data for hour {current}...")
            raw_data = fetch_tick_data("eurusd", current.year, current.month, current.day, current.hour)
            ticks = parse_ticks(raw_data, current)
            inserted = bulk_insert_ticks(table_name, ticks, conn)
            print(f"Inserted {inserted} ticks for hour {current}")
            total_inserted += inserted
        except Exception as e:
            print(f"Error fetching/inserting data for hour {current}: {e}")
        current += timedelta(hours=1)
    conn.close()
    return total_inserted

def fill_gaps_for_table(table_name: str, start: datetime, end: datetime):
    """
    Ermittelt alle Lücken (Gaps) in der Tick-Tabelle im angegebenen Zeitraum und versucht,
    diese mithilfe von Dukascopy-Daten zu füllen.
    """
    gaps = find_gaps(table_name, start, end)
    print(f"Found {len(gaps)} gap(s) in {table_name}:")
    for gap in gaps:
        gap_start, gap_end = gap
        print(f"Processing gap from {gap_start} to {gap_end}")
        inserted = close_gap_for_interval(table_name, gap_start, gap_end)
        print(f"Total ticks inserted for this gap: {inserted}")

if __name__ == "__main__":
    # Beispiel: Schließe Lücken in der eurusd_tick Tabelle zwischen 1.1.2025 und 1.4.2025
    table = "eurusd_tick"
    start_time = datetime(2025, 1, 1)
    end_time = datetime(2025, 4, 1)
    fill_gaps_for_table(table, start_time, end_time)
