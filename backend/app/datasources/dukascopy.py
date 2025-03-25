# TradingCloud7/backend/app/datasources/dukascopy.py
import datetime
import requests
import struct
from lzma import LZMADecompressor, FORMAT_AUTO

def fetch_tick_data(asset: str, year: int, month: int, day: int, hour: int) -> bytes:
    url = f"https://datafeed.dukascopy.com/datafeed/{asset}/{year:04d}/{(month-1):02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    print(f"DEBUG: Fetching tick data from URL: {url}")  
    response = requests.get(url)
    response.raise_for_status()
    raw = response.content
    try:
        data = LZMADecompressor(FORMAT_AUTO).decompress(raw)
    except Exception as e:
        # Protokolliere das Problem und werfe einen aussagekräftigen Fehler:
        raise Exception(f"Decompression failed for {url}: {e}. Raw data (first 100 bytes): {raw[:100].hex()}")
    return data
def parse_ticks(data: bytes, base_time: datetime.datetime) -> list:
    """
    Parst die Binärdaten in eine Liste von Tupeln:
      (timestamp, bid, ask, bid_volume, ask_volume)
    """
    ticks = []
    rec_size = 20
    for i in range(0, len(data), rec_size):
        rec = data[i:i+rec_size]
        if len(rec) < rec_size:
            break
        ms, bid_raw, ask_raw, bid_vol, ask_vol = struct.unpack("!IIIff", rec)
        t = base_time + datetime.timedelta(milliseconds=ms)
        bid = bid_raw / 100000.0
        ask = ask_raw / 100000.0
        ticks.append((t, bid, ask, bid_vol, ask_vol))
    return ticks

def fetch_day_ticks(asset: str, year: int, month: int, day: int) -> list:
    ticks = []
    for hour in range(24):
        base = datetime.datetime(year, month, day, hour, tzinfo=datetime.timezone.utc)
        try:
            data = fetch_tick_data(asset, year, month, day, hour)
            ticks.extend(parse_ticks(data, base))
        except Exception as e:
            print(f"Error {year}-{month:02d}-{day:02d} {hour:02d}h for {asset}: {e}")
    ticks.sort(key=lambda x: x[0])
    return ticks

def fetch_ticks_for_date_range(asset: str, start_date: datetime.date, end_date: datetime.date) -> list:
    ticks = []
    current_date = start_date
    while current_date <= end_date:
        day_ticks = fetch_day_ticks(asset, current_date.year, current_date.month, current_date.day)
        ticks.extend(day_ticks)
        current_date += datetime.timedelta(days=1)
    return ticks
