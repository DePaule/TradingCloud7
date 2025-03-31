import requests
import struct
from lzma import LZMADecompressor, FORMAT_AUTO
from datetime import datetime, timedelta

def fetch_tick_data(instr, yr, m, d, h):
    """
    Downloads raw tick data for the specified instrument and time from Dukascopy.
    """
    url = f"https://datafeed.dukascopy.com/datafeed/{instr.upper()}/{yr:04d}/{(m-1):02d}/{d:02d}/{h:02d}h_ticks.bi5"
    response = requests.get(url)
    response.raise_for_status()
    raw = response.content
    try:
        data = LZMADecompressor(FORMAT_AUTO).decompress(raw)
    except Exception:
        data = raw
    return data

def parse_ticks(data, base_time):
    """
    Parses the binary tick data and returns a list of tick records.
    
    Each tick record is a tuple: (timestamp, bid, ask, bid_volume, ask_volume)
    """
    ticks = []
    rec_size = 20  # Each record is 20 bytes.
    for i in range(0, len(data), rec_size):
        rec = data[i:i+rec_size]
        if len(rec) < rec_size:
            break
        ms, bid_raw, ask_raw, bid_vol, ask_vol = struct.unpack("!IIIff", rec)
        t = base_time + timedelta(milliseconds=ms)
        bid = bid_raw / 100000.0
        ask = ask_raw / 100000.0
        ticks.append((t, bid, ask, bid_vol, ask_vol))
    return ticks