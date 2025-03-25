import requests, struct
from datetime import datetime, timedelta, timezone
from lzma import LZMADecompressor, FORMAT_AUTO

def fetch_ticks(instr: str, date: datetime):
    all_ticks = []
    for hour in range(24):
        url = f"https://datafeed.dukascopy.com/datafeed/{instr}/{date.year:04d}/{(date.month - 1):02d}/{date.day:02d}/{hour:02d}h_ticks.bi5"
        try:
            r = requests.get(url, timeout=20)
            raw = LZMADecompressor(FORMAT_AUTO).decompress(r.content)
            base = datetime(date.year, date.month, date.day, hour, tzinfo=timezone.utc)
            for i in range(0, len(raw), 20):
                if len(raw[i:i+20]) < 20:
                    continue
                ms, bid_raw, ask_raw, bid_vol, ask_vol = struct.unpack("!IIIff", raw[i:i+20])
                t = base + timedelta(milliseconds=ms)
                bid = bid_raw / 100000
                ask = ask_raw / 100000
                all_ticks.append((t, bid, ask, bid_vol, ask_vol))
        except Exception:
            continue
    return sorted(all_ticks, key=lambda x: x[0])
