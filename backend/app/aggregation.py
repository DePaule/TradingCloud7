# TradingCloud7/backend/app/aggregation.py
import datetime

def aggregate_ticks(ticks: list, timeframe: str) -> list:
    """
    Aggregiert eine Liste von Tick-Daten (Tupel: (timestamp, bid, ask, bid_volume, ask_volume))
    zu Candles im gewünschten Timeframe.
    
    Unterstützte Timeframes (klein geschrieben): "s10", "m1", "m5", "m10", "m15", "m30", "h1", "h4", "d1".
    
    Rückgabe:
      Liste von Dictionaries mit:
        - timestamp
        - bid_open, bid_high, bid_low, bid_close
        - ask_open, ask_high, ask_low, ask_close
        - bid_volume, ask_volume, avg_spread
        - first_bid_ticks, first_ask_ticks (Listen mit bis zu 3 Werten)
    """
    timeframe_seconds = {
        "s10": 10,
        "m1": 60,
        "m5": 300,
        "m10": 600,
        "m15": 900,
        "m30": 1800,
        "h1": 3600,
        "h4": 14400,
        "d1": 86400
    }
    tf = timeframe.lower()
    if tf not in timeframe_seconds:
        raise ValueError("Nicht unterstütztes Timeframe")
    interval = timeframe_seconds[tf]

    buckets = {}
    for t, bid, ask, bid_vol, ask_vol in ticks:
        epoch = t.timestamp()
        bucket_epoch = epoch - (epoch % interval)
        bucket_time = datetime.datetime.fromtimestamp(bucket_epoch, tz=t.tzinfo)
        if bucket_time not in buckets:
            buckets[bucket_time] = {
                "timestamp": bucket_time,
                "bid_open": bid,
                "bid_high": bid,
                "bid_low": bid,
                "bid_close": bid,
                "ask_open": ask,
                "ask_high": ask,
                "ask_low": ask,
                "ask_close": ask,
                "bid_volume": bid_vol,
                "ask_volume": ask_vol,
                "spread_sum": ask - bid,
                "tick_count": 1,
                "first_bid_ticks": [bid],
                "first_ask_ticks": [ask]
            }
        else:
            c = buckets[bucket_time]
            c["bid_high"] = max(c["bid_high"], bid)
            c["bid_low"] = min(c["bid_low"], bid)
            c["bid_close"] = bid
            c["ask_high"] = max(c["ask_high"], ask)
            c["ask_low"] = min(c["ask_low"], ask)
            c["ask_close"] = ask
            c["bid_volume"] += bid_vol
            c["ask_volume"] += ask_vol
            c["spread_sum"] += (ask - bid)
            c["tick_count"] += 1
            if len(c["first_bid_ticks"]) < 3:
                c["first_bid_ticks"].append(bid)
            if len(c["first_ask_ticks"]) < 3:
                c["first_ask_ticks"].append(ask)
    
    candles = []
    for bucket_time, c in sorted(buckets.items()):
        c["avg_spread"] = c["spread_sum"] / c["tick_count"]
        del c["spread_sum"], c["tick_count"]
        candles.append(c)
    return candles
