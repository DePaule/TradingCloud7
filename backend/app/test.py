from importer import import_tick_data_range
from datetime import datetime, timedelta

def main():
    start_date = datetime(2025, 3, 1)  # 1. März 2025
    end_date = datetime(2025, 3, 27)   # 27. März 2025
    asset = "EURUSD"
    
    print(f"Importing tick data for {asset} from {start_date} to {end_date}...")
    total_inserted = import_tick_data_range(asset, start_date, end_date)
    
    print(f"Total ticks inserted: {total_inserted}")

if __name__ == "__main__":
    main()
