from backtestTools.histData import getFnoBacktestData
from datetime import datetime
import pandas as pd

def get_option_symbol(baseSym, expiry_str, strike, option_type):
    return f"{baseSym}{expiry_str}{int(strike)}{option_type}"

if __name__ == "__main__":
    baseSym = "NIFTY"
    indexSym = "NIFTY 50"
    expiry_str = "08MAY25"
    strike = 24450
    chosen_day = pd.to_datetime("2025-05-07").date()
    option_types = ["CE", "PE"]

    startDate = datetime(2025, 5, 7, 9, 15)
    endDate = datetime(2025, 5, 7, 15, 30)

    # Fetch NIFTY spot data at 5-min interval and convert to IST
    spot_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "5Min")
    if spot_df is None or spot_df.empty:
        print("No spot data found for index.")
        exit()

    if 't' in spot_df.columns:
        spot_df['datetime'] = pd.to_datetime(spot_df['t'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
    else:
        spot_df['datetime'] = pd.to_datetime(spot_df.index, unit='s', utc=True).tz_convert('Asia/Kolkata')
    spot_df['datetime_min'] = spot_df['datetime'].dt.floor('5T')
    spot_df['date'] = spot_df['datetime_min'].dt.date

    for option_type in option_types:
        sym = get_option_symbol(baseSym, expiry_str, strike, option_type)
        df = getFnoBacktestData(sym, startDate.timestamp(), endDate.timestamp(), "5Min")
        if df is not None and not df.empty:
            df['symbol'] = sym
            # Convert option timestamps to IST
            if 't' in df.columns:
                df['datetime'] = pd.to_datetime(df['t'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
            else:
                df['datetime'] = pd.to_datetime(df.index, unit='s', utc=True).tz_convert('Asia/Kolkata')
            df['datetime_min'] = df['datetime'].dt.floor('5T')
            df['date'] = df['datetime_min'].dt.date
            day_df = df[df['date'] == chosen_day]
            if not day_df.empty:
                # Merge on floored IST 5-min
                merged = pd.merge(
                    day_df,
                    spot_df[['datetime_min', 'c']].rename(columns={'c': 'nifty_close'}),
                    on='datetime_min',
                    how='left'
                )
                merged = merged.drop(columns=['date'])
                outname = f"{sym}_{chosen_day}_5min.csv"
                merged.to_csv(outname, index=False)
                print(f"Saved {sym} 5-min data for {chosen_day} to {outname}")
            else:
                print(f"No data for {sym} on {chosen_day}")
        else:
            print(f"No data found for {sym}")