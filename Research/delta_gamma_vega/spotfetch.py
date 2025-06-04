from backtestTools.histData import getFnoBacktestData
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    indexSym = "NIFTY 50"
    startDate = datetime(2025, 5, 7, 9, 15)
    endDate = datetime(2025, 5, 7, 15, 30)

    # Fetch NIFTY spot data at 1-min interval
    spot_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "1Min")
    if spot_df is None or spot_df.empty:
        print("No spot data found for index.")
        exit()

    # Convert timestamp to IST datetime
    if 't' in spot_df.columns:
        spot_df['datetime'] = pd.to_datetime(spot_df['t'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
    else:
        spot_df['datetime'] = pd.to_datetime(spot_df.index, unit='s', utc=True).tz_convert('Asia/Kolkata')

    # Keep only datetime and closing price "c"
    result = spot_df[['datetime', 'c']]

    # Save to CSV
    result.to_csv("NIFTY_spot_1min_close.csv", index=False)
    print("Saved NIFTY spot 1-min closing prices to NIFTY_spot_1min_close.csv")