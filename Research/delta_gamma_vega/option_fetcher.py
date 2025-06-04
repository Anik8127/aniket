from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from datetime import datetime, timedelta
import pandas as pd

def get_expiry_list(baseSym, startDate, endDate):
    # Collect all unique expiry strings between startDate and endDate
    expiry_set = set()
    current = startDate
    while current <= endDate:
        expiry_info = getExpiryData(current.timestamp(), baseSym)
        if expiry_info is not None:
            expiry_str = expiry_info.get('CurrentExpiry')
            if expiry_str:
                expiry_set.add(expiry_str)
        current += timedelta(days=1)
    return sorted(expiry_set)

def get_option_symbol(baseSym, expiry_str, strike, option_type):
    # Correct format: NIFTY30JAN2524750CE
    return f"{baseSym}{expiry_str}{int(strike)}{option_type}"

if __name__ == "__main__":
    baseSym = "NIFTY"
    indexSym = "NIFTY 50"
    strike = 24750  # <-- Set your strike here
    option_types = ["CE", "PE"]

    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 5, 31, 15, 30)

    # Dynamically get all expiry symbols in the date range
    expiry_list = get_expiry_list(baseSym, startDate, endDate)
    print("Available expiry symbols:", expiry_list)

    found_any = False

    # Fetch index (spot) data once for the whole range
    spot_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "1Min")
    if spot_df is None or spot_df.empty:
        print("No spot data found for index.")
        exit()

    # Prepare spot data datetime and date columns
    if 't' in spot_df.columns:
        spot_df['datetime'] = pd.to_datetime(spot_df['t'], unit='s')
    else:
        spot_df['datetime'] = pd.to_datetime(spot_df.index, unit='s')
    spot_df['date'] = spot_df['datetime'].dt.date

    # Find all dates where the index close price matches the strike
    match_dates = spot_df[spot_df['c'] == strike]['date'].unique()

    if len(match_dates) == 0:
        print(f"No dates found where index close price matches strike {strike}.")
        exit()

    print(f"Dates where index close == strike {strike}: {match_dates}")

    for expiry_str in expiry_list:
        for option_type in option_types:
            sym = get_option_symbol(baseSym, expiry_str, strike, option_type)
            df = getFnoBacktestData(sym, startDate.timestamp(), endDate.timestamp(), "1Min")

            if df is not None and not df.empty:
                df['symbol'] = sym

                # Prepare option data datetime and date columns
                if 't' in df.columns:
                    df['datetime'] = pd.to_datetime(df['t'], unit='s')
                else:
                    df['datetime'] = pd.to_datetime(df.index, unit='s')
                df['date'] = df['datetime'].dt.date

                # For each matching date, save option data for that day
                for day in match_dates:
                    day_df = df[df['date'] == day]
                    if not day_df.empty:
                        outname = f"{sym}_{day}_1min.csv"
                        day_df.to_csv(outname, index=False)
                        print(f"Saved {sym} 1-min data for {day} to {outname}")
                        found_any = True
            else:
                print(f"No data found for {sym}")

    if not found_any:
        print("No option data found for any expiry/strike/date combination where index == strike.")