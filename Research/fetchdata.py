import numpy as np
import talib as ta
import pandas as pd
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData

def extract_monthly_call_and_index_close(startDate, endDate, baseSym, indexSym, strike=25000):
    # 1. Get monthly expiry for the start date
    expiry_info = getExpiryData(startDate.timestamp(), baseSym)
    monthly_expiry = expiry_info['MonthlyExpiry']
    expiry_date = datetime.strptime(monthly_expiry, "%d%b%y").replace(hour=15, minute=30)

    # 2. Build call symbol for the monthly expiry at strike 25000
    call_symbol = f"{baseSym}{expiry_date.strftime('%y%b').upper()}CE{strike}"  # Use 'CE' for call option
    print("Fetching data for call symbol:", call_symbol)

    # 3. Fetch 5-min data for the call option and NIFTY 50
    call_df = getFnoBacktestData(call_symbol, startDate.timestamp(), endDate.timestamp(), "5Min")
    index_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "5Min")

    # Check if data is fetched
    if call_df is None or index_df is None:
        print("Data not found for:", call_symbol, "or", indexSym)
        return

    # 4. Drop missing values
    call_df = call_df.dropna(subset=["c"])
    index_df = index_df.dropna(subset=["c"])

    # 5. Align both DataFrames on timestamp
    merged = pd.DataFrame({
        "timestamp": call_df.index,
        "call_close": call_df["c"].values,
        "nifty_close": index_df.reindex(call_df.index)["c"].values
    })

    # 6. Save to CSV
    merged.to_csv(f"MonthlyExpiry_{call_symbol}_NIFTY50_Close.csv", index=False)
    print("Saved:", f"MonthlyExpiry_{call_symbol}_NIFTY50_Close.csv")

def fetch_april_2024_nifty_call_and_index():
    # Fixed parameters for April 2024
    startDate = datetime(2024, 4, 1, 9, 15)
    endDate = datetime(2024, 4, 30, 15, 30)
    baseSym = "NIFTY"
    indexSym = "NIFTY 50"
    strike = 22500  # Change this if you want a different strike

    # Get April 2024 expiry
    expiry_info = getExpiryData(startDate.timestamp(), baseSym)
    monthly_expiry = expiry_info['MonthlyExpiry']
    expiry_date = datetime.strptime(monthly_expiry, "%d%b%y").replace(hour=15, minute=30)

    # Build call symbol (adjust if your data uses a different format)
    call_symbol = f"{baseSym}{expiry_date.strftime('%y%b').upper()}CE{strike}"
    print("Fetching data for call symbol:", call_symbol)

    # Fetch 5-min data
    call_df = getFnoBacktestData(call_symbol, startDate.timestamp(), endDate.timestamp(), "5Min")
    index_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "5Min")

    if call_df is None or index_df is None:
        print("Data not found for:", call_symbol, "or", indexSym)
        return

    call_df = call_df.dropna(subset=["c"])
    index_df = index_df.dropna(subset=["c"])

    merged = pd.DataFrame({
        "timestamp": call_df.index,
        "call_close": call_df["c"].values,
        "nifty_close": index_df.reindex(call_df.index)["c"].values
    })

    merged.to_csv(f"NIFTY_Apr2024_{strike}CE_and_Index_5min.csv", index=False)
    print("Saved: NIFTY_Apr2024_{strike}CE_and_Index_5min.csv")

def try_symbol_formats(baseSym, expiry_date, strike):
    # Try common symbol formats for NIFTY options
    formats = [
        f"{baseSym}{expiry_date.strftime('%y%b').upper()}CE{strike}",
        f"{baseSym}{expiry_date.strftime('%y%b').upper()}C{strike}",
        f"{baseSym}{expiry_date.strftime('%d%b%y').upper()}CE{strike}",
        f"{baseSym}{expiry_date.strftime('%d%b%y').upper()}C{strike}",
        f"{baseSym}{expiry_date.strftime('%y%b').upper()}{strike}CE",
        f"{baseSym}{expiry_date.strftime('%y%b').upper()}{strike}C",
        f"{baseSym}{expiry_date.strftime('%d%b%y').upper()}{strike}CE",
        f"{baseSym}{expiry_date.strftime('%d%b%y').upper()}{strike}C",
    ]
    return formats

def fetch_monthly_nifty_call_and_index(start_year=2025, start_month=1, end_year=2025, end_month=4, strike=24000):
    baseSym = "NIFTY"
    indexSym = "NIFTY 50"
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == end_year and month > end_month:
                break
            if year == start_year and month < start_month:
                continue
            # 1st day and last day of the month
            startDate = datetime(year, month, 1, 9, 15)
            if month == 12:
                endDate = datetime(year + 1, 1, 1, 9, 15) - pd.Timedelta(minutes=5)
            else:
                endDate = datetime(year, month + 1, 1, 9, 15) - pd.Timedelta(minutes=5)
            # Get expiry info
            expiry_info = getExpiryData(startDate.timestamp(), baseSym)
            monthly_expiry = expiry_info.get('MonthlyExpiry')
            if not monthly_expiry:
                print(f"No expiry info for {year}-{month:02d}")
                continue
            expiry_date = datetime.strptime(monthly_expiry, "%d%b%y").replace(hour=15, minute=30)
            # Try all symbol formats
            found = False
            for call_symbol in try_symbol_formats(baseSym, expiry_date, strike):
                print(f"Trying symbol: {call_symbol}")
                call_df = getFnoBacktestData(call_symbol, startDate.timestamp(), endDate.timestamp(), "5Min")
                index_df = getFnoBacktestData(indexSym, startDate.timestamp(), endDate.timestamp(), "5Min")
                if call_df is not None and index_df is not None and not call_df.empty and not index_df.empty:
                    call_df = call_df.dropna(subset=["c"])
                    index_df = index_df.dropna(subset=["c"])
                    merged = pd.DataFrame({
                        "timestamp": call_df.index,
                        "call_close": call_df["c"].values,
                        "nifty_close": index_df.reindex(call_df.index)["c"].values
                    })
                    fname = f"NIFTY_{year}{month:02d}_{strike}CE_and_Index_5min.csv"
                    merged.to_csv(fname, index=False)
                    print(f"Saved: {fname}")
                    found = True
                    break
            if not found:
                print(f"No data found for {year}-{month:02d} at strike {strike}")

# Example usage:
if __name__ == "__main__":
    startDate = datetime(2025, 4, 1, 9, 15)
    endDate = datetime(2025, 4, 22, 15, 30)
    baseSym =  "NIFTY"
    indexSym = "NIFTY 50"
    extract_monthly_call_and_index_close(startDate, endDate, baseSym, indexSym, strike=24000)
    fetch_april_2024_nifty_call_and_index()
    fetch_monthly_nifty_call_and_index(start_year=2025, start_month=1, end_year=2025, end_month=4, strike=24000)