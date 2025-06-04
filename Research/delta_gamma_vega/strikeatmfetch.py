from backtestTools.histData import getFnoBacktestData
from datetime import datetime
import pandas as pd

def fetch_and_prepare(symbol, start_ts, end_ts):
    df = getFnoBacktestData(symbol, start_ts, end_ts, "1Min")
    if df is None or df.empty:
        return None
    if 't' in df.columns:
        df['datetime'] = pd.to_datetime(df['t'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
    else:
        df['datetime'] = pd.to_datetime(df.index, unit='s', utc=True).tz_convert('Asia/Kolkata')
    df = df[['datetime', 'c']]
    df = df.rename(columns={'c': f'c_{symbol}'})
    return df

if __name__ == "__main__":
    baseSym = "NIFTY"
    indexSym = "NIFTY 50"
    expiry_str = "05JUN25"
    strike1 = 24700
    strike2 = 24750
    chosen_day = pd.to_datetime("2025-06-02").date()

    startDate = datetime(2025, 6, 2, 9, 15)
    endDate = datetime(2025, 6, 2, 15, 30)
    start_ts = startDate.timestamp()
    end_ts = endDate.timestamp()

    # Symbols
    ce1 = f"{baseSym}{expiry_str}{strike1}CE"
    ce2 = f"{baseSym}{expiry_str}{strike2}CE"
    pe1 = f"{baseSym}{expiry_str}{strike1}PE"
    pe2 = f"{baseSym}{expiry_str}{strike2}PE"

    # Fetch data
    ce1_df = fetch_and_prepare(ce1, start_ts, end_ts)
    ce2_df = fetch_and_prepare(ce2, start_ts, end_ts)
    pe1_df = fetch_and_prepare(pe1, start_ts, end_ts)
    pe2_df = fetch_and_prepare(pe2, start_ts, end_ts)

    # Fetch NIFTY spot data
    spot_df = getFnoBacktestData(indexSym, start_ts, end_ts, "1Min")
    if spot_df is None or spot_df.empty:
        print("No spot data found for index.")
        exit()
    if 't' in spot_df.columns:
        spot_df['datetime'] = pd.to_datetime(spot_df['t'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
    else:
        spot_df['datetime'] = pd.to_datetime(spot_df.index, unit='s', utc=True).tz_convert('Asia/Kolkata')
    spot_df = spot_df[['datetime', 'c']].rename(columns={'c': 'nifty_close'})

    # Merge for CE
    ce_merge = pd.merge(ce1_df, ce2_df, on='datetime', how='outer')
    ce_merge = pd.merge(ce_merge, spot_df, on='datetime', how='left')
    ce_merge = ce_merge[(ce_merge['datetime'].dt.date == chosen_day)]
    ce_merge.insert(1, 'symbol_1', ce1)
    ce_merge.insert(3, 'symbol_2', ce2)
    ce_merge = ce_merge.rename(columns={f'c_{ce1}': 'c_1', f'c_{ce2}': 'c_2'})
    ce_merge = ce_merge[['datetime', 'symbol_1', 'c_1', 'symbol_2', 'c_2', 'nifty_close']]
    ce_merge.to_csv(f"{ce1}_{ce2}_{chosen_day}_1min.csv", index=False)
    print(f"Saved CE data to {ce1}_{ce2}_{chosen_day}_1min.csv")

    # Merge for PE
    pe_merge = pd.merge(pe1_df, pe2_df, on='datetime', how='outer')
    pe_merge = pd.merge(pe_merge, spot_df, on='datetime', how='left')
    pe_merge = pe_merge[(pe_merge['datetime'].dt.date == chosen_day)]
    pe_merge.insert(1, 'symbol_1', pe1)
    pe_merge.insert(3, 'symbol_2', pe2)
    pe_merge = pe_merge.rename(columns={f'c_{pe1}': 'c_1', f'c_{pe2}': 'c_2'})
    pe_merge = pe_merge[['datetime', 'symbol_1', 'c_1', 'symbol_2', 'c_2', 'nifty_close']]
    pe_merge.to_csv(f"{pe1}_{pe2}_{chosen_day}_1min.csv", index=False)
    print(f"Saved PE data to {pe1}_{pe2}_{chosen_day}_1min.csv")