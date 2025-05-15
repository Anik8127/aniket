from backtestTools.expiry import getExpiryData
from backtestTools.histData import getFnoBacktestData
import numpy as np
import pandas as pd
from datetime import datetime

start_date = datetime(2025, 5, 1, 9, 15)
end_date = datetime(2025, 5, 13, 15, 00)
baseSym = "NIFTY"

# Get expiry info
expiry_info = getExpiryData(start_date.timestamp(), baseSym)
print(expiry_info)
current_future_expiry = expiry_info['CurrentFutureExpiry']

# Generate 1-minute interval timestamps
dt_range = pd.date_range(start=start_date, end=end_date, freq='1min')
df = pd.DataFrame({'Datetime': dt_range})
df['CurrentFutureExpiry'] = current_future_expiry
baseExp = baseSym + current_future_expiry

# Get OHLC data for all futures
ohlc_df = getFnoBacktestData(baseExp, start_date.timestamp(), end_date.timestamp(), "1Min")
# Save DataFrame to CSV
ohlc_df['symbol'] = baseExp
ohlc_df.to_csv('current_future_expiry_1min.csv', index=False)

# if ohlc_df is not None:
#     ohlc_df = ohlc_df.reset_index()
#     ohlc_df['Datetime'] = pd.to_datetime(ohlc_df['index'], unit='s')
#     # Filter for current future expiry if expiry column exists
#     if 'expiry' in ohlc_df.columns:
#         ohlc_df = ohlc_df[ohlc_df['expiry'] == current_future_expiry]
#     df = pd.merge(df, ohlc_df[['Datetime', 'c']], on='Datetime', how='left')

# # Save DataFrame to CSV
# df.to_csv('current_future_expiry_1min.csv', index=False)

# print("CSV file 'current_future_expiry_1min.csv' created successfully.")
# print(df.head())