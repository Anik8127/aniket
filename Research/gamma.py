import pandas as pd

# Load your data
df = pd.read_csv('NIFTY_202501_24000CE_and_Index_5min.csv')

# Convert epoch (UTC) to datetime in IST (UTC+5:30)
df['datetime_IST'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')

# Calculate Delta
df['delta'] = (df['call_close'] - df['call_close'].shift(1)) / (df['nifty_close'] - df['nifty_close'].shift(1))
df['delta'] = df['delta'].clip(lower=0, upper=1)  # Keep Delta in [0, 1]

# Calculate Gamma
df['gamma'] = (df['delta'] - df['delta'].shift(1)) / (df['nifty_close'] - df['nifty_close'].shift(1))
df['gamma'] = df['gamma'].clip(lower=0)  # Gamma should be >= 0

df.to_csv('delta_gamma.csv', index=False)
