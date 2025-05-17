import pandas as pd

# Read the source CSV
df_source = pd.read_csv('/root/aniket/Research/Synthetic_long_arbitrage/current_future_expiry_1min.csv', usecols=['c', 'symbol'])
df_source.drop(columns=['ti'], inplace=True)
print(df_source.columns)