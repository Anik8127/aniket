import pandas as pd

# File paths
file1 = '/root/aniket/Research/Synthetic_long_arbitrage/current_future_expiry_1min.csv'
file2 = '/root/aniket/Research/Synthetic_long_arbitrage/synthetic.csv'

# Read the CSV files
df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Select columns "c" and "symbol" from the first file
df1_selected = df1[['c', 'symbol']]

# If you want to merge on index (row-wise), reset index to align
df1_selected = df1_selected.reset_index(drop=True)
df2 = df2.reset_index(drop=True)

# Concatenate the columns to the second dataframe
df_merged = pd.concat([df2, df1_selected], axis=1)

# Save the result
df_merged.to_csv('/root/aniket/Research/Synthetic_long_arbitrage/synthetic_with_c_symbol.csv', index=False)

print("Columns 'c' and 'symbol' added to synthetic.csv and saved as synthetic_with_c_symbol.csv")