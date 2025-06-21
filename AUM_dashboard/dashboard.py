import pandas as pd

# Read the CSV file
df = pd.read_csv('/root/aniket/M1.csv', encoding='latin1')

# Convert 'Monthwise' to datetime if it's not already
df['Monthwise'] = pd.to_datetime(df['Monthwise'], errors='coerce')

# Sort by 'Client_Name' and 'Monthwise'
df_sorted = df.sort_values(by=['Client_Name', 'Monthwise'])

# Reset index
df_sorted = df_sorted.reset_index(drop=True)

# Save the sorted DataFrame to a new CSV (optional)
df_sorted.to_csv('/root/aniket/M1_sorted.csv', index=False)

# Print the sorted DataFrame
print(df_sorted)