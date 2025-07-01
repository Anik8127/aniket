import pandas as pd

# Read the CSV file
df = pd.read_csv('/root/aniket/AUM_dashboard/M1.csv', encoding='latin1')

# Convert 'Monthwise' to datetime if it's not already
df['Monthwise'] = pd.to_datetime(df['Monthwise'], errors='coerce')

# Create a new 'Year' column from 'Monthwise'
df['Year'] = df['Monthwise'].dt.year

# Sort by 'Client_Name' and 'Monthwise'
df_sorted = df.sort_values(by=['Client_Name', 'Monthwise'])

# Reset index
df_sorted = df_sorted.reset_index(drop=True)

# Move all rows with Year == 2025 to the end
df_2025 = df_sorted[df_sorted['Year'] == 2025]
df_not_2025 = df_sorted[df_sorted['Year'] != 2025]
df_final = pd.concat([df_not_2025, df_2025], ignore_index=True)

# Save the sorted DataFrame to a new CSV (optional)
df_final.to_csv('/root/aniket/M1_sorted.csv', index=False)

# Print the sorted DataFrame
print(df_final)