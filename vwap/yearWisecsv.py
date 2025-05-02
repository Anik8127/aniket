import pandas as pd
import os

# === CONFIG ===
input_csv_path = '/root/equityResearch/vwap/BacktestResults/NA_Vwap_Aniket_v1/1/closePnl_NA_Vwap_Aniket_v1_1.csv'  # Replace with your actual CSV file name
output_dir = 'year_wise_csvs'     # Output directory to save year-wise CSVs
datetime_column = 'Key'           # Your datetime column (used for year split)

# Create output directory if not exists
os.makedirs(output_dir, exist_ok=True)

# Read the CSV and parse the 'Key' column as datetime
df = pd.read_csv(input_csv_path, parse_dates=[datetime_column])

# Add a 'year' column based on 'Key'
df['year'] = df[datetime_column].dt.year

# Group by year and save each year’s data as a separate CSV
for year, group in df.groupby('year'):
    output_path = os.path.join(output_dir, f'{year}.csv')
    group.drop(columns='year').to_csv(output_path, index=False)
    print(f'Saved: {output_path}')
