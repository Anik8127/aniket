import pandas as pd
import os

# Folder containing CSV files
folder_path = "/root/aniket/vwap/BacktestResults/NA_Vwap_Aniket_v1/1/ClosePnlCsv"

# Get list of all CSV files in the folder
csv_files = [file for file in os.listdir(folder_path) if file.endswith(".csv")]

# Combine all CSVs into one dataframe
df_combined = pd.concat([pd.read_csv(os.path.join(folder_path, file)) for file in csv_files])

# Save the combined file
df_combined.to_csv("combined_output.csv", index=False)

print("All CSV files have been merged successfully!")