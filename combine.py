import os
import pandas as pd

def combine_csv_files(folder_path, output_file):
    """
    Combine all CSV files in a folder into a single DataFrame and save to a new CSV file.

    Args:
        folder_path (str): Path to the folder containing the CSV files.
        output_file (str): Path to save the combined CSV file.
    """
    # List to store individual DataFrames
    data_frames = []

    # Iterate through all files in the folder
    for file_name in os.listdir(folder_path):
        # Check if the file is a CSV file
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            print(f"Processing file: {file_path}")
            
            # Read the CSV file into a DataFrame
            df = pd.read_csv(file_path)
            data_frames.append(df)

    # Combine all DataFrames into a single DataFrame
    combined_df = pd.concat(data_frames, ignore_index=True)

    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_file, index=False)
    print(f"Combined CSV file saved to: {output_file}")

# Main function
if __name__ == "__main__":
    # Path to the folder containing CSV files
    folder_path = "/root/aniket/equityDelta/BacktestResults/NA_equityDelta_v1/5/ClosePnlCsv"
    
    # Path to save the combined CSV file
    output_file = "/root/aniket/equityDelta/BacktestResults/combined_resultsfeb.csv"
    
    # Combine all CSV files in the folder
    combine_csv_files(folder_path, output_file)