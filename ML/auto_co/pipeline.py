import pandas as pd
from statsmodels.tsa.stattools import acf, adfuller
import matplotlib.pyplot as plt
import os
import glob
from tqdm import tqdm
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

def process_file(file_path):
    results = []

    try:
        # Read CSV file
        df = pd.read_csv(file_path)

        # Check if required columns exist
        if 'c' not in df.columns:
            print(f"Skipping {file_path}: 'c' column not found.")
            return results

        # Convert to numeric and clean
        df['c'] = pd.to_numeric(df['c'], errors='coerce')
        df.dropna(subset=['c'], inplace=True)

        if df.empty:
            print(f"Skipping {file_path}: No valid data.")
            return results

        # Generate timestamps if not present
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
        else:
            dates = pd.date_range(
                start=pd.Timestamp.now().normalize() + pd.Timedelta(hours=9, minutes=15),
                periods=len(df),
                freq='1T'
            )
            df.index = dates

        # Define timeframes to test
        timeframes = [1, 5, 15, 20, 30, 60]
        timeframes.extend(list(range(75, 3750, 15)))

        for timeframe in timeframes:
            # Resample data
            if timeframe == 1:
                resampled = df['c'].copy()
            else:
                resampled = df['c'].resample(f'{timeframe}T').last().dropna()

            # Calculate returns
            returns = resampled.pct_change().dropna()

            if len(returns) < 10:  # Minimum data points required
                continue

            # Perform ADF test for stationarity
            adf_result = adfuller(returns)
            if adf_result[1] > 0.05:  # If p-value is greater than 0.05, data is not stationary
                continue

            # Calculate autocorrelation at lag 1
            autocorr = acf(returns, nlags=1, fft=True)[1]

            results.append({
                'file': os.path.basename(file_path),
                'timeframe': timeframe,
                'autocorrelation': autocorr,
                'lag': 1  # Since we are calculating autocorrelation at lag 1
            })

            # Check if we should stop processing larger timeframes
            if abs(autocorr) > 0.6:
                # Plot and save the significant result
                plt.figure(figsize=(10, 5))
                plt.plot(returns, label=f'{timeframe} min returns')
                plt.title(f"{os.path.basename(file_path)} - {timeframe} min (Autocorr: {autocorr:.2f})")
                plt.legend()
                plot_name = f"{os.path.splitext(os.path.basename(file_path))[0]}_{timeframe}min_returns.jpg"
                plt.savefig(plot_name, dpi=300, bbox_inches='tight')
                plt.close()
                break

    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

    return results

def analyze_folder(folder_path):
    # Get all CSV files in the folder
    files = glob.glob(os.path.join(folder_path, '*.csv'))

    # Use ThreadPoolExecutor to read files in parallel
    with ThreadPoolExecutor() as executor:
        # Use a dictionary to store futures and their corresponding file paths
        futures = {executor.submit(pd.read_csv, file_path): file_path for file_path in files}

        all_results = []
        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                df = future.result()
                # Process the DataFrame to calculate autocorrelations
                results = process_file(file_path)
                all_results.extend(results)
            except Exception as e:
                print(f"Error reading {file_path}: {str(e)}")

    # Process results to get highest and lowest autocorrelation for each file
    df_results = pd.DataFrame(all_results)
    final_results = []

    for file in df_results['file'].unique():
        file_data = df_results[df_results['file'] == file]
        if not file_data.empty:
            highest_auto = file_data.loc[file_data['autocorrelation'].idxmax()]
            lowest_auto = file_data.loc[file_data['autocorrelation'].idxmin()]

            final_results.append({
                'Filename': highest_auto['file'],
                'Highest Autocorrelation': highest_auto['autocorrelation'],
                'Timeframe for Highest Autocorrelation': highest_auto['timeframe'],
                'Lag Value of Highest Autocorrelation': highest_auto['lag'],
                'Lowest Autocorrelation': lowest_auto['autocorrelation'],
                'Timeframe for Lowest Autocorrelation': lowest_auto['timeframe'],
                'Lag Value of Lowest Autocorrelation': lowest_auto['lag']
            })

    return pd.DataFrame(final_results)

def visualize_top_results(results):
    # Convert to DataFrame
    df = results

    # Filter significant results
    sig_pos = df.nlargest(25, 'Highest Autocorrelation')
    sig_neg = df.nsmallest(25, 'Lowest Autocorrelation')

    # Create figure
    plt.figure(figsize=(15, 10))

    # Plot positive autocorrelations
    plt.subplot(2, 1, 1)
    plt.barh(
        [f"{row['Filename']} ({row['Timeframe for Highest Autocorrelation']} min)" for _, row in sig_pos.iterrows()],
        sig_pos['Highest Autocorrelation'],
        color='green'
    )
    plt.title('Top 25 Positive Autocorrelations')
    plt.xlabel('Autocorrelation')

    # Plot negative autocorrelations
    plt.subplot(2, 1, 2)
    plt.barh(
        [f"{row['Filename']} ({row['Timeframe for Lowest Autocorrelation']} min)" for _, row in sig_neg.iterrows()],
        sig_neg['Lowest Autocorrelation'],
        color='red'
    )
    plt.title('Top 25 Negative Autocorrelations')
    plt.xlabel('Autocorrelation')

    plt.tight_layout()
    plt.savefig('top_autocorrelations.jpg', dpi=300, bbox_inches='tight')
    plt.show()

if __name__ == "__main__":
    # Set your folder path here
    folder_path = "/root/aniket/CandleData_1min"

    # Analyze all files in the folder
    results_df = analyze_folder(folder_path)

    # Save all results to CSV
    results_df.to_csv('autocorrelation_results.csv', index=False)

    # Visualize top results
    visualize_top_results(results_df)
