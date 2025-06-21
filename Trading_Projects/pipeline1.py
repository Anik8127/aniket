import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime, timedelta
from ta.momentum import RSIIndicator
from ta.trend import MACD, ADXIndicator, EMAIndicator, SMAIndicator
from ta.volatility import BollingerBands, AverageTrueRange
from backtestTools.histData import getEquityBacktestData
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor

# =========================
# Logging Configuration
# =========================
logging.basicConfig(
    filename='stock_data_errors.log',
    level=logging.ERROR,
    format='%(asctime)s - %(message)s'
)

def load_stock_names(file_path):
    """
    Load stock ticker symbols from a file.
    Each line in the file should contain one ticker.
    """
    stocks = []
    with open(file_path, "r") as file:
        for line in file:
            ticker = line.strip()
            if ticker:
                stocks.append(ticker)
    return stocks

def get_buffer_days(std_window, interval_minutes):
    """
    Calculate buffer days needed for indicator calculation.
    For intraday data, buffer is std_window * interval.
    For daily data, buffer is std_window days.
    """
    # For 30min interval, 1 day = 13 bars (assuming 6.5h trading)
    if interval_minutes >= 1440:
        return std_window
    else:
        # Add 2 days buffer for safety for intraday
        return std_window // (13 if interval_minutes == 30 else 1) + 2

def fetch_data_with_buffer(stock, start_date, end_date, std_window, interval):
    """
    Fetch data with enough buffer before start_date to ensure indicators can be calculated.
    Uses UNIX epochs for date calculations.
    If data is not available, keep shifting start date backward until enough buffer is found.
    Logs if buffer data is not found and tries earlier dates until data is found.
    """
    # Convert start and end dates to datetime and then to epoch
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    start_epoch = int(start_dt.timestamp())
    end_epoch = int(end_dt.timestamp())

    # Calculate buffer in seconds (1 day = 86400 seconds)
    buffer_days = std_window + 2  # Minimum buffer for indicators
    seconds_in_day = 86400

    # Try to find earliest available buffer date using epochs
    for extra_days in range(buffer_days, buffer_days + 30):  # Try up to 30 more days back
        buffer_start_epoch = start_epoch - (extra_days * seconds_in_day)
        buffer_start_dt = datetime.fromtimestamp(buffer_start_epoch)
        df = getEquityBacktestData(stock, buffer_start_dt, end_dt, interval)
        if df is not None and not df.empty:
            # Check if we have at least some data before start_date
            df.index = pd.to_datetime(df.index)
            if (df.index < start_dt).any():
                return df
            else:
                logging.warning(f"No buffer data before {start_date} for {stock}, trying earlier epoch {buffer_start_dt.date()}...")
        else:
            logging.warning(f"No data found for {stock} from {buffer_start_dt.date()} to {end_dt.date()}, trying earlier epoch...")

    # If no buffer data found after all attempts, log and return None
    logging.error(f"Insufficient buffer data for {stock} even after checking 30 extra days before {start_date}")
    return None

def calculate_indicators(df, std_window):
    """
    Calculate all technical indicators sequentially (no multi-threading).
    """
    # RSI
    df['RSI_14'] = RSIIndicator(df['Close'], window=14).rsi()

    # EMA and SMA with std_window
    df[f'EMA_{std_window}'] = EMAIndicator(df['Close'], window=std_window).ema_indicator()
    df[f'SMA_{std_window}'] = SMAIndicator(df['Close'], window=std_window).sma_indicator()

    # MACD
    macd = MACD(df['Close'])
    df['MACD'] = macd.macd()
    df['MACD_Signal'] = macd.macd_signal()
    df['MACD_Hist'] = macd.macd_diff()

    # ADX
    adx = ADXIndicator(df['High'], df['Low'], df['Close'], window=14)
    df['ADX'] = adx.adx()
    df['ADX_Pos'] = adx.adx_pos()
    df['ADX_Neg'] = adx.adx_neg()

    # Bollinger Bands
    bb = BollingerBands(df['Close'], window=20, window_dev=2)
    df['BB_Upper'] = bb.bollinger_hband()
    df['BB_Mid'] = bb.bollinger_mavg()
    df['BB_Lower'] = bb.bollinger_lband()
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / df['BB_Mid']

    # ATR
    atr = AverageTrueRange(df['High'], df['Low'], df['Close'], window=14)
    df['ATR'] = atr.average_true_range()

    # Log Return STD
    df[f'LogRet_STD_{std_window}'] = df['Log_Return'].rolling(window=std_window).std()

    return df

def process_and_save_stock(args):
    """
    For a single stock:
    1. Fetch OHLCV data with buffer before start_date.
    2. Calculate log returns, lagged returns, and technical indicators.
    3. Trim data to only show from start_date to end_date (buffer is only for calculation).
    4. Save the resulting DataFrame to CSV.
    """
    stock, start_date, end_date, output_dir, std_window, interval = args
    try:
        # Step 1: Fetch data with buffer
        df = fetch_data_with_buffer(stock, start_date, end_date, std_window, interval)
        if df is None or df.empty:
            logging.error(f"No data available for {stock}")
            print(f"No data for {stock}")
            return None

        # Step 2: Rename columns to standard names
        df = df.rename(columns={
            'c': 'Close',
            'o': 'Open',
            'h': 'High',
            'l': 'Low',
            'v': 'Volume'
        })

        # Ensure 'datetime' column exists and is datetime type
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.set_index('datetime')
        else:
            logging.error(f"No 'datetime' column found for {stock}")
            print(f"No 'datetime' column found for {stock}")
            return None

        # Step 3: Calculate log returns and lagged returns (up to lag 2)
        df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
        df['Log_Return_1'] = df['Log_Return'].shift(1)
        df['Log_Return_2'] = df['Log_Return'].shift(2)

        # Step 4: Calculate technical indicators (on the full buffer-included DataFrame)
        df = calculate_indicators(df, std_window)

        # Step 5: Add lagged values for rolling standard deviation
        std_col = f'LogRet_STD_{std_window}'
        df[f'{std_col}_1'] = df[std_col].shift(1)
        df[f'{std_col}_2'] = df[std_col].shift(2)

        # Step 6: Only now, trim data to show from start_date to end_date
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)
        df = df[(df.index >= start_dt) & (df.index <= end_dt)]

        # Debugging output for data range
        print(f"{stock}: Data index range before trim: {df.index.min()} to {df.index.max()}, shape: {df.shape}")
        print(f"{stock}: Looking for data between {start_dt} and {end_dt}")

        # Step 7: Save to CSV (do not drop columns or rows)
        os.makedirs(output_dir, exist_ok=True)
        csv_filename = os.path.join(output_dir, f"{stock.replace('.NS', '')}_daily_indicators.csv")
        df.to_csv(csv_filename, index=True)
        print(f"Saved {stock} data to {csv_filename} | Rows: {len(df)} | Columns: {len(df.columns)}")
        return csv_filename

    except Exception as e:
        logging.error(f"Error processing {stock}: {str(e)}")
        print(f"Skipped {stock} due to error (see log)")
        return None

def align_csvs(output_dir, stocks):
    """
    Align all CSVs so that only rows (by index) present in all files are kept.
    This ensures all CSVs have the same rows and columns.
    """
    csv_files = [os.path.join(output_dir, f"{stock.replace('.NS', '')}_daily_indicators.csv") for stock in stocks]
    dfs = []
    indices = None

    # Read all CSVs and collect their indices
    for file in csv_files:
        if os.path.exists(file):
            df = pd.read_csv(file, index_col=0)
            dfs.append(df)
            if indices is None:
                indices = set(df.index)
            else:
                indices = indices & set(df.index)
        else:
            print(f"Missing file: {file}")

    if not dfs or indices is None:
        print("No CSVs to align.")
        return

    # Find intersection of all indices
    common_indices = sorted(indices)

    # Filter each DataFrame and overwrite CSV
    for file, df in zip(csv_files, dfs):
        df_aligned = df.loc[common_indices]
        df_aligned.to_csv(file, index=True)
        print(f"Aligned {file}: {df_aligned.shape[0]} rows, {df_aligned.shape[1]} columns")

def main():
    """
    Main function to:
    1. Load stock names.
    2. Download/process data in parallel using 4 processes.
    3. Calculate indicators using multi-threading.
    4. Save CSVs with data from start_date onward (buffer used only for calculation).
    5. Align all CSVs to have the same rows and columns.
    """
    stock_file = "/root/aniket/stockNames/nifty_50.md"
    output_dir = "stock_csvs"
    start_date = "2021-01-01"
    end_date = "2026-06-15"
    std_window = 14  # Used for all rolling windows (SMA, EMA, STD)
    interval = "30min"

    # Load stock tickers
    stocks = load_stock_names(stock_file)

    # Prepare arguments for multiprocessing
    args_list = [(stock, start_date, end_date, output_dir, std_window, interval) for stock in stocks]

    # Use multiprocessing Pool with 4 processes for faster data extraction
    with Pool(processes=4) as pool:
        pool.map(process_and_save_stock, args_list)

    # Align all CSVs to have the same rows (intersection of indices)
    align_csvs(output_dir, stocks)

if __name__ == "__main__":
    main()
