import pandas as pd
from statsmodels.tsa.stattools import adfuller, acf
import matplotlib.pyplot as plt
import sys
import os

def resample_ohlc(df, timeframe_minutes):
    """
    Resamples 1-minute stock data to OHLC (Open-High-Low-Close) candles.
    - Filters market hours (9:15 AM–3:29 PM).
    - Handles missing timestamps by generating a new index.
    - Returns resampled OHLC DataFrame with returns.
    """
    # If 'timestamp' exists, use it; else generate a new one
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
    else:
        # Generate a time index (assuming data starts at 9:15 AM)
        start_time = pd.Timestamp("09:15:00").time()
        end_time = pd.Timestamp("15:29:00").time()
        dates = pd.date_range(
            start=pd.Timestamp.now().normalize() + pd.Timedelta(hours=9, minutes=15),
            periods=len(df),
            freq='1T'
        )
        df.index = dates
    
    # Filter market hours (9:15 AM–3:29 PM)
    market_hours = df.between_time('09:15', '15:29')
    
    # Resample OHLC data (if 'o', 'h', 'l', 'c' columns exist)
    if all(col in market_hours.columns for col in ['o', 'h', 'l', 'c']):
        resampled = market_hours.resample(f'{timeframe_minutes}T').agg({
            'o': 'first',
            'h': 'max',
            'l': 'min',
            'c': 'last'
        }).dropna()
    else:
        # If only 'c' exists, resample closing prices
        resampled = market_hours['c'].resample(f'{timeframe_minutes}T').last().dropna()
        resampled = pd.DataFrame({'c': resampled})
    
    # Calculate returns
    resampled['returns'] = resampled['c'].pct_change().dropna()
    
    return resampled

def analyze_time_series(timeframe_minutes=1):
    """
    Main analysis function:
    1. Loads data from fixed path.
    2. Resamples to given timeframe.
    3. Tests stationarity (ADF Test).
    4. Plots autocorrelation (if stationary).
    """
    try:
        # 1. Load data (modify path if needed)
        file_path = "/root/aniket/CandleData_1min/ABB_1Min.csv"
        df = pd.read_csv(file_path)
        
        # Ensure 'c' (closing price) exists
        if 'c' not in df.columns:
            print("Error: CSV must contain column 'c' (closing price).")
            sys.exit(1)
        
        # Convert 'c' to numeric
        df['c'] = pd.to_numeric(df['c'], errors='coerce')
        df.dropna(subset=['c'], inplace=True)
        
        if df.empty:
            print("Error: No valid data in 'c' column.")
            sys.exit(1)

        # 2. Resample data (if needed)
        if timeframe_minutes != 1:
            print(f"\nResampling to {timeframe_minutes}-minute OHLC data...")
            df = resample_ohlc(df, timeframe_minutes)
            print("\nResampled Data:")
            print(df.head())
            print(f"\nTotal resampled points: {len(df)}")
            returns = df['returns'].dropna()
        else:
            # Use original 1-min data
            returns = df['c'].pct_change().dropna()

        if returns.empty:
            print("Error: Not enough data for returns calculation.")
            sys.exit(1)

        # 3. ADF Stationarity Test
        print("\nPerforming ADF Test on Returns...")
        adf_result = adfuller(returns)
        print(f"ADF Statistic: {adf_result[0]:.4f}")
        print(f"P-value: {adf_result[1]:.4f}")
        print("Critical Values:")
        for key, val in adf_result[4].items():
            print(f"\t{key}: {val:.4f}")

        if adf_result[1] < 0.05:
            print("\n✅ Returns are stationary.")
            
            # 4. Autocorrelation Analysis
            print("\nAutocorrelation (Lags 1-15):")
            autocorr = acf(returns, nlags=15, fft=True)
            for lag, corr in enumerate(autocorr):
                print(f"Lag {lag}: {corr:.4f}")

            # 5. Plot ACF
            plt.figure(figsize=(10, 5))
            plt.stem(range(len(autocorr)), autocorr)
            plt.axhline(0, color='grey', linestyle='--')
            conf = 1.96 / (len(returns)**0.5)
            plt.axhspan(-conf, conf, alpha=0.2, color='blue')
            plt.title(f"ACF of {timeframe_minutes}-min Returns")
            plt.xlabel("Lag")
            plt.ylabel("Autocorrelation")
            plt.grid(True)
            
            # Save plot
            plot_name = f"EXIDEIND_{timeframe_minutes}min_ACF.jpg"
            plt.savefig(plot_name, dpi=300, bbox_inches='tight')
            print(f"\nSaved ACF plot as '{plot_name}'")
            plt.show()
        else:
            print("\n❌ Returns are non-stationary.")
            sys.exit(0)

    except Exception as e:
        print(f"\n🚨 Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Default: 1-minute data
    timeframe = 1
    
    # Check for command-line argument (e.g., `python donchain.py 5`)
    if len(sys.argv) > 1:
        try:
            timeframe = int(sys.argv[1])
            if timeframe < 1:
                print("Error: Timeframe must be ≥1 minute.")
                sys.exit(1)
        except ValueError:
            print("Error: Timeframe must be an integer (e.g., 5 for 5-min data).")
            sys.exit(1)
    
    print(f"\nAnalyzing {timeframe}-minute data...")
    analyze_time_series(timeframe)