import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import talib as ta
from datetime import datetime, time
from backtestTools.util import createPortfolio
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.util import setup_logger
import concurrent.futures

def get_stock_list(file_path):
    with open(file_path, "r") as f:
        return [line.strip() for line in f if line.strip()]

def calculate_volatility(df):
    # Calculate 5-min returns
    returns = df['c'].pct_change()
    # Use standard deviation of returns, annualized by sqrt(N)
    return returns.std() * np.sqrt(len(returns.dropna()))

def calculate_liquidity(df):
    return df['v'].mean()

def get_features_for_stock(stock, start_date, end_date):
    try:
        # Try fetching 5-min data first
        df = getEquityBacktestData(stock, start_date.timestamp(), end_date.timestamp(), "5m")
        if df is None or len(df) < 10:
            # If not enough 5-min data, fetch 1-min and resample to 5-min
            df_1m = getEquityBacktestData(stock, start_date.timestamp(), end_date.timestamp(), "1m")
            if df_1m is None or len(df_1m) < 10:
                print(f"{stock}: No sufficient 1m or 5m data")
                return None
            # Ensure the index is datetime for resampling
            if not isinstance(df_1m.index, pd.DatetimeIndex):
                # Try to convert index to datetime if it's not already
                try:
                    df_1m.index = pd.to_datetime(df_1m.index)
                except Exception as e:
                    print(f"{stock}: Could not convert index to datetime: {e}")
                    return None
            # Resample 1-min OHLCV to 5-min, keep column names unchanged
            df = df_1m.resample('5T').agg({
                'o': 'first',
                'h': 'max',
                'l': 'min',
                'c': 'last',
                'v': 'sum'
            }).dropna()
            df = df[['o', 'h', 'l', 'c', 'v']]  # Ensure column order
            if len(df) < 10:
                print(f"{stock}: Not enough data after resampling 1m to 5m")
                return None
        print(f"{stock}: {len(df)} rows (used {'5m' if 'df_1m' not in locals() else '1m->5m'})")
        volatility = calculate_volatility(df)
        liquidity = calculate_liquidity(df)
        if np.isnan(volatility) or np.isnan(liquidity):
            return None
        return (stock, volatility, liquidity)
    except Exception as e:
        print(f"Error for {stock}: {e}")
        return None

def filter_stocks_by_kmeans(stock_list, start_date, end_date, n_clusters=3):
    features = []
    valid_stocks = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(lambda stock: get_features_for_stock(stock, start_date, end_date), stock_list))
    for result in results:
        if result is not None:
            stock, volatility, liquidity = result
            features.append([volatility, liquidity])
            valid_stocks.append(stock)
    features = np.array(features)
    if len(features) == 0:
        print("No stocks with sufficient data for KMeans clustering.")
        return []
    kmeans = KMeans(n_clusters=n_clusters, random_state=42).fit(features)
    # Select cluster with highest mean volatility and liquidity
    cluster_scores = []
    for i in range(n_clusters):
        cluster_points = features[kmeans.labels_ == i]
        cluster_scores.append(cluster_points.mean(axis=0).sum())
    best_cluster = np.argmax(cluster_scores)
    filtered_stocks = [stock for stock, label in zip(valid_stocks, kmeans.labels_) if label == best_cluster]
    return filtered_stocks

def save_stock_list(stock_list, file_path):
    with open(file_path, "w") as f:
        for stock in stock_list:
            f.write(stock + "\n")

class KMeansStrategy(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        for batch in portfolio:
            for stock in batch:
                self.backtest(stock, startDate, endDate)
                completed_backtests += 1
                print(f"Backtesting {completed_backtests}/{total_backtests} complete.", end="\r")
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False

        try:
            df = getEquityBacktestData(stockName, startTimeEpoch, endTimeEpoch, "5m")
        except Exception as e:
            print(stockName)
            raise Exception(e)

        # Calculate indicators
        df["rsi"] = ta.RSI(df["c"], timeperiod=14)
        df["ema9"] = ta.EMA(df["c"], timeperiod=9)
        df["ema12"] = ta.EMA(df["c"], timeperiod=12)
        upper, middle, lower = ta.BBANDS(df["c"], timeperiod=20, nbdevup=2, nbdevdn=2)
        df["bb_upper"] = upper
        df["bb_lower"] = lower

        # VWAP calculation
        df["vwap"] = (df["c"] * df["v"]).cumsum() / df["v"].cumsum()
        vwap_std = df["vwap"].rolling(window=20).std()
        df["vwap_upper_2"] = df["vwap"] + 2 * vwap_std
        df["vwap_lower_2"] = df["vwap"] - 2 * vwap_std

        df.dropna(inplace=True)
        df = df[df.index > startTimeEpoch]

        amountPerTrade = 100000
        lastIndexTimeData = None

        for timeData in df.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            # Entry Condition
            if (lastIndexTimeData in df.index) and stockAlgoLogic.openPnl.empty and stockAlgoLogic.humanTime.time() < time(15, 20):
                rsi = df.at[lastIndexTimeData, "rsi"]
                ema9 = df.at[lastIndexTimeData, "ema9"]
                ema12 = df.at[lastIndexTimeData, "ema12"]
                close = df.at[lastIndexTimeData, "c"]
                bb_upper = df.at[lastIndexTimeData, "bb_upper"]
                vwap_upper_2 = df.at[lastIndexTimeData, "vwap_upper_2"]

                ema_diff = (ema9 - ema12) / ema12 if ema12 != 0 else 0

                if (
                    rsi > 75 and
                    (ema_diff > 0.10 or ema_diff < -0.10) and
                    close > bb_upper and
                    close > vwap_upper_2
                ):
                    entry_price = close
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade // entry_price), "SELL")

            # Exit Condition
            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    entry_price = row['EntryPrice']
                    current_price = df.at[lastIndexTimeData, "c"]
                    # Target exit
                    if current_price < entry_price * 0.85:
                        stockAlgoLogic.exitOrder(index, "TargetHit", current_price)
                    # Stoploss exit
                    elif current_price > entry_price * 1.7:
                        stockAlgoLogic.exitOrder(index, "StopLossHit", current_price)
                    # Time-based exit
                    elif stockAlgoLogic.humanTime.time() >= time(15, 20):
                        stockAlgoLogic.exitOrder(index, "TimeUp", current_price)

            lastIndexTimeData = timeData
            stockAlgoLogic.pnlCalculator()

        # Final exit for open positions
        if not stockAlgoLogic.openPnl.empty:
            for index, row in stockAlgoLogic.openPnl.iterrows():
                stockAlgoLogic.exitOrder(index, "TimeUp")
        stockAlgoLogic.pnlCalculator()

if __name__ == "__main__":
    # Step 1: Input for K-Means filteration
    kmeans_start = input("Enter K-Means filter start date (YYYY-MM-DD): ")
    kmeans_end = input("Enter K-Means filter end date (YYYY-MM-DD): ")
    kmeans_start_date = datetime.strptime(kmeans_start, "%Y-%m-%d")
    kmeans_end_date = datetime.strptime(kmeans_end, "%Y-%m-%d")

    # Step 2: Input for backtest date range
    bt_start = input("Enter backtest start date (YYYY-MM-DD): ")
    bt_end = input("Enter backtest end date (YYYY-MM-DD): ")
    bt_start_date = datetime.strptime(bt_start, "%Y-%m-%d")
    bt_end_date = datetime.strptime(bt_end, "%Y-%m-%d")

    stock_file = "/root/aniket/349_stocks.md"
    filtered_file = "/root/aniket/stocks349_filtered.md"

    stock_list = get_stock_list(stock_file)
    filtered_stocks = filter_stocks_by_kmeans(stock_list, kmeans_start_date, kmeans_end_date)
    save_stock_list(filtered_stocks, filtered_file)
    print(f"Filtered stocks saved to {filtered_file}")

    # Prevent ZeroDivisionError
    if not filtered_stocks:
        print("No stocks found after KMeans filtering. Exiting.")
        exit(1)

    # Step 3: Backtest on filtered stocks
    portfolio = createPortfolio(filtered_file, 1)
    algoLogicObj = KMeansStrategy("NA", "KMeansStrategy", "v1")
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, bt_start_date, bt_end_date)
    print("Backtest complete.")