from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
import talib
import concurrent.futures
import pandas as pd
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger


class R_40_40_30(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "R_40_40_30":
            raise Exception("Strategy Name Mismatch")
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        first_stock = portfolio if portfolio and portfolio else None
        if first_stock:
            self.backtest(first_stock, startDate, endDate)
            print(colored("Backtesting 100% complete.", "light_yellow"))
        else:
            print(colored("No stocks to backtest.", "red"))
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False

        def process_stock(stock, startTimeEpoch, endTimeEpoch, df_dict):
            df = getEquityBacktestData(stock, startTimeEpoch - (86400 * 50), endTimeEpoch, "1H")
            if df is not None:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df["rsi"] = talib.RSI(df["c"], timeperiod=14)
                df['prev_rsi'] = df['rsi'].shift(1)
                df.dropna(inplace=True)
                # df.index = df.index + 33300
                df = df[df.index > startTimeEpoch]
                df_dict[stock] = df
                df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")
                print(f"Finished processing {stock}")

        def process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch):
            df_dict = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(process_stock, stock, startTimeEpoch, endTimeEpoch, df_dict): stock for stock in stocks}
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            return df_dict

        stocks = [
            "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJFINANCE",
            "BAJAJFINSV", "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
            "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
            "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", 
            "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", 
            "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", 
            "TATASTEEL", "TECHM", "TITAN", "UPL", "ULTRACEMCO", "WIPRO"
        ]

        df_dict = process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch)

        # amountPerTrade = 100000
        # lastIndexTimeData = None

        # for timeData in df_dict['ADANIENT'].index:
        #     for stock in stocks:
        #         stockAlgoLogic.timeData = timeData
        #         stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
        #         print(stock, stockAlgoLogic.humanTime)

        #         stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]
        #         if not stock_openPnl.empty:

        #             for index, row in stock_openPnl.iterrows():
        #                 try:
        #                     stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df_dict[stock].at[lastIndexTimeData, "c"]
        #                 except Exception as e:
        #                     print(f"Error fetching historical data for {row['Symbol']}")
        #         stockAlgoLogic.pnlCalculator()

        #         for index, row in stock_openPnl.iterrows():
        #             if lastIndexTimeData in df_dict[stock].index:
        #                 if index in stock_openPnl.index:
        #                     if stockAlgoLogic.humanTime.time() >= time(15,15):

        #                         exitType = "EOD"
        #                         stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
        #                         logger.info(f"EOD- Datetime: {stockAlgoLogic.humanTime}\tStock: {stock}\tClose: {df_dict[stock].at[lastIndexTimeData, 'c']}")

        #                     elif df_dict[stock].at[lastIndexTimeData, "rsi"] > 50 and df_dict[stock].at[lastIndexTimeData, "prev_rsi"] < 50:

        #                         exitType = "TargetHIt"
        #                         stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
        #                         logger.info(f"TargetHIt- Datetime: {stockAlgoLogic.humanTime}\tStock: {stock}\tClose: {df_dict[stock].at[lastIndexTimeData, 'c']}")

        #         if (lastIndexTimeData in df_dict[stock].index) and (stock_openPnl.empty):

        #             if df_dict[stock].at[lastIndexTimeData, "rsi"] > 30 and df_dict[stock].at[lastIndexTimeData, "prev_rsi"] < 30:

        #                 entry_price = df_dict[stock].at[lastIndexTimeData, "c"]
        #                 stockAlgoLogic.entryOrder(entry_price, stock, (amountPerTrade // entry_price), "BUY")
        #                 logger.info(f"Entry- Datetime: {stockAlgoLogic.humanTime}\tStock: {stock}\tClose: {df_dict[stock].at[lastIndexTimeData, 'c']}")

        #         lastIndexTimeData = timeData
        #         stockAlgoLogic.pnlCalculator()

        # for index, row in stockAlgoLogic.openPnl.iterrows():
        #     if lastIndexTimeData in df_dict[stock].index:
        #         if index in stockAlgoLogic.openPnl.index:
        #             exitType = "TimeUp"
        #             stockAlgoLogic.exitOrder(index, exitType, row['CurrentPrice'])

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "AK"
    strategyName = "R_40_40_30"
    version = "v1"

    startDate = datetime(2022, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    portfolio = 'combinedList'

    algoLogicObj = R_40_40_30(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")