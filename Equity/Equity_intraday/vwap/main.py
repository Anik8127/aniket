from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.histData import getEquityHistData
from backtestTools.util import setup_logger
from datetime import datetime, timedelta
from termcolor import colored, cprint
from datetime import datetime, time
import multiprocessing
import numpy as np
import logging
import talib as ta
import pandas as pd

class Vwap_Aniket(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "Vwap_Aniket":
            raise Exception("Strategy Name Mismatch")
        total_backtests = sum(len(batch) for batch in portfolio)
        completed_backtests = 0
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        print(colored("Backtesting 0% complete.", "light_yellow"), end="\r")
        for batch in portfolio:
            processes = []
            for stock in batch:
                p = multiprocessing.Process(target=self.backtest, args=(stock, startDate, endDate))
                p.start()
                processes.append(p)
            for p in processes:
                p.join()
                completed_backtests += 1
                percent_done = (completed_backtests / total_backtests) * 100
                print(colored(f"Backtesting {percent_done:.2f}% complete.", "light_yellow"), end=("\r" if percent_done != 100 else "\n"))
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):

        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()

        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)

        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log",)
        logger.propagate = False

        try:
            df = getEquityBacktestData(stockName, startTimeEpoch-7776000, endTimeEpoch, "15Min")
        except Exception as e:
            print(stockName)
            raise Exception(e)
        
        print(df)

        df['typical_price'] = (df['h'] + df['l'] + df['c']) / 3

        tpv = df['typical_price'] * df['v']
        vwap = tpv.rolling(window=20).sum() / df['v'].rolling(window=20).sum()
        df['vwap'] = vwap

        df['vwap_std'] = df['vwap'].rolling(window=20).std()
        df["rsi"] = ta.RSI(df["c"], timeperiod=14)#rsi values using talib library

        df['vwap_upper_2'] = df['vwap'] + 1.0 * df['vwap_std']
        df['vwap_lower_2'] = df['vwap'] - 1.0 * df['vwap_std']

        df['EntryLong'] = np.where(df['c'] > df['vwap_upper_2'], "EntryLong", "")
        df['EntryShort'] = np.where(df['c'] < df['vwap_lower_2'], "EntryShort", "")

        df.dropna(inplace=True)
        df = df[df.index > startTimeEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")

        amountPerTrade = 100000
        stoploss = 0.01
        lastIndexTimeData = None
        
        trade_counter = 0  # Initialize trade counter
        for timeData in df.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if lastIndexTimeData in df.index:
                logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData,'c']}\tClose: {df.at[lastIndexTimeData,'c']}\trsi: {df.at[lastIndexTimeData,'rsi']}\tvwap: {df.at[lastIndexTimeData,'vwap']}\tvwap_upper_2: {df.at[lastIndexTimeData,'vwap_upper_2']}\tvwap_lower_2: {df.at[lastIndexTimeData,'vwap_lower_2']}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():

                    try:
                        stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df.at[lastIndexTimeData, "c"]
                    except Exception as e:
                        logging.info(e)

            stockAlgoLogic.pnlCalculator()

            for index, row in stockAlgoLogic.openPnl.iterrows():
                if lastIndexTimeData in df.index:

                    if stockAlgoLogic.humanTime.time() >= time(15, 15):
                        exitType = "Time Up"
                        stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

                    elif row['PositionStatus'] == 1:

                        if (row['EntryPrice']*0.99) > df.at[lastIndexTimeData, "c"]:
                            exitType = "stopLossHit"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

                        elif (row['EntryPrice']*1.05) < df.at[lastIndexTimeData, "c"]:
                            exitType = "TargetHit"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

                    elif row['PositionStatus'] == -1:
                    
                        if (row['EntryPrice']*1.01) < df.at[lastIndexTimeData, "c"] :
                            exitType = "stopLossHit"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

                        elif (row['EntryPrice']*0.95) > df.at[lastIndexTimeData, "c"]:
                            exitType = "TargetHit"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

            if (lastIndexTimeData in df.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() < time(15, 10)):

                if (df.at[lastIndexTimeData, "EntryLong"] == "EntryLong" and df.at[lastIndexTimeData, "rsi"] > 55):
                    entry_price = df.at[lastIndexTimeData, "c"]
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")
                    trade_counter += 1  # Increment trade counter

                elif (df.at[lastIndexTimeData, "EntryShort"] == "EntryShort") and df.at[lastIndexTimeData, "rsi"] > 55:
                    entry_price = df.at[lastIndexTimeData, "c"]
                    quantity = ((amountPerTrade * stoploss) * stoploss)
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "SELL")
                    trade_counter += 1 # Increment trade counter

            lastIndexTimeData = timeData
            stockAlgoLogic.pnlCalculator()

        if not stockAlgoLogic.openPnl.empty:
            for index, row in stockAlgoLogic.openPnl.iterrows():
                exitType = "Time Up"
                stockAlgoLogic.exitOrder(index, exitType)
        stockAlgoLogic.pnlCalculator()

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "Vwap_Aniket"
    version = "v1"

    startDate = datetime(2022, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/aniket/vwap/stockNifty200.md", 1)

    algoLogicObj = Vwap_Aniket(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")