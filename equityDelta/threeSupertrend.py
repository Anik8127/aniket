from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.util import setup_logger, calculate_mtm
from datetime import datetime, timedelta
from termcolor import colored, cprint
from datetime import datetime, time
import multiprocessing
import numpy as np
import logging
import talib
import pandas_ta as ta
import pandas as pd

class equityDelta(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "equityDelta":
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
            df = getEquityBacktestData(stockName, startTimeEpoch-(86400*500), endTimeEpoch, "75Min")
        except Exception as e:
            print(stockName)
            raise Exception(e)
        print(df)

        df.dropna(inplace=True)

        supertrend_one = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=3.6)
        supertrend_two = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=2.7)
        supertrend_three = ta.supertrend(df["h"], df["l"], df["c"], length=100, multiplier=1.8)
        stoch_rsi = ta.momentum.stochrsi(df['c'], length=14, rsi_length=14, k=3, d=3)

        df['SupertrendColourOne'] = supertrend_one['SUPERTd_100_3.6']
        df['SupertrendColourTwo'] = supertrend_two['SUPERTd_100_2.7']
        df['SupertrendColourThree'] = supertrend_three['SUPERTd_100_1.8']
        df['Stochastic_rsi'] = stoch_rsi['STOCHRSIk_14_14_3_3']
        
        

        df['EntryLong'] = np.where(
            (df['SupertrendColourOne'] == 1) &
            (df['SupertrendColourTwo'] == 1) &
            (df['SupertrendColourThree'] == 1)&
            (df['Stochastic_rsi'] > 0.8),
            "EntryLong", "")

        # df['EntryShort'] = np.where(
        #     (df['SupertrendColourOne'] == -1) &
        #     (df['SupertrendColourTwo'] == -1) &
        #     (df['SupertrendColourThree'] == -1),
        #     "EntryShort", "")

        df['ExitLong'] = np.where(df['SupertrendColourTwo'] == -1, "ExitLong", "")
        df['ExitShort'] = np.where(df['SupertrendColourThree'] == 1, "ExitShort", "")

        df = df[df.index > startTimeEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")

        amountPerTrade = 100000
        lastIndexTimeData = None

        for timeData in df.index:
            stockAlgoLogic.timeData = timeData
            stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)

            if lastIndexTimeData in df.index:
                logger.info(f"Datetime: {stockAlgoLogic.humanTime}\tStock: {stockName}\tClose: {df.at[lastIndexTimeData,'c']}")

            if not stockAlgoLogic.openPnl.empty:
                for index, row in stockAlgoLogic.openPnl.iterrows():
                    try:
                        stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df.at[lastIndexTimeData, "c"]
                    except Exception as e:
                        logging.info(e)
            stockAlgoLogic.pnlCalculator()

            for index, row in stockAlgoLogic.openPnl.iterrows():
                if lastIndexTimeData in df.index:

                    if row['PositionStatus'] == 1:

                        if df.at[lastIndexTimeData, "ExitLong"] == "ExitLong":
                            exitType = "ExitUsingSupertrend"
                            stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

                    #elif row['PositionStatus'] == -1:

                        # if df.at[lastIndexTimeData, "ExitShort"] == "ExitShort":
                        #     exitType = "ExitUsingSupertrend"
                        #     stockAlgoLogic.exitOrder(index, exitType, df.at[lastIndexTimeData, "c"])

            if (lastIndexTimeData in df.index) & (stockAlgoLogic.openPnl.empty) & (stockAlgoLogic.humanTime.time() < time(15, 15)):

                if (df.at[lastIndexTimeData, "EntryLong"] == "EntryLong"):
                    entry_price = df.at[lastIndexTimeData, "c"]
                    stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "BUY")

                # elif (df.at[lastIndexTimeData, "EntryShort"] == "EntryShort"):
                #     entry_price = df.at[lastIndexTimeData, "c"]
                #     stockAlgoLogic.entryOrder(entry_price, stockName, (amountPerTrade//entry_price), "SELL")

            lastIndexTimeData = timeData
            stockAlgoLogic.pnlCalculator()

        if not stockAlgoLogic.openPnl.empty:
            for index, row in stockAlgoLogic.openPnl.iterrows():
                exitType = "TimeUp"
                stockAlgoLogic.exitOrder(index, exitType)
        stockAlgoLogic.pnlCalculator()


if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "equityDelta"
    version = "v1"

    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2025, 4, 30, 15, 30)

    # portfolio = createPortfolio("/root/akashResearchAndDevelopment/stocksList/nifty500.md", 1)
    # portfolio = createPortfolio("/root/akashResearchAndDevelopment/stocksList/ani.md", 1)
    portfolio = createPortfolio("/root/aniket/stock_names (1).txt", 1)

    algoLogicObj = equityDelta(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculate_mtm(closedPnl, fileDir, timeFrame="15T", mtm=False, equityMarket=True)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")