import talib
import logging
import numpy as np
import multiprocessing
from termcolor import colored, cprint
from datetime import datetime, time
from backtestTools.util import setup_logger
from backtestTools.histData import getEquityHistData
from backtestTools.histData import getEquityBacktestData
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from datetime import datetime, timedelta
from backtestTools.util import createPortfolio, calculateDailyReport, limitCapital, generateReportFile


class SUPERTREND(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "SUPERTREND":
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
            df = getEquityBacktestData( stockName, startTimeEpoch-7776000, endTimeEpoch, "D")
        except Exception as e:
            raise Exception(e)

        df.dropna(inplace=True)
        df.index = df.index + 33300
        # Calculate ATR
        atr = talib.ATR(df['h'], df['l'], df['c'], timeperiod=14)

        # Define multiplier
        multiplier = 3

        # Calculate Basic Bands
        df['basic_ub'] = (df['h'] + df['l']) / 2 + multiplier * atr
        df['basic_lb'] = (df['h'] + df['l']) / 2 - multiplier * atr

        # Compute Final Bands
        df['final_ub'] = df['basic_ub']
        df['final_lb'] = df['basic_lb']

        # SuperTrend Calculation
        df['supertrend'] = 0
        for i in range(1, len(df)):
            try:
                # Use .iloc to access rows by position
                if df['c'].iloc[i] > df['final_ub'].iloc[i-1]:
                    df.loc[df.index[i], 'supertrend'] = df['final_lb'].iloc[i]
                elif df['c'].iloc[i] < df['final_lb'].iloc[i-1]:
                    df.loc[df.index[i], 'supertrend'] = df['final_ub'].iloc[i]
                else:
                    df.loc[df.index[i], 'supertrend'] = df['supertrend'].iloc[i-1]
            except KeyError as e:
                print(f"KeyError at index {i}: {e}")
                continue

        # Create Green & Red Columns
        df['green'] = df['c'] > df['supertrend']
        df['red'] = df['c'] < df['supertrend']


        df = df[df.index > startTimeEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "SUPERTREND"
    version = "v1"

    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/aniket/stockNames/nifty_50.md",2)

    algoLogicObj = SUPERTREND(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")