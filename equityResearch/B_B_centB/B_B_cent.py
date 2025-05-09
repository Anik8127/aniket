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


class B_B_CENTB(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "B_B_CENTB":
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
        # Calculate Bollinger Bands
        window = 20  # Standard period
        df['SMA'] = df['c'].rolling(window).mean()
        df['StdDev'] = df['c'].rolling(window).std()
        df['UpperBand'] = df['SMA'] + (2 * df['StdDev'])
        df['LowerBand'] = df['SMA'] - (2 * df['StdDev'])

        # Calculate BB%B
        df['BB_PercentB'] = (df['c'] - df['LowerBand']) / (df['UpperBand'] - df['LowerBand'])


        df = df[df.index > startTimeEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stockName}_df.csv")

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "NA"
    strategyName = "B_B_CENTB"
    version = "v1"

    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 31, 15, 30)

    portfolio = createPortfolio("/root/aniket/stockNames/nifty_50.md",2)

    algoLogicObj = B_B_CENTB(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(days=1), mtm=True, fno=False)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")