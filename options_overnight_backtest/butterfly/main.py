from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from datetime import datetime, time, timedelta
from backtestTools.expiry import getExpiryData
import talib as ta
import numpy as np

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(indexSym, startEpoch-886400, endEpoch, "5Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df_5min['rsi'] = ta.RSI(df_5min["c"], timeperiod=14)
        df_5min["rsiCross60"] = np.where((df_5min["rsi"] > 60) & (df_5min["rsi"].shift(1) <= 60), "rsiCross60", 0)
        df_5min["rsiCross40"] = np.where((df_5min["rsi"] < 40) & (df_5min["rsi"].shift(1) >= 40), "rsiCross40", 0)

        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)

        df_5min = df_5min[df_5min.index > startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")
   
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]

        CurrentExpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(CurrentExpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date() :
                CurrentExpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(CurrentExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if self.timeData > row["Expiry"]:
                        exitType = f"ExpiryExit, {row['time']}"
                        self.exitOrder(index, exitType)

            if ((timeData-300) in df_5min.index) and (self.openPnl.empty):

                if df_5min.at[last5MinIndexTimeData[1], "rsiCross60"] == "rsiCross60":

                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {putSym}: {e}")

                    self.entryOrder(data["c"], putSym, (lotSize*2), "SELL", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})

                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry, -1)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {putSym}: {e}")

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})

                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry, 1)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {putSym}: {e}")

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})


                elif df_5min.at[last5MinIndexTimeData[1], "rsiCross40"] == "rsiCross40":

                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry, -1)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {callSym}: {e}")

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})


                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry, 1)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {callSym}: {e}")

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})


                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], CurrentExpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(f"Error fetching data for {callSym}: {e}")

                    self.entryOrder(data["c"], callSym, (lotSize*2), "BUY", {"Expiry": expiryEpoch, "time": df.at[lastIndexTimeData[1], "datetime"]})

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "NA"
    strategyName = "coveredCallEveryMonth"
    version = "v1"

    startDate = datetime(2022, 1, 1, 9, 15)
    endDate = datetime(2025, 4, 30, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True)

    limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)

    generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")