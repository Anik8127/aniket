from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
import pandas_ta as taa
import numpy as np
import talib as ta

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_1H = getFnoBacktestData(indexSym, startEpoch-886400, endEpoch, "1H")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df_1H.dropna(inplace=True)

        results = taa.supertrend(df_1H["h"], df_1H["l"], df_1H["c"], length=10, multiplier=3.0)
        df_1H["Supertrend"] = results["SUPERTd_10_3.0"]
        df_1H.dropna(inplace=True)

        df_1H = df_1H[df_1H.index > startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1H.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1H.csv")
      
        lastIndexTimeData = [0, 0]
        last1HIndexTimeData = [0, 0]
        # FirstEntry = False
        # secondEntry = False
        FirstCall = False
        FirstPut = False
        SecondCall = False
        SecondPut = False
        DatenotEqual = 0
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = (int(getExpiryData(self.timeData, baseSym)["LotSize"])//3)

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            timeEpochSubstract = (timeData-3600)
            if timeEpochSubstract in df_1H.index:
                last1HIndexTimeData.pop(0)
                last1HIndexTimeData.append(timeEpochSubstract)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if timeEpochSubstract in df_1H.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                DatenotEqual = self.humanTime.date()
                FirstCall = True
                FirstPut = True

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = f"Target Hit,{row['entryTypeee']}"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = f"Stoploss Hit, {row['entryTypeee']}"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit,{row['entryTypeee']}"
                        self.exitOrder(index, exitType)

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            if (timeEpochSubstract in df_1H.index):

                if self.openPnl.empty and self.humanTime.date() == DatenotEqual:
                    if df_1H.at[last1HIndexTimeData[1], "Supertrend"] == -1 and FirstCall == True:
                        callSym = self.getCallSym(self.timeData, baseSym, df_1H.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 2.0 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "entryTypeee": "firstcall"})
                        FirstCall = False
                        SecondPut = True
                        SecondCall = False
                        FirstPut = False

                    elif df_1H.at[last1HIndexTimeData[1], "Supertrend"] == 1 and FirstPut == True:
                        putSym = self.getPutSym(self.timeData, baseSym, df_1H.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 2.0 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "entryTypeee": "firstput"},)
                        FirstPut = False
                        SecondCall = True
                        SecondPut = False
                        FirstCall = False

                else:
                    if self.humanTime.date() == expiryDatetime.date():
                        Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                    if SecondPut and putCounter < 1 and df_1H.at[last1HIndexTimeData[1], "Supertrend"] == 1:

                        putSym = self.getPutSym(self.timeData, baseSym, df_1H.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 2.0 * data["c"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "entryTypeee": "secondput"},)
                        SecondPut = False
                        SecondCall = False

                    elif SecondCall and callCounter < 1 and df_1H.at[last1HIndexTimeData[1], "Supertrend"] == -1:
            
                        callSym = self.getCallSym(self.timeData, baseSym, df_1H.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 0.3 * data["c"]
                        stoploss = 2.0 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "entryTypeee": "secondCall"})
                        SecondCall = False
                        SecondPut = False

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "FridayCall"
    version = "v1"

    startDate = datetime(2020, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "SENSEX"
    indexName = "SENSEX"

    # baseSym = "NIFTY"
    # indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True)

    limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)
    generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")