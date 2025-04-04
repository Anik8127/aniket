from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_30min = getFnoBacktestData(indexSym, startEpoch-886400, endEpoch, "30Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df_30min.dropna(inplace=True)

        df_30min = df_30min[df_30min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_30min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_30Min.csv")

        # callEntryAllow = True
        # putEntryAllow = True        
        lastIndexTimeData = [0, 0]
        last30MinIndexTimeData = [0, 0]
        entry = False

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
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
            if (timeData-1800) in df_30min.index:
                last30MinIndexTimeData.pop(0)
                last30MinIndexTimeData.append(timeData-1800)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if (timeData-1800) in df_30min.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date() : #next day after expiry to build condor
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                entry= True
                
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = "Expiry Exit"
                        self.exitOrder(index, exitType)

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            if ((timeData-1800) in df_30min.index) and self.openPnl.empty:
                if entry == True and self.humanTime.time() == time(10, 15):    
                
                    #Call Entry
                    callSym = self.getCallSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                    #callHedge Entry
                    callSym = self.getCallSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=4)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                    #Put Entry
                    putSym = self.getPutSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=0)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                    #Put Hedge Entry
                    putSym = self.getPutSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=4)
                    
                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                    #Buying call at strike distance of 500
                    
                    callSym = self.getCallSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=10)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                    #Buying put at strike distance of 550

                    putSym = self.getPutSym(self.timeData, baseSym, df_30min.at[last30MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=11)
                    
                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, 5*lotSize, "BUY", {"Expiry": expiryEpoch},)


                    entry=False

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "Aniket"
    strategyName = "Iron Condor"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    # dr = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)
    # generateReportFile(dr, fileDir)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")