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
            df_5min = getFnoBacktestData(indexSym, startEpoch-886400, endEpoch, "5Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)
        df_5min['adx'] = ta.ADX(df_5min['h'], df_5min['l'], df_5min['c'], timeperiod=14)
        df_5min['EMA'] = ta.EMA(df_5min['c'], timeperiod=20)
        df_5min.dropna(inplace=True)


        df_5min['EntryADX'] = np.where(df_5min['adx'] > 25, "EntryADX", "")
        df_5min['EntryCE'] = np.where((df_5min['adx'] > 25) & (df_5min['c'] > df_5min['EMA']), "EntryCE", "")
        df_5min['EntryPE'] = np.where((df_5min['adx'] > 25) & (df_5min['c'] < df_5min['EMA']), "EntryPE", "")

        df_5min = df_5min[df_5min.index > startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")

        callEntryAllow = True
        putEntryAllow = True        
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]

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
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if (timeData-300) in df_5min.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}\tadx: {df_5min.at[last5MinIndexTimeData[1],'adx']}\tEMA: {df_5min.at[last5MinIndexTimeData[1],'EMA']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            if self.humanTime.date() == expiryDatetime.date() :
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]
      
                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = "Target Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif self.timeData >= row["Expiry"]:
                        exitType = "Intraday Exit"
                        self.exitOrder(index, exitType)

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            if ((timeData-300) in df_5min.index) and self.openPnl.empty:

                if df_5min.at[last5MinIndexTimeData[1], "EntryADX"] == "EntryADX" and df_5min.at[last5MinIndexTimeData[1], "EntryCE"] == "EntryCE":
                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 0.3 * data["c"]
                    stoploss = 1.3 * data["c"]

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                    "Target": target,"Stoploss": stoploss,"Expiry": expiryEpoch, })

                if df_5min.at[last5MinIndexTimeData[1], "EntryADX"] == "EntryADX" and df_5min.at[last5MinIndexTimeData[1], "EntryPE"] == "EntryPE":
                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    target = 1.3 * data["c"]
                    stoploss = 0.3 * data["c"]

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                    "Target": target,"Stoploss": stoploss,"Expiry": expiryEpoch, },)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "Aniket"
    strategyName = "Ema_Adx"
    version = "v1"

    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2023, 1, 25, 15, 30)

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