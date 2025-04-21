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
            df_1h = getFnoBacktestData(indexSym, startEpoch-886400, endEpoch, "1H")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df_1h.dropna(inplace=True)

        # Calculate Atr
        df_1h['atr'] = ta.ATR(df_1h['h'], df_1h['l'], df_1h['c'], timeperiod=14)
        df_1h['ema_atr'] = df_1h['atr'].ewm(span=20, adjust=False).mean()
        df_1h.dropna(inplace=True)

        # Calculate AO
        df_1h['Midpoint'] = (df_1h['h'] + df_1h['l']) / 2
        df_1h['AO'] = df_1h['Midpoint'].rolling(window=5).mean() - df_1h['Midpoint'].rolling(window=34).mean()
        df_1h['ema_AO'] = df_1h['AO'].ewm(span=20, adjust=False).mean()
        df_1h.dropna(inplace=True)

        df_1h['Entry'] = np.where((df_1h['atr'] > df_1h['ema_AO']) & (df_1h['AO'] > df_1h['ema_AO']), "Entry", "")
    
        df_1h = df_1h[df_1h.index > startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_1h.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1h.csv")

        lastIndexTimeData = [0, 0]
        last1HIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = (int(getExpiryData(self.timeData, baseSym)["LotSize"]))

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            timeEpochSubstract = (timeData-3600)
            if timeEpochSubstract in df_1h.index:
                last1HIndexTimeData.pop(0)
                last1HIndexTimeData.append(timeEpochSubstract)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if timeEpochSubstract in df_1h.index:
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

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if row["CurrentPrice"] <= row["Target"]:
                        exitType = f"Target Hit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = f"Stoploss Hit,"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

            if (timeEpochSubstract in df_1h.index):

                if self.openPnl.empty:
                    if df_1h.at[last1HIndexTimeData[1], "Entry"] == "Entry":
                        callSym = self.getCallSym(self.timeData, baseSym, df_1h.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 1.5 * df_1h.at[last1HIndexTimeData[1], "atr"]
                        stoploss = 2.0 * df_1h.at[last1HIndexTimeData[1], "atr"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})

                        putSym = self.getPutSym(self.timeData, baseSym, df_1h.at[last1HIndexTimeData[1], "c"],expiry= Currentexpiry)

                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        except Exception as e:
                            self.strategyLogger.info(e)

                        target = 1.5 * df_1h.at[last1HIndexTimeData[1], "atr"]
                        stoploss = 2.0 * df_1h.at[last1HIndexTimeData[1], "atr"]

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {
                        "Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch},)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "Ani"
    strategyName = "Aniket"
    version = "v1"

    startDate = datetime(2023, 1, 1, 9, 15)
    endDate = datetime(2024, 12, 31, 15, 30)

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