import numpy as np
import talib as ta
# import ta
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic, optIntraDayAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData
import multiprocessing as mp

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(indexSym, startEpoch - 432000, endEpoch, "5Min")#fetching 5min data of nifty
            
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)
        
        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)
        df_5min['adx'] = ta.ADX(df_5min['h'], df_5min['l'], df_5min['c'], timeperiod=14)
        df_5min['macd'], df_5min['macdsignal'], df_5min['macdhist'] = ta.MACD(df_5min['c'], fastperiod=12, slowperiod=26, signalperiod=9)
        df_5min.dropna(inplace=True)

        df_5min['macdBullish'] = np.where((df_5min['macd'] > df_5min['macdsignal']) & (df_5min['macd'].shift(1) < df_5min['macdsignal'].shift(1)), "macdBullish", "")
        df_5min['macdBearish'] = np.where((df_5min['macd'] < df_5min['macdsignal']) & (df_5min['macd'].shift(1) > df_5min['macdsignal'].shift(1)), "macdBearish", "")
        df_5min['EntryCall'] = np.where((df_5min['adx'] > 25) & (df_5min['macdBullish'] == "macdBullish"),"EntryCall","")
        df_5min['EntryPut'] = np.where((df_5min['adx'] > 25) & (df_5min['macdBearish'] == "macdBearish"),"EntryPut","")

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

            self.timeData = float(timeData)#float values only
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)
            
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue
           
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            if (timeData-900) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-900)

            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue
            
            if (timeData-900) in df_5min.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df_5min.at[last5MinIndexTimeData[1],'c']}\tHigh: {df_5min.at[last5MinIndexTimeData[1],'h']}\tMACD: {df_5min.at[last5MinIndexTimeData[1],'macd']}\tADX: {df_5min.at[last5MinIndexTimeData[1],'adx']}\tOpen: {df_5min.at[last5MinIndexTimeData[1],'o']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], last5MinIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            if self.humanTime.date() == expiryDatetime.date() : #next day after expiry to build condor
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
                
                expiryEpoch= expiryDatetime.timestamp()
            
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = "Expiry Exit"
                        self.exitOrder(index, exitType)

                    elif df_5min.at[last5MinIndexTimeData[1], "adx"] < 20: #adx exit
                        exitType = "adxexit"
                        self.exitOrder(index, exitType)#, row["Target"]
                        self.strategyLogger.info(f"TargetHit: Datetime: {self.humanTime}")#logging the datetime at TargetHit


            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)


            if (timeData - 900) in df_5min.index:
                if callCounter <= 3 and df_5min.at[last5MinIndexTimeData[1] , "EntryCall"] == "EntryCall":
                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], expiry=Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        data = None
                        self.strategyLogger.info(f"Could not fetch data for {callSym}")
                        self.strategyLogger.exception(e)

                    if data is not None:
                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})

                elif putCounter <= 3 and df_5min.at[last5MinIndexTimeData[1] , "EntryPut"] == "EntryPut":
                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"], expiry=Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        data = None
                        self.strategyLogger.info(f"Could not fetch data for {putSym}")
                        self.strategyLogger.exception(e)

                    if data is not None:
                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                        self.strategyLogger.info(
                            f"optinPrice: {data['c']}, Entry: Datetime: {self.humanTime}\t"
                            f"Open: {df_5min.at[last5MinIndexTimeData[1], 'o']}\t"
                            f"High: {df_5min.at[last5MinIndexTimeData[1], 'h']}\t"
                            f"Low: {df_5min.at[last5MinIndexTimeData[1], 'l']}\t"
                            f"Close: {df_5min.at[last5MinIndexTimeData[1], 'c']}\t"
                            f"adx: {df_5min.at[last5MinIndexTimeData[1], 'adx']}"
                        )
            # else:
            #     self.strategyLogger.info(f"Timestamp {last5MinIndexTimeData[1]} not found in df_5min index.")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "Aniket"
    strategyName = ""
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