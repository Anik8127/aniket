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
            df_15min = getFnoBacktestData(indexSym, startEpoch - 432000, endEpoch, "15Min")#fetching 15min data of nifty
            
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)
        
        df_15min['adx'] = ta.ADX(df_15min['h'], df_15min['l'], df_15min['c'], timeperiod=14)
        df_15min['macd'], df_15min['macdsignal'], df_15min['macdhist'] = ta.MACD(df_15min['c'], fastperiod=12, slowperiod=26, signalperiod=9)#adding columns for macd
        df_15min.dropna(inplace=True)

        df_15min['EntryCall'] = np.where((df_15min['adx'] > 25) & (df_15min['adx'].shift(1) < 25) & (df_15min['macd'] > df_15min['macdsignal']) & (df_15min['macd'].shift(1) < df_15min['macdsignal'].shift(1)), "EntryCall", "")#condition for entry
        df_15min['EntryPut'] = np.where((df_15min['adx'] > 25) & (df_15min['adx'].shift(1) < 25) & (df_15min['macd'] < df_15min['macdsignal']) & (df_15min['macd'].shift(1) > df_15min['macdsignal'].shift(1)), "EntryPut", "")#condition for entry
        df.dropna(inplace=True)

        df = df[df.index >= startEpoch]
        df_15min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")

        lastIndexTimeData = [0, 0]
        lastIndex15MinTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)["CurrentExpiry"]  
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20) 
        lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])

        for timeData in df.index: 

            self.timeData = float(timeData)#float values only
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)
            
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue
           
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

            if (timeData-900) in df_15min.index:
                lastIndex15MinTimeData.pop(0)
                lastIndex15MinTimeData.append(timeData-900)


            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue
            
            if (timeData-900) in df_15min.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df_15min.at[lastIndex15MinTimeData[1],'c']}\tHigh: {df_15min.at[lastIndex15MinTimeData[1],'h']}\tMACD: {df_15min.at[lastIndex15MinTimeData[1],'macd']}\tADX: {df_15min.at[lastIndex15MinTimeData[1],'adx']}\tOpen: {df_15min.at[lastIndex15MinTimeData[1],'o']}")


            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndex15MinTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()
            
            if self.humanTime.date() > expiryDatetime.date() : #next day after expiry to build condor
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                lotSize = int(getExpiryData(startEpoch, baseSym)["LotSize"])
                
                expiryEpoch= expiryDatetime.timestamp()
            
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = "Expiry Exit"
                        self.exitOrder(index, exitType)

                    elif df_15min.at[lastIndex15MinTimeData[1], "adx"]>20: #adx exit
                        exitType = "adxexit"
                        self.exitOrder(index, exitType)#, row["Target"]
                        self.strategyLogger.info(f"TargetHit: Datetime: {self.humanTime}")#logging the datetime at TargetHit
            
            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)




            if (timeData - 900) in df_15min.index:
                if df_15min.at[lastIndex15MinTimeData[1], "EntryCall"] == "EntryCall":
                    callSym = self.getCallSym(self.timeData, baseSym, df_15min.at[lastIndex15MinTimeData[1], "c"], expiry=Currentexpiry)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        data = None
                        self.strategyLogger.info(f"Could not fetch data for {callSym}")
                        self.strategyLogger.exception(e)

                    if data is not None:
                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})

                elif df_15min.at[lastIndex15MinTimeData[1], "EntryPut"] == "EntryPut":
                    putSym = self.getPutSym(self.timeData, baseSym, df_15min.at[lastIndex15MinTimeData[1], "c"], expiry=Currentexpiry)

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
                            f"Open: {df_15min.at[lastIndex15MinTimeData[1], 'o']}\t"
                            f"High: {df_15min.at[lastIndex15MinTimeData[1], 'h']}\t"
                            f"Low: {df_15min.at[lastIndex15MinTimeData[1], 'l']}\t"
                            f"Close: {df_15min.at[lastIndex15MinTimeData[1], 'c']}\t"
                            f"adx: {df_15min.at[lastIndex15MinTimeData[1], 'adx']}"
                        )
            # else:
            #     self.strategyLogger.info(f"Timestamp {lastIndex15MinTimeData[1]} not found in df_15min index.")

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