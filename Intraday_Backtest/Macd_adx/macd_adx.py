import numpy as np
import talib as ta
# import ta
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic, optIntraDayAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData
import multiprocessing as mp

class algoLogic(optIntraDayAlgoLogic):

    #Defining the function to get data for the strategy
    def getCurrentExpiryEpoch(self, date, baseSym):
        expiryData = getExpiryData(date, baseSym) #to get expiry dates of the contracts of nifty
        nextExpiryData = getExpiryData(date + 86400, baseSym) #to get extra data of expiry dates of the contracts of nifty
    
        expiry = expiryData["CurrentExpiry"]#referring "CurrentExpiry" saved in expiryData dictionary
        expiryDatetime = datetime.strptime(expiry, "%d%b%y")

        if self.humanTime.date() == expiryDatetime.date():
            expiry = nextExpiryData["CurrentExpiry"]#if time period of a contract is over, then it will refer to the next contract
        else:
            expiry = expiryData["CurrentExpiry"]#if time period of a contract is not over, then it will refer to the current contract

        expiryDatetime = datetime.strptime(expiry, "%d%b%y") #converting expiry date to datetime
        expiryDatetime = expiryDatetime.replace(hour=15, minute=15)
        expiryEpoch = expiryDatetime.timestamp()

        return expiryEpoch
        

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "BaseSymStoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()#start time of the strategy
        endEpoch = endDate.timestamp()#end time of the strategy

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            df_5min = getFnoBacktestData(indexSym, startEpoch - 432000, endEpoch, "5Min")#fetching 15min data of nifty
            df_5min['adx'] = ta.ADX(df_5min['h'], df_5min['l'], df_5min['c'], timeperiod=14)
            df_5min['macd'], df_5min['macdsignal'], df_5min['macdhist'] = ta.MACD(df_5min['c'], fastperiod=12, slowperiod=26, signalperiod=9)#adding columns for macd
            df_5min.dropna(inplace=True)

            df_5min['EntryCall'] = np.where((df_5min['adx'] > 25) & (df_5min['adx'].shift(1) < 25) & (df_5min['macd'] > df_5min['macdsignal']) & (df_5min['macd'].shift(1) < df_5min['macdsignal'].shift(1)), "EntryCall", "")#condition for entry
            df_5min['EntryPut'] = np.where((df_5min['adx'] > 25) & (df_5min['adx'].shift(1) < 25) & (df_5min['macd'] < df_5min['macdsignal']) & (df_5min['macd'].shift(1) > df_5min['macdsignal'].shift(1)), "EntryPut", "")#condition for entry
            df.dropna(inplace=True)

            df = df[df.index >= startEpoch]
            df_5min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")
            df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        lastIndexTimeData = [0, 0]
        lastIndex5MinTimeData = [0, 0]
        expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)    
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        for timeData in df.index: 

            self.timeData = float(timeData)#float values only
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)
            
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue
           
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)


            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)):
                continue

            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}\t adx: {df.at[lastIndexTimeData[1],'adx']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            pnll = self.openPnl["Pnl"].sum()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if self.humanTime.time() >= time(15, 15):
                        exitType = "IntradayExit"
                        self.exitOrder(index, exitType)
                        self.strategyLogger.info(f"IntradayExit: Datetime: {self.humanTime}")

                    elif row["CurrentPrice"] <= row["Target"]: #EXIT AT TARGET i.e., 0.7 * data["c"]
                        exitType = "TargetHit"
                        self.exitOrder(index, exitType)#, row["Target"]
                        self.strategyLogger.info(f"TargetHit: Datetime: {self.humanTime}")#logging the datetime at TargetHit

                    elif row["CurrentPrice"] >= row["Stoploss"]: #EXIT AT STOPLOSS i.e., 1.3 * data["c"]
                        exitType = "StoplossHit"
                        self.exitOrder(index, exitType, row["Stoploss"])
                        self.strategyLogger.info(f"StoplossHit: Datetime: {self.humanTime}")#logging the datetime and close values


            if ((timeData - 60) in df.index):

                if (df.at[lastIndexTimeData[1], "EntryCall"] == "EntryCall"): 

                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        data = None
                        self.strategyLogger.info(f"COuld not fetch data for {callSym}")
                        self.strategyLogger.exception(e)
                    
                    Target = 0.3 * data["c"]
                    Stoploss = 1.3 * data["c"]

                    if data is not None:
                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": Target, "Stoploss": Stoploss})

                    elif (df.at[lastIndexTimeData[1], "EntryPut"] == "EntryPut"): 

                     putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"])

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        data = None
                        self.strategyLogger.info(f"COuld not fetch data for {callSym}")
                        self.strategyLogger.exception(e)
                    
                    Target = 0.3 * data["c"]
                    Stoploss = 1.3 * data["c"]

                    if data is not None:
                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch, "Target": Target, "Stoploss": Stoploss})

                        self.strategyLogger.info(f"optinPrice: {data['c']},Entry: Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}\t rsi:{df.at[lastIndexTimeData[1], 'adx']}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    start = datetime.now()

    devName = "AN"
    strategyName = "adxNewAlgo"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2025, 1, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    maxConcurrentProcesses = 4
    processes = []

currentDate = startDate
while currentDate <= endDate:
        startTime = datetime(currentDate.year, currentDate.month, currentDate.day, 9, 15, 0)
        endTime = datetime(currentDate.year, currentDate.month, currentDate.day, 15, 30, 0)
       
        p = mp.Process(target=algo.run, args=(startTime, endTime, baseSym, indexName))
        p.start()
        processes.append(p)

        if len(processes) >= maxConcurrentProcesses:
            for p in processes:
                p.join()
            processes = []
        currentDate += timedelta(days=1)

end = datetime.now()
print(f"Done. Ended in {end-start}.")