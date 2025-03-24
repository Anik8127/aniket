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
        expiryDatetime = expiryDatetime.replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        return expiryEpoch
        

    #run function is responsible to run the strategy

    def run(self, startDate, endDate, baseSym, indexSym):
         #defining run to execute the strategy

        col = ["Target", "Stoploss", "BaseSymStoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()#start time of the strategy
        endEpoch = endDate.timestamp()#end time of the strategy
        
        #creating a dataframe of nifty and nifty 15min
        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")#fetching 1min data of nifty
            df_15min = getFnoBacktestData(indexSym, startEpoch - 432000, endEpoch, "15Min")#fetching 15min data of nifty
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e) #raising exception if data is not found

        if df is None or df_15min is None:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            return

        df.dropna(inplace=True)#dropping null values
        df_15min.dropna(inplace=True)

        #Creating RSI Columns for 15 Min Data:

        df_15min["rsi"] = ta.RSI(df_15min["c"], timeperiod=14)#rsi values using talib library
        # df_15min['rsi'] = ta.momentum.RSIIndicator(df_15min['c'], window=14).rsi()

        df_15min.dropna(inplace=True)#dropping null values
        df_15min['prev_rsi'] = df_15min['rsi'].shift(1)
        df_15min['putSell'] = np.where((df_15min['rsi'] < 30) & (df_15min['prev_rsi'] > 30), "putSell", "")
        df_15min['callSell'] = np.where((df_15min['rsi'] > 70) & (df_15min['prev_rsi'] < 70), "callSell", "")
        # df_15min['TakeEntry'] = np.where((df_15min['rsi'] < 30) & (df_15min['prev_rsi'] > 30), "TakeEntry", "")

        df_15min = df_15min[df_15min.index >= startEpoch]#df value should be greater than startEpoch
         
         #Creating RSI CrossOver Columns

        df_15min["rsiCross55"] = np.where((df_15min["rsi"] > 55) & (df_15min["rsi"].shift(1) <= 55), 1, 0)#creating a column rsiCross55, using shift function to shift the values by 1 under opposite conditions
        df_15min["rsiCross45"] = np.where((df_15min["rsi"] > 45) & (df_15min["rsi"].shift(1) <= 45), 1, 0)#creating a column rsiCross45, using shift function to shift the values by 1 under opposite conditions

        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")#coverting df to csv file  
        df_15min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_15Min.csv")#coverting df_15min to csv file

        lastIndexTimeData = [0, 0]#initialization of lastIndexTimeData
        last15MinIndexTimeData = [0, 0]#initialization of last15MinIndexTimeData
        expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)    
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        #iterating through the index of df
        for timeData in df.index: 

            self.timeData = float(timeData)#float values only
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            #Checking if time is less than 9:16 or greater than 15:30
            
            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)): #checking if time is less than 9:16 or greater than 15:30
                continue #condition to check market time
           
            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-900) in df_15min.index:
                last15MinIndexTimeData.pop(0)
                last15MinIndexTimeData.append(timeData-900)

            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)): #checking if time is less than 9:20 or greater than 15:25
                continue #condition to check strategy time

            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}")#logging the datetime and close values

            #fetching and caching historical data of the symbol
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            # if last15MinIndexTimeData[1] in df_15min.index:
            #     self.strategyLogger.info(f"Datetime: {self.humanTime}\tRSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']}")

            # to log rsi for 15min Df in the loop
            if timeData-900 in df_15min.index:
                rsi = df_15min.at[last15MinIndexTimeData[1], "rsi"]
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df_15min.at[last15MinIndexTimeData[1], 'c']} RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]#taking last 2 characters of the symbol

                    #EXIT CONDITIONS

                    if self.humanTime.time() >= time(15, 15):#EXIT AT 15:15
                        exitType = "IntradayExit"
                        self.exitOrder(index, exitType)
                        self.strategyLogger.info(f"IntradayExit: Datetime: {self.humanTime}")#logging the datetime at IntradayExit

                    elif row["CurrentPrice"] <= row["Target"]: #EXIT AT TARGET i.e., 0.7 * data["c"]
                        exitType = "TargetHit"
                        self.exitOrder(index, exitType)#, row["Target"]
                        self.strategyLogger.info(f"TargetHit: Datetime: {self.humanTime}")#logging the datetime at TargetHit

                    elif row["CurrentPrice"] >= row["Stoploss"]: #EXIT AT STOPLOSS i.e., 1.3 * data["c"]
                        exitType = "StoplossHit"
                        self.exitOrder(index, exitType, row["Stoploss"])
                        self.strategyLogger.info(f"StoplossHit: Datetime: {self.humanTime}")#logging the datetime and close values

                    elif last15MinIndexTimeData[1] in df_15min.index: #exit at RSI>55 for CE
                        if (df_15min.at[last15MinIndexTimeData[1], "rsiCross55"] == 1) & (symSide == "CE"):#exit at RSI>55 for CE
                            exitType = "rsiCross55"
                            self.exitOrder(index, exitType)
                            self.strategyLogger.info(f"rsiCross55: Datetime: {self.humanTime}\t rsi:{df_15min.at[last15MinIndexTimeData[1], 'rsi']}")#logging the datetime and close values

                        elif (df_15min.at[last15MinIndexTimeData[1], "rsiCross45"] == 1) & (symSide == "PE"): #exit at RSI>45 for PE
                            exitType = "rsiCross45"
                            self.exitOrder(index, exitType)
                            self.strategyLogger.info(f"rsiCross45: Datetime: {self.humanTime}\t rsi:{df_15min.at[last15MinIndexTimeData[1], 'rsi']}")#logging the datetime and close values

            #Entry Logic

            # '''for taking more than one lot size use trade counters or
            #     len(self.openPnl) < 2'''
            if ((timeData - 900) in df_15min.index):

                tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
                callTradeCounter = tradecount.get('CE',0)
                putTradeCounter = tradecount.get('PE',0)

                if callTradeCounter <= 1:
                            
                    if (df_15min.at[last15MinIndexTimeData[1], "callSell"] == "callSell"): 
                            
                        callSym = self.getCallSym(self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"])

                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        except Exception as e:
                            data = None
                            self.strategyLogger.info(f"COuld not fetch data for {callSym}")
                            self.strategyLogger.exception(e)

                        if data is not None:
                            target = 0.7 * data["c"]#target value is 0.7 * data["c"] 
                            stoploss = 1.3 * data["c"]#stoploss value is 1.3 * data["c"]
                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target,"Stoploss": stoploss,
                                            "Expiry": expiryEpoch })

                            self.strategyLogger.info(f"optinPrice: {data['c']},Entry: Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}\t rsi:{df_15min.at[last15MinIndexTimeData[1], 'rsi']}")#logging the datetime and close values

                if putTradeCounter <= 1:
                    if (df_15min.at[last15MinIndexTimeData[1], "putSell"] == "putSell"): 

                        putSym = self.getPutSym(self.timeData, baseSym, df_15min.at[last15MinIndexTimeData[1], "c"])

                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        except Exception as e:
                            data = None
                            self.strategyLogger.info(f"COuld not fetch data for {putSym}")
                            self.strategyLogger.exception(e)

                        if data is not None:
                            target = 0.7 * data["c"]#target value is 0.7 * data["c"] FOR PUT
                            stoploss = 1.3 * data["c"]#stoploss value is 1.3 * data["c"] FOR PUT
                            self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss,
                                            "Expiry": expiryEpoch })
                            #to change lot size write lotSize(2)
                            self.strategyLogger.info(f"optinPrice: {data['c']},Entry: Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}\t rsi:{df_15min.at[last15MinIndexTimeData[1], 'rsi']}")#logging the datetime and close values

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]




if __name__ == "__main__":
    start = datetime.now()

    # Define Strategy Nomenclature
    devName = "AN"
    strategyName = "rsiNewAlgo"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2025, 1, 1, 9, 15)
    endDate = datetime(2025, 1, 31, 15, 30)

    # Create algoLogic object
    algo = algoLogic(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Start a loop from Start Date to End Date
    currentDate = startDate
    while currentDate <= endDate:
        # Define trading period for Current day
        startTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 9, 15, 0)
        endTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 15, 30, 0)

        algo.run(startTime, endTime, baseSym, indexName)

        currentDate += timedelta(days=1)
        
end = datetime.now()
print(f"Done. Ended in {end-start}.")