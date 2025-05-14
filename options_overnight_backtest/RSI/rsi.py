import numpy as np
import talib as ta
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optOverNightAlgoLogic, optIntraDayAlgoLogic
from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.histData import getFnoBacktestData
import multiprocessing as mp

class algoLogic(optIntraDayAlgoLogic):


     def run(self, startDate, endDate, baseSym, indexSym):
         #defining run to execute the strategy

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()#start time of the strategy
        endEpoch = endDate.timestamp()#end time of the strategy
        
        #creating a dataframe of nifty and nifty 15min
        try:
            df = getFnoBacktestData(indexSym, startEpoch - 432000, endEpoch, "1Min")#fetching 1min data of nifty
            df['rsi'] = ta.RSI(df["c"], timeperiod=14)#rsi values using talib library
            df['prev_rsi'] = df['c'].shift(1)
            df['CE_SELL_ABOVE_50'] = np.where((df['rsi'] > 50) & (df['rsi'].shift(1) <= 50), "CE_SELL_ABOVE_50", "")
            df['cE_SELL_BELOW_50'] = np.where((df['rsi'] < 50) & (df['rsi'].shift(1) >=50), "PE_SELL_BELOW_50", "")
            df['ce_buy_below_30'] = np.where((df['rsi'] < 30) & (df['prev_rsi'] >= 30), "ce_buy_below_30", "")
            df['ce_buy_below_20'] = np.where((df['rsi'] < 20) & (df['prev_rsi'] >= 20), "pe_buy_above_70", "")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e) #raising exception if data is not found

        df.dropna(inplace=True)#dropping null values

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
            if (timeData-900) in df.index:
                last15MinIndexTimeData.pop(0)
                last15MinIndexTimeData.append(timeData-900)

            if (self.humanTime.time() < time(9, 20)) | (self.humanTime.time() > time(15, 25)): #checking if time is less than 9:20 or greater than 15:25
                continue #condition to check strategy time

            if lastIndexTimeData[1] in df.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tOpen:{df.at[lastIndexTimeData[1],'o']}\t High:{df.at[lastIndexTimeData[1],'h']}\t Low:{df.at[lastIndexTimeData[1],'l']}\t Close: {df.at[lastIndexTimeData[1],'c']}\t rsi: {df.at[lastIndexTimeData[1],'rsi']}")#logging the datetime and close values

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
            if timeData-900 in df.index:
                rsi = df.at[last15MinIndexTimeData[1], "rsi"]
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[last15MinIndexTimeData[1], 'c']} RSI: {df_15min.at[last15MinIndexTimeData[1], 'rsi']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]#taking last 2 characters of the symbol

                    #EXIT CONDITIONS

                    if row["CurrentPrice"] <= row["Target"]: #EXIT AT TARGET i.e., 0.7 * data["c"]
                        exitType = "TargetHit"
                        self.exitOrder(index, exitType)#, row["Target"]
                        self.strategyLogger.info(f"TargetHit: Datetime: {self.humanTime}")#logging the datetime at TargetHit

                    elif row["CurrentPrice"] >= row["Stoploss"]: #EXIT AT STOPLOSS i.e., 1.3 * data["c"]
                        exitType = "StoplossHit"
                        self.exitOrder(index, exitType, row["Stoploss"])
                        self.strategyLogger.info(f"StoplossHit: Datetime: {self.humanTime}")#logging the datetime and close values

            #Entry Logic

            # '''for taking more than one lot size use trade counters or
            #     len(self.openPnl) < 2'''
            if ((timeData - 900) in df.index):

                tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
                callTradeCounter = tradecount.get('CE',0)
                putTradeCounter = tradecount.get('PE',0)

                if callTradeCounter <= 1:
                            
                    if (df.at[last15MinIndexTimeData[1], "callSell"] == "callSell"): 
                            
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[last15MinIndexTimeData[1], "c"])

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
                    if (df.at[last15MinIndexTimeData[1], "putSell"] == "putSell"): 

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

    # Configure number of processes to be created
    maxConcurrentProcesses = 4
    processes = []

 #For Multiprocessing   
# Start a loop from Start Date to End Date
currentDate = startDate
while currentDate <= endDate:
        # Define trading period for Current day
        startTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 9, 15, 0)
        endTime = datetime(
            currentDate.year, currentDate.month, currentDate.day, 15, 30, 0)
       
        p = mp.Process(target=algo.run, args=(
            startTime, endTime, baseSym, indexName))
        p.start()
        processes.append(p)

        if len(processes) >= maxConcurrentProcesses:
            for p in processes:
                p.join()
            processes = []

        currentDate += timedelta(days=1)
        

end = datetime.now()
print(f"Done. Ended in {end-start}.")