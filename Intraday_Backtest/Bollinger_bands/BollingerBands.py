import numpy as np
import talib as ta
from datetime import datetime, time, timedelta
from backtestTools.algoLogic import optIntraDayAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
import multiprocessing as mp
from backtestTools.util import setup_logger

class BollingerBandStrategy(optIntraDayAlgoLogic):
    def run(self, startDate, endDate, baseSym, indexSym):
        startepoch = startDate.timestamp()
        endepoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startepoch - (86400 * 30), endepoch, '5Min')
            df_1min=getFnoBacktestData(indexSym, startepoch , endepoch, '1Min')
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}: {e}") #if data not found
            raise Exception(e)

        if df is None or df_1min is None:
            self.strategyLogger.info(f"Data fetch returned None for {baseSym} between {startDate} and {endDate}.")
            raise Exception(f"Data not available for {baseSym} between {startDate} and {endDate}.") #raise exception if data not found

        # Calculate Bollinger Bands
        df['upper'], df['middle'], df['lower'] = ta.BBANDS(df['c'], timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)

        df['signal']=np.where((df["c"] > df["upper"]) & (df["c"].shift(1) <= df["upper"].shift(1)), 1, 
                                   (np.where((df['c'] < df["lower"]) & (df["c"].shift(1) >= df["lower"].shift(1)), -1, 0)))    
       
        df.dropna(inplace=True) #drop rows with NaN values
      
        df = df[df.index >= startepoch] #drop extra data
        df_1min.dropna(inplace=True) #drop rows with NaN values
    
        df_1min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_5Min.csv")


        col = ['Target', 'Stoploss', 'Expiry']
        self.addColumnsToOpenPnlDf(col)
        lastindextimeData=[0,0]
        lastindextimeData1m=[0,0]
        lotSize = int(getExpiryData(startDate.timestamp(), baseSym)["LotSize"])

        #Setup Logger
        date = startDate.strftime('%Y-%m-%d')
        daily_strategy_logger = setup_logger(
            f"strategyLogger_{str(date)}", f"{self.fileDir['backtestResultsStrategyLogs']}/backTest_{str(date)}.log",)

        #Loop through each time period                     
        for timeData in df_1min.index:
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 16)) or (self.humanTime.time() > time(15, 30)):
                continue

            lastindextimeData1m.pop(0)
            lastindextimeData1m.append(timeData-60)
            
            if (timeData-300) in df.index:
                lastindextimeData.pop(0)
                lastindextimeData.append(timeData - 300)

            #Logging
            if lastindextimeData1m[1] in df_1min.index:
                daily_strategy_logger.info(
                    f"Datetime: {self.humanTime}\tOpen: {df_1min.at[lastindextimeData1m[1],'o']}\tHigh: {df_1min.at[lastindextimeData1m[1],'h']}\tLow:{df_1min.at[lastindextimeData1m[1],'l']}\tClose: {df_1min.at[lastindextimeData1m[1],'c']}\t ")
            else:
                self.strategyLogger.info(f"Data not found for timestamp: {lastindextimeData1m}")
    
            if timeData-300 in df.index:
                daily_strategy_logger.info(
                    f"Datetime: {self.humanTime}\tOpen: {df.at[lastindextimeData[1],'o']}\tHigh: {df.at[lastindextimeData[1],'h']}\tLow:{df.at[lastindextimeData[1],'l']}\tClose: {df.at[lastindextimeData[1],'c']}\tUB:{df.at[lastindextimeData[1],'upper']}\tLB: {df.at[lastindextimeData[1], 'lower']}")
            
            

            # Update PnL
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row['Symbol'], timeData)
                        self.openPnl.at[index, 'CurrentPrice'] = data['c']
                    except Exception as e:
                        self.strategyLogger.info(e)

            # Calculate and update PnL
            self.pnlCalculator()

            # EXIT LOGIC
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if self.humanTime.time() >= time(15, 15):
                        self.exitOrder(index, "End of Day") #intraday exit
                    elif row['CurrentPrice'] <= row['Target']:
                        self.exitOrder(index, "TargetHit")
                    elif row['CurrentPrice'] >= row['Stoploss']:
                        self.exitOrder(index, "StoplossHit")

            # ENTRY LOGIC
            if timeData-300 in df.index:
                
                if len(lastindextimeData) > 1 and lastindextimeData[1] in df.index:
                 
                 # Sell CE 
                    if df.at[lastindextimeData[1],'signal']==1:
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastindextimeData[1], 'c'])
                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, timeData)
                        except Exception as e:
                            daily_strategy_logger.info(e)
                            continue
                        self.entryOrder(data['c'], callSym, lotSize, "SELL", {
                            "Target": 0.7 * data['c'],   
                            "Stoploss": 1.3 * data['c'],    
                        })
                        daily_strategy_logger.info(
                         f"BearishEntry at Datetime: {self.humanTime}\tOpen: {df.at[lastindextimeData[1],'o']}\tHigh: {df.at[lastindextimeData[1],'h']}\tLow: {df.at[lastindextimeData[1],'l']}\tClose: {df.at[lastindextimeData[1],'c']}\tUB: {df.at[lastindextimeData[1],'upper']}\tLB: {df.at[lastindextimeData[1], 'lower']}\tSignal: {df.at[lastindextimeData[1], 'signal']}")
            

                        # Sell PE when price closes below the lower Bollinger Band
                    elif df.at[lastindextimeData[1],'signal']==-1:
                        putSym = self.getPutSym(self.timeData, baseSym, df.at[lastindextimeData[1], 'c'])
                        
                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, timeData)
                        except Exception as e:
                            daily_strategy_logger.info(e)
                            continue
                        self.entryOrder(data['c'], putSym, lotSize, "SELL", {
                            "Target": 0.7 * data['c'],
                            "Stoploss": 1.3 * data['c'],
                            
                        })
                       
                        daily_strategy_logger.info(
                         f"BullishEntry at Datetime: {self.humanTime}\tOpen: {df.at[lastindextimeData[1],'o']}\tHigh: {df.at[lastindextimeData[1],'h']}\tLow: {df.at[lastindextimeData[1],'l']}\tClose: {df.at[lastindextimeData[1],'c']}\tUB: {df.at[lastindextimeData[1],'upper']}\tLB: {df.at[lastindextimeData[1], 'lower']}\tSignal: {df.at[lastindextimeData[1], 'signal']}")

            

        self.pnlCalculator()
        self.combinePnlCsv()


if __name__ == "__main__":
    start = datetime.now()

    # Define Strategy Nomenclature
    devName = "HA"
    strategyName = "BollingerBandStrategy"
    version = "1.0"

    # Define Start date and End date
    startDate = datetime(2021, 1, 1, 9, 15)
    endDate = datetime(2021, 1, 31, 15, 30)

    # Create strategy object
    algo = BollingerBandStrategy(devName, strategyName, version)

    # Define Index Name
    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    # Configure number of processes to be created
    maxConcurrentProcesses = 4
    processes = []

    # Start a loop from Start Date to End Date
    currentDate = startDate
    while currentDate <= endDate:
        # Define trading period for Current day
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

    # Wait for remaining processes to finish
    for p in processes:
        p.join()

    end = datetime.now()
    print(f"Done. Execution completed in {end - start}.")
