import numpy as np
import talib as ta
from datetime import datetime, time, timedelta, date
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
import pandas_ta as pta
from backtestTools.util import setup_logger


class Break(optOverNightAlgoLogic):
       

    def run(self, startDate, endDate, baseSym, indexSym):
        startepoch = startDate.timestamp()
        endepoch = endDate.timestamp()

        #Setup Logger
        date = startDate.strftime('%Y-%m-%d')
        daily_strategy_logger = setup_logger(
            f"strategyLogger_{str(date)}", f"{self.fileDir['backtestResultsStrategyLogs']}/backTest_{str(date)}.log",)
        

        try:
            df_1d = getFnoBacktestData(indexSym, startepoch-(86400 * 30), endepoch, '1D')
            df_1h=getFnoBacktestData(indexSym,startepoch,endepoch,'1H')
            df_1m=getFnoBacktestData(indexSym,startepoch,endepoch,'1Min')
        except Exception as e:
            daily_strategy_logger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)
        
        #Alligning Index
        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300

        #DataCleaning
        df_1d = df_1d[df_1d.index >= startepoch-86400]
        df_1d.dropna(inplace=True)
        df_1h.dropna(inplace=True)
        df_1m.dropna(inplace=True)

        df_1d.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Day.csv")
        df_1h.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Hour.csv")
        df_1m.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        # Get current expiry and lot size
        currentExpiry = getExpiryData(startDate, baseSym)['CurrentExpiry']
        currentExpiryDt = datetime.strptime(
            currentExpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch=currentExpiryDt.timestamp()
        
    

        col = ['Target', 'Stoploss', 'Expiry']
        self.addColumnsToOpenPnlDf(col)
        
        lastindextimeData1m=[0,0]
        lastindextimeData1h=[0,0]
        lastindextimeData1d=[0,0] 

        daily_high = None                                  # Remove variable/ Entry on expiry 
        daily_low = None
        current_day = None
        prev_day = None
        callTradeCounter = 0           
        putTradeCounter = 0

        last_breakout = {"CE": None, "PE": None}  


        for timeData in df_1m.index:
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            current_day = self.humanTime.date()

            # Skip time periods outside trading hours
            if (self.humanTime.time() < time(9, 15)) or (self.humanTime.time() > time(15, 30)):
                continue
            
            lastindextimeData1m.pop(0)
            lastindextimeData1m.append(timeData-60)

            if (timeData-3600) in df_1h.index:
                lastindextimeData1h.pop(0)
                lastindextimeData1h.append(timeData - 3600)

            
            #Updating daily Index
            previous_day = timeData - 86400         # Subtract 1 day 
            if timeData in df_1d.index and df_1d.index.get_loc(timeData) > 1: 
                # Check if previous day exists
                while previous_day not in df_1d.index:  
                    previous_day -= 86400

            if previous_day in df_1d.index:
                lastindextimeData1d.pop(0)
                lastindextimeData1d.append(previous_day)
            

            # Expiry for entry
            if self.humanTime.date() == currentExpiryDt.date():
                currentExpiry = getExpiryData(self.humanTime + timedelta(days=1), baseSym)[
                    'CurrentExpiry']
                currentExpiryDt = datetime.strptime(
                    currentExpiry, "%d%b%y").replace(hour=15, minute=20)      
                expiryEpoch = currentExpiryDt.timestamp()   
            

            #Logging           
            if lastindextimeData1m[1] in df_1m.index:
                daily_strategy_logger.info(
                    f"Datetime: {self.humanTime}\tOpen: {df_1m.at[lastindextimeData1m[1],'o']}\tHigh: {df_1m.at[lastindextimeData1m[1],'h']}\tLow:{df_1m.at[lastindextimeData1m[1],'l']}\tClose: {df_1m.at[lastindextimeData1m[1],'c']}\tCurrentExpiry:{currentExpiryDt} ")
           

            #Storing Daily High and Low
            if current_day != prev_day:
                prev_day = current_day
                if (lastindextimeData1d[1] in df_1d.index):
                    daily_high = df_1d.at[lastindextimeData1d[1],'h'] 
                    daily_low = df_1d.at[lastindextimeData1d[1],'l']
                    last_breakout = {"CE": None, "PE": None}
                    daily_strategy_logger.info(f"Captured Daily High: {daily_high}\tDaily Low: {daily_low}")
                    
                    lotSize=int(getExpiryData(startDate, baseSym)["LotSize"])


           
            
            # Update and calculate PnL
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    try:
                        data = self.fetchAndCacheFnoHistData(row['Symbol'], timeData)
                        self.openPnl.at[index, 'CurrentPrice'] = data['c']
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            # EXIT LOGIC
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if row['CurrentPrice']< row['Target']:
                        self.exitOrder(index,'TargetHit')

                    elif row['CurrentPrice'] > row['Stoploss']:
                        self.exitOrder(index,'StoplossHit')
                   
                    elif self.timeData >= row['Expiry']:          
                        exitType = "Expiry"
                        self.exitOrder(index, exitType)


            # Update trade counters based on active trades
            open_trade_counts = self.openPnl['Symbol'].str[-2:].value_counts()
            callTradeCounter = open_trade_counts.get("CE", 0)
            putTradeCounter = open_trade_counts.get("PE", 0)

            
            
            #EntryLogic:  
            if (self.humanTime.time() < time(9, 15)) or (self.humanTime.time() > time(15, 14)):
                continue

            if timeData-3600 in lastindextimeData1h:                        
                
                close_price = df_1h.at[lastindextimeData1h[1], 'c']
                self.strategyLogger.info(f"Close Price at {self.humanTime}: {close_price}")

            if daily_high and daily_low is not None: 

                #Bullish
                if putTradeCounter < 1:
                    if close_price > daily_high and last_breakout["PE"] != daily_high:
                        putSym = self.getPutSym(self.timeData, baseSym, df_1h.at[lastindextimeData1h[1], "c"],expiry=currentExpiry)
                        
                        try:
                            data = self.fetchAndCacheFnoHistData(putSym, timeData)
                        except Exception as e:
                                self.strategyLogger.info(e)
                                continue
                        
                        self.entryOrder(data['c'], putSym, lotSize, "SELL", {
                                "Target": 0.7 * data['c'],
                                "Stoploss": 1.3 * data['c'],
                                "Expiry": expiryEpoch,
                            })
                        
                        last_breakout["PE"] = daily_high 
                        daily_strategy_logger.info(
                                f"\nBullishEntry at Datetime: {self.humanTime}\tSym:{putSym}\tLastDailyHigh:{daily_high}\tLastClose:{close_price}\tPutCounter:{putTradeCounter}\tOpen: {df_1m.at[lastindextimeData1m[1],'o']}\tHigh: {df_1m.at[lastindextimeData1m[1],'h']}\tLow:{df_1m.at[lastindextimeData1m[1],'l']}\tClose: {df_1m.at[lastindextimeData1m[1],'c']}\n")

                #Bearish
                if callTradeCounter < 1:
                    if close_price < daily_low and last_breakout["CE"] != daily_low:
                        callSym = self.getCallSym(self.timeData, baseSym,df_1h.at[lastindextimeData1h[1], 'c'],expiry=currentExpiry)
                        # lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
                        # expiryEpoch = self.getCurrentExpiryEpoch(self.timeData, baseSym)

                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, timeData)
                        except Exception as e:
                            self.strategyLogger.info(e)
                            continue
                        self.entryOrder(data['c'], callSym, lotSize, "SELL",{
                            "Target": 0.7 * data['c'],
                            "Stoploss": 1.3 * data['c'],
                            "Expiry": expiryEpoch,
                            
                            })
                        last_breakout["CE"] = daily_low 
                        daily_strategy_logger.info(
                            f"\nBearishEntry at Datetime: {self.humanTime}\tSym:{callSym}\tLastDailyLow:{daily_low}\tLastClose:{close_price}\tCallCounter:{callTradeCounter}\tOpen: {df_1m.at[lastindextimeData1m[1],'o']}\tHigh: {df_1m.at[lastindextimeData1m[1],'h']}\tLow:{df_1m.at[lastindextimeData1m[1],'l']}\tClose: {df_1m.at[lastindextimeData1m[1],'c']}\n")
                
               
            # else:
            #    daily_strategy_logger.info(f"{lastindextimeData1d}lasttimeindex not found")
        self.pnlCalculator()
        self.combinePnlCsv()
  

if  __name__ == "__main__":
    # Define Strategy Nomenclature
    devName = "HA"
    strategyName = "BREAK"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2024, 3, 30, 15, 30)

    # Create algoLogic object
    algo = Break(devName, strategyName, version)

    # Define Index Name
    baseSym = 'NIFTY'
    indexName = 'NIFTY 50'

    # Execute the algorithm
    algo.run(startDate, endDate, baseSym, indexName)