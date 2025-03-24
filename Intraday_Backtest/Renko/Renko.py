import numpy as np
import talib as ta
from datetime import datetime, time
from backtestTools.algoLogic import optIntraDayAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData    # use groupby to group current-renko for log,CSV issue
import pandas_ta as pta
import multiprocessing as mp
from datetime import datetime, timedelta, time
from backtestTools.util import setup_logger
import indicators as indicators
import pandas as pd 

class Renko(optIntraDayAlgoLogic):
    def __init__(self, devName, strategyName, version):
        super().__init__(devName, strategyName, version)
        self.max_loss = 8000
        self.daily_pnl = 0
        self.last_pnl_checkpoints = [0]  #  profit checkpoints for trailing stop adjustment

    def run(self, startDate, endDate, baseSym, indexSym):
        startepoch = startDate.timestamp()
        endepoch = endDate.timestamp()

        #SetUp Logger
        date = startDate.strftime('%Y-%m-%d')
        daily_strategy_logger = setup_logger(
            f"strategyLogger_{str(date)}", f"{self.fileDir['backtestResultsStrategyLogs']}/backTest_{str(date)}.log",)


        try:
            df = getFnoBacktestData(indexSym, startepoch - (86400 * 30), endepoch, '1Min')
        except Exception as e:
            daily_strategy_logger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}: {e}")
            raise Exception(e)

        if df is None:
            self.strategyLogger.info(f"Data fetch returned None for {baseSym} between {startDate} and {endDate}.")
            raise Exception(f"Data not available for {baseSym} between {startDate} and {endDate}.")


        def getRenkoData(df, renkoSize):
                

            df = df.rename(
                columns={
                    "o": "open",
                    "h": "high",
                    "l": "low",
                    "c": "close",
                    "datetime": "date",
                }
            ) #convert open high low close for Renko
            renko = indicators.Renko(df)
            renko.brick_size = renkoSize
            renko.chart_type = indicators.Renko.PERIOD_CLOSE
            renkoData = renko.get_ohlc_data()

            renkoData = renkoData.rename(
                columns={
                    "open": "o",
                    "high": "h",
                    "low": "l",
                    "close": "c",
                    "date": "datetime",
                }
            ) #convert back to ohlc format
            
            renkoData["datetime2"] = renkoData["datetime"]
            renkoData.set_index("datetime2", inplace=True)
            renkoData = renkoData.reset_index().set_index("datetime2").sort_index()

            renkoData.index = (renkoData.index.values.astype(
                np.int64) // 10**9) - 19800  # type: ignore
            renkoData.insert(loc=0, column="ti", value=renkoData.index)

            renkoSize = renkoSize

            return renkoData

        df = df[df.index >= startepoch]
        df.dropna(inplace=True)
       

        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
      # df_renko.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_Renko.csv")


        col = ['Target', 'Stoploss', 'Expiry']
        self.addColumnsToOpenPnlDf(col)

        current_day=None
        previous_renko = None
        current_renko=None
        callTradeCounter=0
        putTradeCounter=0
        lotSize = int(getExpiryData(startDate.timestamp(), baseSym)["LotSize"])

        lastindextimeData = [0, 0]
        lastindextimeData_r=[0,0]


        for timeData in df.index:
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            new_day=self.humanTime.date()

            #Updating Renko
            if current_day != new_day:
                current_day=new_day
                day_close=df.loc[df.index[0],'c']
                renkoSize=round(day_close * 0.0015)
                daily_strategy_logger.info(f"Datetime:{self.humanTime}\tRenkoSize:{renkoSize}\tDayClose:{day_close}")
                df_renko=getRenkoData(df,renkoSize)
                # df_renko['uptrend'] = df_renko['uptrend']
                df_renko['signal']=np.where(df_renko['uptrend']==True,1,-1)
                df_renko.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_Renko.csv")


            # Skip time periods outside trading hours
            if self.humanTime.time() < time(9, 16) or self.humanTime.time() > time(15, 15):
                continue
             
            lastindextimeData.pop(0)
            lastindextimeData.append(timeData-60)
            colour_change_exit=True


            #Updating current_renko:        
            if lastindextimeData_r[1] in df_renko.index:
                current_renko = df_renko.at[lastindextimeData_r[1], 'signal']
            current_renko_value = current_renko.iloc[0] if isinstance(current_renko, pd.Series) else current_renko
            
            #Logging
            if lastindextimeData[1] in df.index:
                daily_strategy_logger.info(f"Datetime:{self.humanTime}\tOpen:{df.at[lastindextimeData[1],'o']}\tHigh:{df.at[lastindextimeData[1],'h']}\tLow:{df.at[lastindextimeData[1],'l']}\tClose:{df.at[lastindextimeData[1],'c']}\tRenko:{current_renko}")
            else:
                daily_strategy_logger.info(f"Data not found for timestamp: {lastindextimeData}")
    
            # Update PnL
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row['Symbol'], timeData)
                        self.openPnl.at[index, 'CurrentPrice'] = data['c']
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()
          
            # Trail MaxLoss
            self.daily_pnl = self.netPnl
            if self.daily_pnl - self.last_pnl_checkpoints[-1] >= 500:
                self.max_loss = max(8000 - (len(self.last_pnl_checkpoints) * 500), 0)
                self.last_pnl_checkpoints.append(self.daily_pnl)
                if len(self.last_pnl_checkpoints) > 15:
                    self.last_pnl_checkpoints.pop(0)
                daily_strategy_logger.info(f"MaxLoss Updated to {self.max_loss}\tLast_pnl: {self.last_pnl_checkpoints[-1]}")
            
            putTradeCounter=self.openPnl['Symbol'].str[-2:].value_counts().get('PE', 0)
            callTradeCounter=self.openPnl['Symbol'].str[-2:].value_counts().get('CE', 0)

            #EXIT 
            if not self.openPnl.empty:
                #  max loss exit                            
                if (self.daily_pnl <= -self.max_loss):
                    for index in self.openPnl.index:
                        self.exitOrder(index, "Max Daily Loss Hit")
                    daily_strategy_logger.info(f"\nMaxLossHit of {-self.max_loss}.STOP\n")
                     
                for index, row in self.openPnl.iterrows():
                    symSide = row['Symbol'][-2:]

                    #  renko color change exit    
                    if (previous_renko) and (current_renko_value != previous_renko) and colour_change_exit==True:
                        if (symSide== "PE" and current_renko_value == -1) or \
                            (symSide == "CE" and current_renko_value == +1):
                            self.exitOrder(index, "Renko Color Change")
                            daily_strategy_logger.info( f"\nExit for {row['Symbol']} RenkoColChange" )
                            colour_change_exit= False           # Exit first sell position
                            
                    #  end of day exit
                    if self.humanTime.time() >= time(15, 15):
                        self.exitOrder(index, "End of Day")
                       
                    # # Trade Counter
                    # if (index not in self.openPnl.index) & (symSide == "CE"):
                    #     callTradeCounter -= 1
                    # elif (index not in self.openPnl.index) & (symSide == "PE"):
                    #     putTradeCounter -= 1

           # ENTRY 
            
            if self.daily_pnl > -self.max_loss:   #No entry after MaxLossHit
                #Bullish

                if current_renko_value == +1 and putTradeCounter < 3:
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastindextimeData[1], 'c'])
                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, timeData)
                    except Exception as e:
                        self.strategyLogger.info(e)
                        
                    self.entryOrder(data['c'], putSym, lotSize, "SELL")
                    putTradeCounter +=1

                    daily_strategy_logger.info(f"\nDatetime:{self.humanTime}\tOpen:{df.at[lastindextimeData[1],'o']}\tHigh:{df.at[lastindextimeData[1],'h']}\tLow:{df.at[lastindextimeData[1],'l']}\tClose:{df.at[lastindextimeData[1],'c']}\t\tRenko: {current_renko_value}\tEntry:{putSym}\tTradeCountPE:{putTradeCounter}\n")
                
                    #Bearish
                elif current_renko_value == -1  and callTradeCounter < 3:
                        callSym = self.getCallSym(self.timeData, baseSym, df.at[lastindextimeData[1], 'c'])
                        try:
                            data = self.fetchAndCacheFnoHistData(callSym, timeData)
                        except Exception as e:
                            self.strategyLogger.info(e)
                            continue
                        self.entryOrder(data['c'], callSym, lotSize, "SELL")
                        callTradeCounter +=1
                        daily_strategy_logger.info(f"\nDatetime:{self.humanTime}\tOpen:{df.at[lastindextimeData[1],'o']}\tHigh:{df.at[lastindextimeData[1],'h']}\tLow:{df.at[lastindextimeData[1],'l']}\tClose:{df.at[lastindextimeData[1],'c']}\tRenko: {current_renko_value}\tEntry:{callSym}\tTradeCountCE:{callTradeCounter}\t\n")

            previous_renko = current_renko_value

            lastindextimeData_r.pop(0)
            lastindextimeData_r.append(timeData)

        
        self.pnlCalculator()
        self.combinePnlCsv()



if __name__ == "__main__":
    start = datetime.now()

    # Define Strategy Nomenclature
    devName = "HA"
    strategyName = "Renko"
    version = "v1"

    # Define Start date and End date
    startDate = datetime(2024, 10, 1, 9, 15)
    endDate = datetime(2024, 10, 31, 15, 30)

    # Create algoLogic object
    algo = Renko(devName, strategyName, version)

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

    end = datetime.now()
    print(f"Done. Ended in {end-start}.")



