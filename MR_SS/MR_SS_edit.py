from datetime import datetime, time, timedelta, date
import math
import multiprocessing
import numpy as np
import talib as ta
import pandas_ta as pta
from termcolor import colored, cprint
from backtestTools.util import setup_logger, createPortfolio, calculateDailyReport, limitCapital, generateReportFile
from backtestTools.expiry import getExpiryData
from backtestTools.histData import getEquityBacktestData, getFnoBacktestData, connectToMongo
from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic, optOverNightAlgoLogic
import os
import shutil
import logging
import pandas as pd


class MR_SS_Strategy(optOverNightAlgoLogic):
    def __init__(self, devName, strategyName, version):
        super().__init__(devName, strategyName, version)
        self.symMap = {
            "SENSEX": "SENSEX"
        }

        self.strikeDistMap = {
            "SENSEX": 100
        }

        self.lotSizeMap = {
            "SENSEX": 20
        }

    def getFileDir(self):
        return self.fileDir["backtestResultsStrategyUid"]

    

    def getCallSym(self, baseSym, expiry, indexPrice,strikeDist, otmFactor=0):
        symWithExpiry = baseSym + expiry

        # Calculate nearest multiple of 500
        remainder = indexPrice % 500
        atm = (indexPrice - remainder if remainder <= 250 else indexPrice - remainder + 500)

        if int(atm + (otmFactor * 500)) == (atm + (otmFactor * 500)):
            callSym = (symWithExpiry + str(int(atm + (otmFactor * 500))) + "CE")
        else:
            callSym = (symWithExpiry + str(float(atm + (otmFactor * 500))) + "CE")

        return callSym

    def getPutSym(self, baseSym, expiry, indexPrice, strikeDist, otmFactor=0):
        symWithExpiry = baseSym + expiry

        remainder = indexPrice % 500
        atm = (indexPrice - remainder if remainder <= 250 else (
            indexPrice - remainder + 500))

        if int(atm - (otmFactor * 500)) == (atm - (otmFactor * 500)):
            putSym = (symWithExpiry +
                      str(int(atm - (otmFactor * 500))) + "PE")
        else:
            putSym = (symWithExpiry +
                      str(float(atm - (otmFactor * 500))) + "PE")

        return putSym

    def runBacktest(self, baseSym, startDate, endDate):
        # Set start and end timestamps for data retrieval
        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()


        self.humanTime = startDate
        self.addColumnsToOpenPnlDf(["Expiry"])

        self.strategyLogger.info("TEST")

        try:
            df = getFnoBacktestData(
                self.symMap[baseSym], startTimeEpoch-(86400 * 30), endTimeEpoch, "T")
            df_1d = getFnoBacktestData(
                self.symMap[baseSym], startTimeEpoch-(86400 * 30), endTimeEpoch, "1D")
        except Exception as e:
            raise Exception(e)

        if df is None:
            self.strategyLogger.info(f"Data not found for {baseSym}")
            return

        df.dropna(inplace=True)
        df = df[df.index >= startTimeEpoch]

        df_1d.dropna(inplace=True)
        df_1d = df_1d[df_1d.index >= startTimeEpoch]

        df_1d.index = df_1d.index + 33300
        df_1d.ti = df_1d.ti + 33300


        # Filter dataframe from timestamp greater than (30 mins before) start time timestamp
        df = df[df.index > startTimeEpoch - (15*60*2)]
        df_1d = df_1d[df_1d.index > startTimeEpoch - (15*3600*2)]

        df.dropna(inplace=True)
        df = df[df.index >= startTimeEpoch]

        df_1d.dropna(inplace=True)
        df_1d = df_1d[df_1d.index >= startTimeEpoch]
  

        df.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{baseSym}_1Min.csv")

        df_1d.to_csv(
            f"{self.fileDir['backtestResultsCandleData']}{baseSym}_1Day.csv")


        currentExpiry = getExpiryData(startDate, baseSym)['CurrentExpiry']
        currentExpiryDt = datetime.strptime(
            currentExpiry, "%d%b%y").replace(hour=15, minute=20)
        



        lastIndexTimeData = [0, 0]
        lastIndex1dTimeData = [0, 0]
        last_close =  None
        last_open = None
        call_sell = 0
        put_sell = 0
        pe_count = 0
        ce_count = 0        
        startAtmFactor = 0
        previous_trade_date = 0
        last_1_min_close = 0
        last_1_min_open = 0


        for timeData in df.index:
            self.timeData = timeData
            self.humanTime = datetime.fromtimestamp(timeData)

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)


            previous_day = timeData - 86400  # Subtract 1 day (in seconds)

            if timeData in df_1d.index and df_1d.index.get_loc(timeData) > 1:
                # Check if previous day exists in the DataFrame
                while previous_day not in df_1d.index:  # Use the column where epoch times are stored
                    previous_day -= 86400


            if timeData in df_1d.index:
                lastIndex1dTimeData.pop(0)
                lastIndex1dTimeData.append(previous_day)

           
                
            if (lastIndex1dTimeData[1] in df_1d.index) and (lastIndexTimeData[1] in df.index):
                # self.strategyLogger.info(
                #     f"Datetime: {self.humanTime}\tlastIndexTimeData: {lastIndexTimeData}\tlastIndex1dTimeData {lastIndex1dTimeData}")                            
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1], 'c']}\tOpen: {df.at[lastIndexTimeData[1], 'o']}\tClose_1d: {df_1d.at[lastIndex1dTimeData[1], 'c']}\tOpen_1d: {df_1d.at[lastIndex1dTimeData[1], 'o']}")                            
                last_open = df_1d.at[lastIndex1dTimeData[1],'o']
                last_close = df_1d.at[lastIndex1dTimeData[1],'c']

            if (lastIndex1dTimeData[1] in df_1d.index) and (lastIndexTimeData[1] in df.index):
                if self.humanTime.time() == time(15,29):
                    last_1_min_close = df.at[timeData,'c']
                    last_1_min_open = df.at[timeData,'o']                    

            # Expiry for entry
            if self.humanTime.date() >= currentExpiryDt.date():
                currentExpiry = getExpiryData(self.humanTime + timedelta(days=1), baseSym)[
                    'CurrentExpiry']
                currentExpiryDt = datetime.strptime(
                    currentExpiry, "%d%b%y").replace(hour=15, minute=20)         
            

            if (lastIndex1dTimeData[1] in df_1d.index) and (lastIndexTimeData[1] in df.index):
                pe_count = self.openPnl['Symbol'].str.contains('PE').sum()
                ce_count = self.openPnl['Symbol'].str.contains('CE').sum()
                self.strategyLogger.info(
                    f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1], 'c']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(
                            row['Symbol'], lastIndexTimeData[1], maxCacheSize=1000)
                        self.openPnl.at[index,
                                        "CurrentPrice"] = data['c']
                    except Exception as e:
                        self.strategyLogger.info(
                            f"Datetime: {self.humanTime}\tCouldn't update current price for {row['Symbol']} at {lastIndexTimeData[1]}")
                                     

            # Exit
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    sym = row["Symbol"]
                    symSide = sym[len(sym) - 2:]
                    if (self.humanTime >= row["Expiry"]):
                        exitType = "Expiry Hit"
                        self.exitOrder(index, exitType)
                    elif (row['CurrentPrice'] <= (0.3*row['EntryPrice'])):
                        exitType = "Target"
                        self.exitOrder(index, exitType)
                    elif (row['CurrentPrice'] >= 1.5*row['EntryPrice']):
                        exitType = "Stoploss Hit"
                        self.exitOrder(index, exitType)
               

            self.pnlCalculator()

            #Entry Pre-conditions
            # if not last_open or not last_close:
            #     continue

            if not last_open:
                last_open = last_1_min_open
            if not last_close:
                last_close = last_1_min_close            

            if self.humanTime.time() == time(9,16):
                if last_open>last_close:
                    put_sell = True
                    call_sell = False
                elif last_open<last_close:
                    call_sell = True
                    put_sell = False

            if (lastIndex1dTimeData[1] in df_1d.index) and (lastIndexTimeData[1] in df.index):
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tcurrentExpiryDt:{currentExpiryDt}\tcurrentExpiryDt:{currentExpiryDt}\tput_sell:{put_sell}\tcall_sell:{call_sell}\tlast_open:{last_open}\tlast_close:{last_close}")

            # Entry
            if (lastIndexTimeData[1] in df.index) and (self.humanTime.time() < time(15,19)) and (self.humanTime.time() > time(9, 15)) and (previous_trade_date != self.humanTime.date()):
                if  call_sell and (ce_count < 2):                             
                    ceData = None
                    callSym = self.getCallSym(
                            baseSym, currentExpiry, df.at[lastIndexTimeData[1], 'c'], self.strikeDistMap[baseSym], otmFactor=0)
                    self.strategyLogger.info(f"Checking entry data for {callSym}")
                    try:
                        ceData = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[0], maxCacheSize=100)
                    except Exception as e:
                        ceData = None
                        self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {callSym}")                        
                    
                 
                    if ceData is not None:
                        self.entryOrder(ceData['c'], callSym,  self.lotSizeMap[baseSym], "SELL", {"Expiry": currentExpiryDt})
                        previous_trade_date = self.humanTime.date()
                        self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tCALL SELL ENTRY {callSym} @ {ceData['c']}\n")
                    else:
                        self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {callSym}\n")



                elif put_sell and  (pe_count < 2):  
                    # Put Option (PE) handling
                    peData = None
                    putSym = self.getPutSym(
                        baseSym, currentExpiry, df.at[lastIndexTimeData[1], 'c'], self.strikeDistMap[baseSym], otmFactor=0)
                    self.strategyLogger.info(f"Checking entry data for {putSym}")
                    try:
                        peData = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[0], maxCacheSize=100)
                    except Exception as e:
                        peData = None
                        self.strategyLogger.info(f"Datetime: {self.humanTime}\tEntry Data not available for {putSym}")

                                                                  
                    if peData is not None:
                        # quantity = math.floor(50_000 / (peData['c'] * self.lotSizeMap[baseSym])) * self.lotSizeMap[baseSym]
                        self.entryOrder(peData['c'], putSym,  self.lotSizeMap[baseSym] , "SELL", {"Expiry": currentExpiryDt})
                        previous_trade_date = self.humanTime.date()                        
                        self.strategyLogger.info(f"\nDatetime: {self.humanTime}\tPUT SELL ENTRY {putSym} @ {peData['c']}\n")
                    else:
                        self.strategyLogger.warning(f"\nDatetime: {self.humanTime}\tData not available for {putSym}\n")


        return self.combinePnlCsv()


if __name__ == "__main__":
    startNow = datetime.now()

    baseSym = "SENSEX"

    # Define Strategy Nomenclature
    devName = "NA"
    strategyName = "MR_SS"
    version = "PREMIUM_200_1000"

    # Define Start date and End date
    startDate = datetime(2025, 1, 1, 9, 15)
    # endDate = datetime(2025, 1, 31, 15, 30)
    # endDate = datetime(2024, 1, 11, 0, 0)
    endDate = datetime.now()

    algoLogicObj = MR_SS_Strategy(devName, strategyName, version)

    # Copy strategy Code
    sourceFile = os.path.abspath(__file__)
    fileDir = algoLogicObj.getFileDir()
    shutil.copy2(sourceFile, fileDir)

    closedPnl = algoLogicObj.runBacktest(baseSym, startDate,
                                         endDate)

    # Generate metric report based on backtest results
    print("Starting post processing calculation...")

    dailyReport = calculateDailyReport(
        closedPnl, fileDir, timeFrame=timedelta(minutes=15), mtm=True, fno=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=100000)

    generateReportFile(dailyReport, fileDir)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")