from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
import numpy as np
import talib as ta
import pandas_ta as taa
from backtestTools.expiry import getExpiryData
from datetime import datetime, time, timedelta
import pandas as pd

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            if df is None:
                raise ValueError(f"No data returned for {indexSym} in the range {startDate} to {endDate}")
            
            df_5min = getFnoBacktestData(indexSym, startEpoch - 886400, endEpoch, "5Min")
            if df_5min is None:
                raise ValueError(f"No 5Min data returned for {indexSym} in the range {startDate} to {endDate}")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df_5min.dropna(inplace=True)

        df_5min = df_5min[df_5min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")

        callEntryAllow = True
        putEntryAllow = True        
        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]
        entry = False

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 00)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            if (timeData-300) in df_5min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeData-300)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 00)):
                continue

            if (timeData-300) in df_5min.index:
                self.strategyLogger.info(f"Datetime: {self.humanTime}\tClose: {df.at[lastIndexTimeData[1],'c']}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date() : #next day after expiry to build condor
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                entry= True
                
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = "Expiry Exit"
                        self.exitOrder(index, exitType)

            # tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            # callCounter= tradecount.get('CE',0)
            # putCounter= tradecount.get('PE',0)

            if ((timeData-300) in df_5min.index) and self.openPnl.empty:
                if entry == True and self.humanTime.time() == time(10, 15):    
                
                    #Call Entry
                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=4)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                    #callHedge Entry
                    callSym = self.getCallSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=2)

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch},)

                    #Put Entry
                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry, otmFactor=4)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch},)

                    #Put Hedge Entry
                    putSym = self.getPutSym(self.timeData, baseSym, df_5min.at[last5MinIndexTimeData[1], "c"],expiry= Currentexpiry,otmFactor=2)

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)

                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch},)
                    entry=False

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]

def create_combined_premium_dataframe(algo, df, baseSym, Currentexpiry):
    data_list = []

    for timeData in df.index:
        try:
            # Convert timestamp to human-readable datetime
            humanTime = datetime.fromtimestamp(timeData)

            # Skip rows outside trading hours
            if (humanTime.time() < time(9, 16)) or (humanTime.time() > time(15, 00)):
                continue

            # Get call and put symbols with otmFactor=0
            callSym = algo.getCallSym(timeData, baseSym, df.at[timeData, "c"], expiry=Currentexpiry, otmFactor=0)
            putSym = algo.getPutSym(timeData, baseSym, df.at[timeData, "c"], expiry=Currentexpiry, otmFactor=0)

            # Fetch premium data for call and put symbols
            call_data = algo.fetchAndCacheFnoHistData(callSym, timeData)
            put_data = algo.fetchAndCacheFnoHistData(putSym, timeData)

            # Extract premium values
            premium_call = call_data["c"]
            premium_put = put_data["c"]

            # Calculate combined premium
            combined_premium = premium_call + premium_put

            # Append data to the list
            data_list.append({
                "Datetime": humanTime,
                "callSym": callSym,
                "PremiumcallSym": premium_call,
                "putSym": putSym,
                "PremiumputSym": premium_put,
                "Combined_Premium": combined_premium
            })

        except Exception as e:
            algo.strategyLogger.info(f"Error processing timeData {timeData}: {e}")
            continue

    # Create DataFrame from the collected data
    df_combined = pd.DataFrame(data_list)
    df_combined['Put-Call Ratio'] = df_combined['PremiumcallSym'] / df_combined['PremiumputSym']

    # Drop any rows with missing data
    df_combined.dropna(inplace=True)

    return df_combined

if __name__ == "__main__":
    startTime = datetime.now()

    devName = "Aniket"
    strategyName = "Testing"
    version = "v1"

    startDate = datetime(2025, 4, 29, 9, 16)
    endDate = datetime(2025, 4, 29, 15, 00)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    # dr = calculateDailyReport(closedPnl, fileDir, timeFrame=timedelta(minutes=5), mtm=True)

    # limitCapital(closedPnl, fileDir, maxCapitalAmount=1000)
    # generateReportFile(dr, fileDir)

    # Example usage of create_combined_premium_dataframe
    df = getFnoBacktestData(indexName, startDate.timestamp(), endDate.timestamp(), "1Min")
    df.dropna(inplace=True)

    Currentexpiry = getExpiryData(startDate.timestamp(), baseSym)['CurrentExpiry']
    combined_premium_df = create_combined_premium_dataframe(algo, df, baseSym, Currentexpiry)

    print(combined_premium_df)
    combined_premium_df.to_csv("combined_premium.csv", index=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")