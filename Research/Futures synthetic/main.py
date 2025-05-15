from backtestTools.util import calculateDailyReport, limitCapital, generateReportFile
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from datetime import datetime, time, timedelta
from backtestTools.expiry import getExpiryData
import talib as ta
import numpy as np
import pandas as pd
import csv

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            if df is None:
                return            
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Min.csv")

        lastIndexTimeData = [0, 0]

        CurrentExpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(CurrentExpiry, "%d%b%y").replace(hour=15, minute=20)

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 15)) | (self.humanTime.time() > time(15, 00)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData)

            if (self.humanTime.time() < time(9, 15)) | (self.humanTime.time() > time(15, 00)):
                continue

            if self.humanTime.date() > expiryDatetime.date() :
                CurrentExpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(CurrentExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if (timeData in df.index):
                    
                    strikePrice = None
                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], CurrentExpiry)
                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], CurrentExpiry)
                    data_put = None
                    data_call = None
                    strikePrice = putSym[-7:-2]
                    try:
                        data_put = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        data_call = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                        self.strategyLogger.info(f"Data not found for {putSym} or {callSym}")
                        continue

                    if data_call is not None and data_put is not None and strikePrice is not None:
                        log_message = f"{self.humanTime}, {strikePrice}, {putSym}, {data_put['c']}, {callSym}, {data_call['c']}"
                        self.strategyLogger.info(log_message)
                        with open("straddle_log.txt", "a") as f:
                            f.write(log_message + "\n")
                        data_call = None
                        data_put = None

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]

if __name__ == "__main__":
    startTime = datetime.now()

    devName = "NA"
    strategyName = "strikePriceLog"
    version = "v1"

    startDate = datetime(2025, 5, 2, 9, 15)
    endDate = datetime(2025, 5, 30, 15, 00)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    input_file = 'straddle_log.txt'
    output_file = 'synthetic.csv'

    # Write CSV with header
    with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(["Datetime","Strike_Price","putSym", "Put_Premium","callSym","Call_Premium"])
        for line in infile:
            if line.strip():
                parts = [p.strip() for p in line.strip().split(',')]
                if len(parts) == 6:
                    writer.writerow(parts)

    # Set Datetime as index in the CSV using pandas
    df = pd.read_csv(output_file)
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df.set_index('Datetime', inplace=True)
    df["Synthetic"] = (df["Put_Premium"] - df["Call_Premium"]) + df['Strike_Price']
    df.to_csv(output_file)

    print(f"CSV file '{output_file}' created successfully with Datetime as index.")

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")