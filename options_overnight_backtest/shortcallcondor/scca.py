import pandas as pd
from datetime import datetime, time
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData

class algoLogic(optOverNightAlgoLogic):

    def generate_combined_dataframe(self, startDate, endDate, baseSym):
        """
        Generate a DataFrame with columns: callSym, putSym, combined(callSym+putSym), datetime.
        """
        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        # Fetch expiry data
        expiryData = getExpiryData(startEpoch, baseSym)
        if not expiryData or 'CurrentExpiry' not in expiryData or 'LotSize' not in expiryData:
            raise ValueError(f"Invalid expiry data returned: {expiryData}. Please check the input parameters or data source.")
        Currentexpiry = expiryData['CurrentExpiry']
        lotSize = int(expiryData["LotSize"])

        # Initialize an empty list to store combined data
        combined_data = []

        # Log fetching data
        self.strategyLogger.info(f"Fetching data for {baseSym} from {startEpoch} to {endEpoch} with interval '1Min'")

        # Fetch 1-minute interval data
        df = getFnoBacktestData(baseSym, startEpoch, endEpoch, "1Min")
        if df is None or df.empty:
            raise ValueError("No data returned by getFnoBacktestData. Please check the input parameters or data availability.")

        for timeData in df.index:
            humanTime = datetime.fromtimestamp(timeData)

            # Skip times outside trading hours
            if (humanTime.time() < time(9, 20)) or (humanTime.time() > time(15, 30)):
                continue

            # Get callSym and putSym for ATM (otmFactor=0)
            callSym = self.getCallSym(timeData, baseSym, df.at[timeData, "c"], expiry=Currentexpiry, otmFactor=0)
            putSym = self.getPutSym(timeData, baseSym, df.at[timeData, "c"], expiry=Currentexpiry, otmFactor=0)

            try:
                # Fetch closing prices for callSym and putSym
                call_data = self.fetchAndCacheFnoHistData(callSym, timeData)
                put_data = self.fetchAndCacheFnoHistData(putSym, timeData)

                if call_data is None or put_data is None:
                    raise ValueError(f"Data not found for callSym: {callSym} or putSym: {putSym} at time {humanTime}")

                call_price = call_data["c"]
                put_price = put_data["c"]

                # Append data to the list
                combined_data.append({
                    "callSym": callSym,
                    "putSym": putSym,
                    "combined(callSym+putSym)": call_price + put_price,
                    "datetime": humanTime
                })

            except Exception as e:
                self.strategyLogger.info(f"Error fetching data for {callSym} or {putSym}: {e}")
                continue

        # Convert the list to a DataFrame
        df_combined = pd.DataFrame(combined_data)

        if df_combined.empty:
            raise ValueError("No valid data could be fetched for the given date range.")

        return df_combined


if __name__ == "__main__":
    # Inputs for startDate and endDate
    startDate = datetime(2025, 4, 1, 9, 15)
    endDate = datetime(2025, 4, 30, 15, 30)

    devName = "Aniket"
    strategyName = "Short Call Condor"
    version = "v1"

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY 50"

    try:
        # Generate the DataFrame
        df_combined = algo.generate_combined_dataframe(startDate, endDate, baseSym)

        # Save the DataFrame to a CSV file
        df_combined.to_csv("combined_data.csv", index=False)

        print("DataFrame generated and saved to 'combined_data.csv'.")
    except Exception as e:
        print(f"An error occurred: {e}")