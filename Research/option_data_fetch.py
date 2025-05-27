from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from datetime import datetime, timedelta

# Set parameters
baseSym = "NIFTY"
indexName = "NIFTY 50"
startDate = datetime(2025, 5, 1, 9, 15)
endDate = datetime(2025, 5, 31, 15, 30)  # One month duration

# Convert to epoch
startEpoch = startDate.timestamp()
endEpoch = endDate.timestamp()

# Change expiry from CurrentExpiry to MonthlyExpiry
monthlyExpiry = getExpiryData(startEpoch, baseSym)['MonthlyExpiry']
expiryDatetime = datetime.strptime(monthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
expiryEpoch = expiryDatetime.timestamp()

# Fetch one callSymbol
# Assume ATM price of 18000 for NIFTY
# This method may differ based on your actual implementation
strikePrice = 25000
data = fetchAndCacheFnoHistData( callSym , lastIndexTimeData[1])
callSym = int(row['Symbol'][-7:-2])

# Fetch FNO data for the callSymbol at 5-min interval
callData_5min = getFnoBacktestData(callSym, startEpoch, endEpoch, "5Min")

# Fetch NIFTY 50 index data at 5-min interval for the same time range
nifty_5min = getFnoBacktestData(indexName, startEpoch, endEpoch, "5Min")

# Save or return the data
callData_5min.to_csv("callSymbol_data.csv")
nifty_5min.to_csv("nifty50_data.csv")

print("Data saved for call symbol and NIFTY 50 at 5-minute interval.")
