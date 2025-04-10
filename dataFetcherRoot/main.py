# from backtestTools.histData import getEquityBacktestData
# from datetime import datetime


# stocks = [
#     "ADANIENT", "ADANIPORTS", "APOLLOHOSP", "ASIANPAINT", "AXISBANK", "BAJFINANCE",
#     "BAJAJFINSV", "BPCL", "BHARTIARTL", "BRITANNIA", "CIPLA", "COALINDIA", "DIVISLAB",
#     "DRREDDY", "EICHERMOT", "GRASIM", "HCLTECH", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
#     "HINDALCO", "HINDUNILVR", "ICICIBANK", "ITC", "INDUSINDBK", "INFY", "JSWSTEEL", 
#     "KOTAKBANK", "LT", "M&M", "MARUTI", "NTPC", "NESTLEIND", "ONGC", "POWERGRID", 
#     "RELIANCE", "SBILIFE", "SBIN", "SUNPHARMA", "TCS", "TATACONSUM", "TATAMOTORS", 
#     "TATASTEEL", "TECHM", "TITAN", "UPL", "ULTRACEMCO", "WIPRO"
# ]

# startDate = datetime(2022, 1, 1, 9, 15, 0)
# endDate = datetime(2024, 12, 31, 15, 30, 0)
# startTimeEpoch = startDate.timestamp()
# endTimeEpoch = endDate.timestamp()

# for stock in stocks:

#     df = getEquityBacktestData(stock, startTimeEpoch, endTimeEpoch, "5Min")

#     df.to_csv(f"allData/{stock}.csv", index=False)

#     print(df)



# ********************************** index ****************************************

from backtestTools.histData import getEquityBacktestData, getFnoBacktestData
from datetime import datetime

stock = "SENSEX"


startDate = datetime(2022, 1, 1, 9, 15, 0)
endDate = datetime(2024, 12, 31, 15, 30, 0)
startTimeEpoch = startDate.timestamp()
endTimeEpoch = endDate.timestamp()


df = getFnoBacktestData(stock, startTimeEpoch, endTimeEpoch, "5Min")

df.to_csv(f"allData/{stock}.csv", index=False)

print(df)