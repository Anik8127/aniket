import pandas as pd
from backtestTools.histData import getEquityBacktestData

# Load stock names from the file
def load_stock_names(file_path):
    stocks = []
    with open(file_path, "r") as file:
        for line in file:
            stocks.append(line.strip())  # Add each line to the list after stripping whitespace
    return stocks

# Fetch historical data for all stocks
def fetch_stock_data(stocks, start_date, end_date):
    data_frames = []
    for stock in stocks:
        try:
            # Ensure stock is a string
            if not isinstance(stock, str):
                raise ValueError(f"Invalid stock name: {stock}. Expected a string.")

            # Debug: Print the stock name being processed
            print(f"Fetching data for stock: {stock}")

            # Fetch historical data for the stock with start_date and end_date
            df = getEquityBacktestData(stock, start_date.timestamp(), end_date.timestamp(),"1D")  # Fetch daily data
            df = df[['c']].rename(columns={'c': stock})  # Keep only the closing price and rename the column to the stock name
            data_frames.append(df)
        except Exception as e:
            print(f"Error fetching data for {stock}: {e}")
    return data_frames

# Combine all stock data into a single DataFrame
def create_combined_dataframe(data_frames):
    combined_df = pd.concat(data_frames, axis=1)  # Combine all data frames by columns
    combined_df.reset_index(inplace=True)  # Reset the index to include datetime as a column
    combined_df.rename(columns={'index': 'datetime'}, inplace=True)  # Rename the index column to 'datetime'
    return combined_df

# Main function
def main():
    # Load stock names
    stock_file = "/root/aniket/vwap/stockNifty200.md"
    stocks = load_stock_names(stock_file)

    # Define start and end dates
    start_date = pd.Timestamp("2022-01-01")
    end_date = pd.Timestamp("2025-01-31")

    # Fetch stock data
    data_frames = fetch_stock_data(stocks, start_date, end_date)

    # Create combined DataFrame
    combined_df = create_combined_dataframe(data_frames)

    # Save the DataFrame to a CSV file (optional)
    combined_df.to_csv("/root/aniket/vwap/stock_data.csv", index=False)

    # Print the first few rows of the DataFrame
    print(combined_df.head())

if __name__ == "__main__":
    main()