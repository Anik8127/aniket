import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from backtestTools.histData import getEquityBacktestData
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load stock names from the file
def load_stock_names(file_path):
    stocks = []
    with open(file_path, "r") as file:
        for line in file:
            stocks.append(line.strip())  # Add each line to the list after stripping whitespace
    return stocks

def fetch_single_stock(stock, start_date, end_date):
    try:
        if not isinstance(stock, str):
            raise ValueError(f"Invalid stock name: {stock}. Expected a string.")
        print(f"Fetching data for stock: {stock}")
        df = getEquityBacktestData(stock, start_date.timestamp(), end_date.timestamp(), "5min")
        if df is None or df.empty:
            print(f"Data not found for {stock}. Skipping...")
            return None, None, None
        price_df = df[['c']].rename(columns={'c': stock})
        volume_df = df[['v']].rename(columns={'v': stock})
        return price_df, volume_df, stock
    except Exception as e:
        print(f"Error fetching data for {stock}: {e}")
        return None, None, None

# Fetch historical data for all stocks (now also fetch volume)
def fetch_stock_data(stocks, start_date, end_date, max_workers=8):
    price_frames = []
    volume_frames = []
    valid_stocks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_single_stock, stock, start_date, end_date) for stock in stocks]
        for future in as_completed(futures):
            price_df, volume_df, stock = future.result()
            if price_df is not None and volume_df is not None and stock is not None:
                price_frames.append(price_df)
                volume_frames.append(volume_df)
                valid_stocks.append(stock)
    return price_frames, volume_frames, valid_stocks

# Combine all stock data into a single DataFrame (for price and volume)
def create_combined_dataframe(data_frames):
    combined_df = pd.concat(data_frames, axis=1)  # Combine all data frames by columns
    combined_df.reset_index(inplace=True)  # Reset the index to include datetime as a column
    combined_df.rename(columns={'index': 'datetime'}, inplace=True)  # Rename the index column to 'datetime'
    combined_df.dropna(inplace=True)  # Remove any empty rows
    return combined_df

# Calculate returns for each stock
def calculate_returns(df):
    returns_df = df.set_index('datetime').pct_change().dropna()  # Calculate percentage change and drop NaN rows
    return returns_df

# Calculate average volume for each stock
def calculate_avg_volume(volume_df, valid_stocks):
    avg_volume = volume_df[valid_stocks].mean()
    return avg_volume

# Create a DataFrame for stddev and average volume
def calculate_volatility_liquidity(returns_df, avg_volume, valid_stocks):
    df = pd.DataFrame({
        'Stock': valid_stocks,
        'StdDevReturn': returns_df.std(),
        'AvgVolume': avg_volume
    }).reset_index(drop=True)
    return df

# Perform KMeans clustering on volatility and liquidity
def perform_kmeans_clustering(data, n_clusters=4):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['Cluster'] = kmeans.fit_predict(data[['StdDevReturn', 'AvgVolume']])
    return data, kmeans

# Plot clusters and save as JPEG
def plot_clusters(data, kmeans, best_cluster_number, output_path):
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(data['StdDevReturn'], data['AvgVolume'], c=data['Cluster'], cmap='viridis', s=50)
    plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], c='red', marker='X', s=200, label='Centroids')

    # Annotate the best cluster
    plt.annotate(f"Best Cluster: {best_cluster_number}",
                 (kmeans.cluster_centers_[best_cluster_number, 0], kmeans.cluster_centers_[best_cluster_number, 1]),
                 textcoords="offset points", xytext=(10, -10), ha='center', color='blue', fontsize=12, fontweight='bold')

    plt.title('KNN Clustering of Stocks')
    plt.xlabel('Standard Deviation of Return')
    plt.ylabel('Average Volume')
    plt.legend(*scatter.legend_elements(), title="Clusters")
    plt.grid()
    plt.savefig(output_path, format='jpeg')  # Save the plot as a JPEG file
    print(f"Cluster plot saved as {output_path}")
    plt.close()

# Apply the Elbow Method to find the optimal number of clusters
def apply_elbow_method(data, max_clusters=10, output_path="/root/aniket/Equity/Equity_intraday/intraday1.py/elbow_plot.jpeg"):
    inertia = []
    cluster_range = range(1, max_clusters + 1)

    for k in cluster_range:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(data[['StdDevReturn', 'AvgVolume']])
        inertia.append(kmeans.inertia_)  # Append the inertia (sum of squared distances)

    # Plot the Elbow Method
    plt.figure(figsize=(10, 6))
    plt.plot(cluster_range, inertia, marker='o', linestyle='--')
    plt.title('Elbow Method for Optimal Clusters')
    plt.xlabel('Number of Clusters')
    plt.ylabel('Inertia')
    plt.grid()
    plt.savefig(output_path, format='jpeg')  # Save the elbow plot as a JPEG file
    print(f"Elbow plot saved as {output_path}")
    plt.close()

# Main function
def main():
    # Load stock names
    stock_file = "/root/aniket/349_stocks.md"
    stocks = load_stock_names(stock_file)

    # Define start and end dates
    start_date = pd.Timestamp("2021-01-01")
    end_date = pd.Timestamp("2023-12-31")

    # Fetch stock data (prices and volumes)
    price_frames, volume_frames, valid_stocks = fetch_stock_data(stocks, start_date, end_date)

    # Create combined DataFrames
    combined_price_df = create_combined_dataframe(price_frames)
    combined_volume_df = create_combined_dataframe(volume_frames)

    # Calculate returns
    returns_df = calculate_returns(combined_price_df)

    # Calculate average volume
    avg_volume = calculate_avg_volume(combined_volume_df, valid_stocks)

    # Calculate volatility and liquidity DataFrame
    vol_liq_df = calculate_volatility_liquidity(returns_df, avg_volume, valid_stocks)

    # Apply the Elbow Method (optional, can use on vol_liq_df)
    apply_elbow_method(vol_liq_df)

    # Perform KMeans clustering
    clustered_data, kmeans = perform_kmeans_clustering(vol_liq_df, n_clusters=4)

    # Analyze clusters
    cluster_analysis = clustered_data.groupby('Cluster').agg(
        MeanVolatility=('StdDevReturn', 'mean'),
        MeanLiquidity=('AvgVolume', 'mean'),
        StockCount=('Stock', 'count')
    ).reset_index()

    print("Cluster Analysis:")
    print(cluster_analysis)

    # Identify the cluster with highest volatility and highest liquidity
    best_cluster = cluster_analysis.loc[
        (cluster_analysis['MeanVolatility'] == cluster_analysis['MeanVolatility'].max()) &
        (cluster_analysis['MeanLiquidity'] == cluster_analysis['MeanLiquidity'].max())
    ]
    if best_cluster.empty:
        # If no cluster matches both, pick the one with highest volatility
        best_cluster = cluster_analysis.loc[cluster_analysis['MeanVolatility'].idxmax()]
    else:
        best_cluster = best_cluster.iloc[0]
    best_cluster_number = int(best_cluster['Cluster'])
    print("\nBest Cluster (Highest Volatility & Liquidity):")
    print(best_cluster)

    # Print the stocks in the best cluster
    best_cluster_stocks = clustered_data[clustered_data['Cluster'] == best_cluster_number]
    print("\nStocks in the Best Cluster:")
    print(best_cluster_stocks['Stock'].tolist())

    # Plot clusters and save as JPEG
    plot_clusters(clustered_data, kmeans, best_cluster_number, "/root/aniket/Equity/Equity_intraday/intraday1.py")

if __name__ == "__main__":
    main()