import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
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
    valid_stocks = []  # Keep track of stocks with valid data
    for stock in stocks:
        try:
            # Ensure stock is a string
            if not isinstance(stock, str):
                raise ValueError(f"Invalid stock name: {stock}. Expected a string.")

            # Debug: Print the stock name being processed
            print(f"Fetching data for stock: {stock}")

            # Fetch historical data for the stock with start_date and end_date
            df = getEquityBacktestData(stock, start_date.timestamp(), end_date.timestamp(), "75min")  # Fetch daily data
            if df is None or df.empty:
                print(f"Data not found for {stock}. Skipping...")
                continue  # Skip this stock if no data is found

            # Keep only the closing price and rename the column to the stock name
            df = df[['c']].rename(columns={'c': stock})
            data_frames.append(df)
            valid_stocks.append(stock)  # Add to valid stocks list
        except Exception as e:
            print(f"Error fetching data for {stock}: {e}")
    return data_frames, valid_stocks

# Combine all stock data into a single DataFrame
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

# Create a DataFrame for average returns and standard deviation
def calculate_avg_std(returns_df, valid_stocks):
    avg_std_df = pd.DataFrame({
        'Stock': valid_stocks,
        'AverageReturn': returns_df.mean(),
        'StdDevReturn': returns_df.std()
    }).reset_index(drop=True)
    return avg_std_df

# Perform KNN clustering
def perform_knn_clustering(data, n_clusters=4):
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    data['Cluster'] = kmeans.fit_predict(data[['AverageReturn', 'StdDevReturn']])
    return data, kmeans

# Plot clusters and save as JPEG
def plot_clusters(data, kmeans, best_cluster_number, output_path):
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(data['AverageReturn'], data['StdDevReturn'], c=data['Cluster'], cmap='viridis', s=50)
    plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], c='red', marker='X', s=200, label='Centroids')

    # Annotate the best cluster
    plt.annotate(f"Best Cluster: {best_cluster_number}",
                 (kmeans.cluster_centers_[best_cluster_number, 0], kmeans.cluster_centers_[best_cluster_number, 1]),
                 textcoords="offset points", xytext=(10, -10), ha='center', color='blue', fontsize=12, fontweight='bold')

    plt.title('KNN Clustering of Stocks')
    plt.xlabel('Average Return')
    plt.ylabel('Standard Deviation of Return')
    plt.legend(*scatter.legend_elements(), title="Clusters")
    plt.grid()
    plt.savefig(output_path, format='jpeg')  # Save the plot as a JPEG file
    print(f"Cluster plot saved as {output_path}")
    plt.close()

# Apply the Elbow Method to find the optimal number of clusters
def apply_elbow_method(data, max_clusters=10, output_path="/root/aniket/ML/elbow_plot.jpeg"):
    inertia = []
    cluster_range = range(1, max_clusters + 1)

    for k in cluster_range:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(data[['AverageReturn', 'StdDevReturn']])
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
    stock_file = "/root/aniket/nifty_50.md"
    stocks = load_stock_names(stock_file)

    # Define start and end dates
    start_date = pd.Timestamp("2021-01-01")
    end_date = pd.Timestamp("2024-05-31")

    # Fetch stock data
    data_frames, valid_stocks = fetch_stock_data(stocks, start_date, end_date)

    # Create combined DataFrame
    combined_df = create_combined_dataframe(data_frames)

    # Calculate returns
    returns_df = calculate_returns(combined_df)

    # Calculate average returns and standard deviation
    avg_std_df = calculate_avg_std(returns_df, valid_stocks)

    # Apply the Elbow Method
    apply_elbow_method(avg_std_df)

    # Perform KNN clustering with the chosen number of clusters (e.g., 4)
    clustered_data, kmeans = perform_knn_clustering(avg_std_df, n_clusters=4)

    # Analyze clusters
    cluster_analysis = clustered_data.groupby('Cluster').agg(
        MeanReturn=('AverageReturn', 'mean'),
        StdDev=('StdDevReturn', 'mean'),
        StockCount=('Stock', 'count')
    ).reset_index()

    print("Cluster Analysis:")
    print(cluster_analysis)

    # Identify the cluster with the highest average return and lowest standard deviation
    best_cluster = cluster_analysis.loc[
        cluster_analysis['MeanReturn'].idxmax() & cluster_analysis['StdDev'].idxmin()
    ]
    best_cluster_number = int(best_cluster['Cluster'])  # Ensure this is an integer
    print("\nBest Cluster:")
    print(best_cluster)

    # Print the stocks in the best cluster
    best_cluster_stocks = clustered_data[clustered_data['Cluster'] == best_cluster_number]['Stock']
    print("\nStocks in the Best Cluster:")
    print(best_cluster_stocks.tolist())

    # Plot clusters and save as JPEG
    plot_clusters(clustered_data, kmeans, best_cluster_number, "/root/aniket/ML/cluster_plot.jpeg")

if __name__ == "__main__":
    main()