import pandas as pd
import matplotlib.pyplot as plt

# Load the Excel file
file_path = "/root/aniket/Research/25-02-2025/combined_premium.csv"
data = pd.read_csv(file_path)
data['put-call_ratio'] = data['PremiumcallSym'] / data['PremiumputSym']

# Define the columns to plot
columns_to_plot = ["Combined_Premium", "PremiumcallSym", "PremiumputSym","put-call_ratio"]

# Loop through each column and create individual line plots
for column in columns_to_plot:
    plt.figure(figsize=(10, 6))
    plt.plot(data[column], label=column, color='blue', linewidth=2)
    plt.title(f"Line Plot for {column}")
    plt.xlabel("Index")
    plt.ylabel(column)
    plt.legend()
    plt.grid(True)
    # Save the plot as a JPEG file
    output_file = f"{column}.jpeg"
    plt.savefig(output_file, format='jpeg')
    plt.close()

print("Plots saved successfully.")