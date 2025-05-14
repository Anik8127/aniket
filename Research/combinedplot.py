import pandas as pd
import matplotlib.pyplot as plt

# Load the Excel file
file_path = "/root/aniket/Research/25-02-2025/combined_premium.csv"
data = pd.read_csv(file_path)

# Define the columns to plot
columns_to_plot = ["Combined_Premium", "PremiumcallSym", "PremiumputSym"]

# Create a combined line plot
plt.figure(figsize=(12, 8))
for column in columns_to_plot:
    plt.plot(data[column], label=column, linewidth=2)

plt.title("Combined Line Plot for Selected Columns")
plt.xlabel("Index")
plt.ylabel("Values")
plt.legend()
plt.grid(True)

# Save the combined plot as a JPEG file
output_file = "combined_plot.jpeg"
plt.savefig(output_file, format='jpeg')
plt.close()

print("Combined plot saved successfully.")