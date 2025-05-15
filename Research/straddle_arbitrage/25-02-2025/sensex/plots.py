import pandas as pd
import plotly.graph_objects as go

# Load the CSV file
file_path = "/root/aniket/Research/straddle_arbitrage/25-02-2025/sensex/combined_premium.csv"
data = pd.read_csv(file_path)

# Convert the Datetime column to pandas datetime and set it as the index
data['Datetime'] = pd.to_datetime(data['Datetime'])  # Replace 'Datetime' with the actual column name for datetime
data.set_index('Datetime', inplace=True)

# Define the columns to plot
columns_to_plot = ["Combined_Premium", "PremiumcallSym", "PremiumputSym", "Put-Call Ratio"]

# Loop through each column and create individual interactive plots
for column in columns_to_plot:
    # Create the figure
    fig = go.Figure()

    # Add the trace for the current column
    fig.add_trace(go.Scatter(
        x=data.index,
        y=data[column],
        mode='lines',
        name=column
    ))

    # Update layout
    fig.update_layout(
        title=f"Interactive Line Plot for {column}",
        xaxis=dict(title="Datetime", tickformat="%Y-%m-%d %H:%M:%S"),  # Format datetime for better readability
        yaxis=dict(title=column),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )

    # Save the interactive plot as an HTML file
    output_file = f"/root/aniket/Research/straddle_arbitrage/25-02-2025/sensex/plots/{column}_interactive_plot.html"
    fig.write_html(output_file)

    print(f"Interactive plot for {column} saved as {output_file}")

    # Show the plot in a browser window
    fig.show()

print("All interactive plots saved successfully.")