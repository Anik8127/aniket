import pandas as pd
import plotly.graph_objects as go

# Load the CSV file
file_path = "/root/aniket/Research/08-05-2025/Nifty/combined_premium.csv"
data = pd.read_csv(file_path)

# Convert the datetime column to pandas datetime and set it as the index
data['Datetime'] = pd.to_datetime(data['Datetime'])  # Replace 'datetime' with the actual column name for datetime
data.set_index('Datetime', inplace=True)

# Create the figure
fig = go.Figure()

# Add traces for the primary columns
fig.add_trace(go.Scatter(
    x=data.index,
    y=data["Combined_Premium"],
    mode='lines',
    name="Combined_Premium"
))

fig.add_trace(go.Scatter(
    x=data.index,
    y=data["PremiumcallSym"],
    mode='lines',
    name="PremiumcallSym"
))

fig.add_trace(go.Scatter(
    x=data.index,
    y=data["PremiumputSym"],
    mode='lines',
    name="PremiumputSym"
))

# Add a secondary y-axis trace for put-call_ratio
fig.add_trace(go.Scatter(
    x=data.index,
    y=data["Put-Call Ratio"],
    mode='lines',
    name="Put-Call Ratio",
    yaxis="y2",
    line=dict(color="red", dash="dash")
))

# Update layout for dual y-axes
fig.update_layout(
    title="Interactive Combined Line Plot with Put-Call Ratio",
    xaxis=dict(title="Datetime", tickformat="%Y-%m-%d %H:%M:%S"),  # Format datetime for better readability
    yaxis=dict(title="Values (Primary Axis)"),
    yaxis2=dict(
        title="Put-Call Ratio (Secondary Axis)",
        overlaying="y",
        side="right"
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=-0.2,
        xanchor="center",
        x=0.5
    )
)

# Save the interactive plot as an HTML file
output_file = "/root/aniket/Research/08-05-2025/Nifty/COMBINED_interactive_plot.html"
fig.write_html(output_file)

# Show the interactive plot
fig.show()