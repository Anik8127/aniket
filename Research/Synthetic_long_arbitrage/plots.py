import pandas as pd
import plotly.graph_objects as go

# Load data
df = pd.read_csv("/root/aniket/Research/Synthetic_long_arbitrage/final_df.csv")

# Create figure
fig = go.Figure()

# Add traces for main y-axis (Datetime on x-axis)
fig.add_trace(go.Scatter(x=df['Datetime'], y=df['Future_close'], mode='lines', name='Future_close'))
fig.add_trace(go.Scatter(x=df['Datetime'], y=df['Synthetic'], mode='lines', name='Synthetic'))
fig.add_trace(go.Scatter(x=df['Datetime'], y=df['Straddle'], mode='lines', name='Straddle'))


# Add trace for secondary y-axis
fig.add_trace(go.Scatter(x=df['Datetime'], y=df['Arbitrage'], mode='lines', name='Arbitrage', yaxis='y2'))

# Update layout for secondary y-axis
fig.update_layout(
    title="Arbitrage, Future_close, Synthetic, Call_Premium & Put_Premium Over Time",
    xaxis_title="Datetime",
    yaxis_title="Price",
    yaxis2=dict(
        title="Arbitrage",
        overlaying='y',
        side='right'
    ),
    legend=dict(x=0, y=1.1, orientation="h"),
    hovermode="x unified"
)

# Save as interactive HTML
fig.write_html("finaldf_interactive_plot.html")

print("Interactive plot saved as finaldf_interactive_plot.html")