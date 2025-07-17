import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
import plotly.graph_objects as go
from sklearn.metrics import r2_score

# --- RSI calculation ---
def compute_rsi(series, period=14):
    """
    Computes the Relative Strength Index (RSI) for a given price series.

    Args:
        series (pd.Series): The price series (e.g., 'c' for close prices).
        period (int): The number of periods to use for RSI calculation.

    Returns:
        pd.Series: A series containing the RSI values.
    """
    delta = series.diff()
    # Calculate gains (positive changes) and losses (negative changes)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

    # Avoid division by zero for RS
    # If loss is zero, rs becomes infinity, which makes rsi 100
    # If gain is zero and loss is non-zero, rs is 0, which makes rsi 0
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Load data
# The file path is corrected to directly use the uploaded filename.
data = pd.read_csv('/root/aniket/aarti_test_1.csv')
data['datetime'] = pd.to_datetime(data['datetime'])

# Feature Engineering
# Calculate daily returns
data['Returns'] = data['c'].pct_change()
# Calculate 10-period Simple Moving Average (SMA)
data['SMA_10'] = data['c'].rolling(10).mean()
# Calculate 14-period Relative Strength Index (RSI)
data['RSI'] = compute_rsi(data['c'], 14)
# Drop any rows with NaN values that result from feature engineering
data.dropna(inplace=True)

# Normalize features (scale values to be between 0 and 1)
# This is crucial for neural networks like LSTMs
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data[['c', 'v', 'SMA_10', 'RSI']])

def create_sequences(data, seq_length):
    """
    Creates sequences of data for LSTM input and corresponding target values.

    Args:
        data (np.array): The scaled input data (features).
        seq_length (int): The number of previous time steps to consider for each sequence.

    Returns:
        tuple: A tuple containing two numpy arrays (X, y):
               X: Input sequences (samples, time steps, features)
               y: Target values (samples, 1)
    """
    X, y = [], []
    # Loop through the data to create sequences
    # The loop runs until there's enough data for a sequence and a target value
    for i in range(len(data) - seq_length - 1):
        X.append(data[i:(i + seq_length)])
        y.append(data[i + seq_length, 0])  # Predict 'Close' price (index 0 of scaled_data)
    return np.array(X), np.array(y)

seq_length = 20  # Define the lookback window (number of previous time steps)
X, y = create_sequences(scaled_data, seq_length)

# --- Debugging: Check if X is empty due to insufficient data ---
# This check prevents IndexError if the dataset is too small
if X.shape[0] == 0:
    print(f"Error: Not enough data to create sequences with seq_length={seq_length}.")
    print(f"Original data length: {len(data)}")
    print(f"Data length after dropping NaNs: {len(data.dropna())}")
    print(f"Required minimum data length for seq_length={seq_length}: {seq_length + 2} rows.")
    print("Please provide a CSV file with more data or reduce the 'seq_length'.")
    exit() # Exit the script if no sequences can be created

# Reshape X if it's 2D (e.g., if only one feature was used) to make it 3D for LSTM
# LSTM expects input in the shape (samples, time steps, features)
if X.ndim == 2:
    X = X.reshape((X.shape[0], X.shape[1], 1))

# Train-test split (80% training, 20% testing)
split = int(0.8 * len(X))
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

# Build the LSTM model
model = Sequential([
    # First LSTM layer: 50 units, returns sequences for the next LSTM layer
    LSTM(50, return_sequences=True, input_shape=(seq_length, X.shape[2])),
    # Second LSTM layer: 50 units, does not return sequences (feeds into Dense layer)
    LSTM(50),
    # Output Dense layer: 1 unit for predicting the single 'Close' price
    Dense(1)
])
# Compile the model with Adam optimizer and Mean Squared Error (MSE) loss
model.compile(optimizer='adam', loss='mse')
# Train the model
# verbose=0 means no training output, set to 1 or 2 for progress
model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test), verbose=0)

# Predict on the test set using the trained model
y_pred_scaled = model.predict(X_test)

# Prepare date/time index for plotting
# Align the date_times with the 'y' (target) values
date_times = data['datetime'].values[seq_length + 1 : len(y) + seq_length + 1]
row_numbers = np.arange(len(y)) # Row numbers for hover info
date_times_train = date_times[:split]
date_times_test = date_times[split:]
row_numbers_train = row_numbers[:split]
row_numbers_test = row_numbers[split:]

# Inverse transform predictions and actual test targets to their original scale
# This is necessary because the model was trained on scaled data
y_pred_full = np.concatenate([y_pred_scaled, np.zeros((len(y_pred_scaled), scaled_data.shape[1] - 1))], axis=1)
y_pred = scaler.inverse_transform(y_pred_full)[:, 0]

y_test_full = np.concatenate([y_test.reshape(-1, 1), np.zeros((len(y_test), scaled_data.shape[1] - 1))], axis=1)
y_test_inv = scaler.inverse_transform(y_test_full)[:, 0]

# Use actual close prices from the original dataset for plotting
# This ensures alignment with the test set's time frame
actual_close_for_test = data['c'].values[seq_length + 1 + split : seq_length + 1 + split + len(y_test)]
actual_close_for_train = data['c'].values[seq_length + 1 : seq_length + 1 + split]

# Model score (R-squared for regression)
score = r2_score(y_test_inv, y_pred)

# --- Candle direction prediction ---
# Get the previous close price for each test sequence to determine direction
prev_close_test = X_test[:, -1, 0]  # Last close in each test input sequence (scaled)
prev_close_full = np.concatenate([prev_close_test.reshape(-1, 1), np.zeros((len(prev_close_test), scaled_data.shape[1] - 1))], axis=1)
prev_close_actual = scaler.inverse_transform(prev_close_full)[:, 0]

# Determine predicted and true candle directions (1 for green/up, 0 for red/down)
predicted_direction = (y_pred > prev_close_actual).astype(int)
true_direction = (y_test_inv > prev_close_actual).astype(int)

# Calculate overall candle accuracy
candle_accuracy = (predicted_direction == true_direction).mean()

# Calculate accuracy for green and red candles separately
green_mask = true_direction == 1
red_mask = true_direction == 0
green_candle_acc = (predicted_direction[green_mask] == 1).mean() if green_mask.sum() > 0 else np.nan
red_candle_acc = (predicted_direction[red_mask] == 0).mean() if red_mask.sum() > 0 else np.nan

# Predict next value (for next 15-min candle)
# Take the last 'seq_length' data points to predict the very next value
last_sequence = scaled_data[-seq_length:]
last_sequence = np.reshape(last_sequence, (1, seq_length, X.shape[2]))
predicted_scaled = model.predict(last_sequence)
predicted_price = scaler.inverse_transform(
    np.concatenate([predicted_scaled, np.zeros((1, scaled_data.shape[1] - 1))], axis=1)
)[0, 0]

# Get the current close price from the last data point
current_close_scaled = last_sequence[0, -1, 0]
current_close_actual = scaler.inverse_transform(
    np.concatenate([[current_close_scaled], np.zeros(scaled_data.shape[1] - 1)]).reshape(1, -1)
)[0, 0]

# Determine the predicted direction of the next candle
next_candle = "GREEN" if predicted_price > current_close_actual else "RED"

# Get the datetime for the current close and estimate the predicted next close datetime
current_close_datetime = data['datetime'].values[-1]
# Use the median difference between datetimes to estimate the next timestamp
predicted_close_datetime = pd.to_datetime(current_close_datetime) + (data['datetime'].diff().median())


# Print the prediction results and accuracy scores
print(f"Predicted Close Price (Next 15-min): {predicted_price:.2f} as on {predicted_close_datetime}")
print(f"Current Close Price: {current_close_actual:.2f} as on {current_close_datetime}")
print(f"Predicted Next Candle: {next_candle}")
print(f"Green Candle Accuracy: {green_candle_acc:.4f}")
print(f"Red Candle Accuracy: {red_candle_acc:.4f}")
print(f"R-squared (R²) score: {score:.4f}")

# --- Plotly interactive graph for test data only, with split marker and hover info ---
fig = go.Figure()

def candle_color(val):
    """Helper function to return 'green' or 'red' based on candle direction."""
    return "green" if val == 1 else "red"

# Prepare hover text for test set
hover_text_test = [
    f"Test<br>Date: {pd.to_datetime(dt).strftime('%Y-%m-%d %H:%M')}<br>Row: {rn}<br>"
    f"Predicted: {yp:.2f} ({candle_color(pd_)})<br>"
    f"Actual: {ya:.2f} ({candle_color(td_)})"
    for dt, rn, yp, ya, pd_, td_ in zip(
        date_times_test, row_numbers_test, y_pred, actual_close_for_test, predicted_direction, true_direction
    )
]

# Plot test actual (true close prices)
fig.add_trace(go.Scatter(
    x=date_times_test,
    y=actual_close_for_test,
    mode='lines',
    name='Test Actual',
    line=dict(color='blue'),
    customdata=np.stack([row_numbers_test], axis=-1),
    hovertext=hover_text_test,
    hoverinfo="text"
))

# Plot test predicted
fig.add_trace(go.Scatter(
    x=date_times_test,
    y=y_pred,
    mode='lines+markers',
    name='Test Predicted',
    line=dict(dash='dot', color='orange'),
    customdata=np.stack([row_numbers_test], axis=-1),
    hovertext=hover_text_test,
    hoverinfo="text"
))

# Add vertical dotted line to show train/test split
# Adjusted y-axis limits to focus on the test data range
fig.add_shape(
    type="line",
    x0=date_times_test[0], x1=date_times_test[0],
    y0=np.min(actual_close_for_test) * 0.95, # Adjust min for better visual range
    y1=np.max(actual_close_for_test) * 1.05, # Adjust max for better visual range
    line=dict(color="red", width=2, dash="dot"),
    name="Train/Test Split"
)

# Update layout for title, axis labels, legend, and theme
fig.update_layout(
    title=f"Actual vs Predicted Close Price (R² Score: {score:.4f}, Green Acc: {green_candle_acc:.4f}, Red Acc: {red_candle_acc:.4f})",
    xaxis_title="Date/Time",
    yaxis_title="Close Price",
    legend=dict(x=0, y=1),
    template="plotly_dark"
)
# Display the plot
fig.show()

# --- Export test results to CSV ---
export_df = pd.DataFrame({
    'datetime': date_times_test,
    'actual_close': actual_close_for_test,
    'predicted_close': y_pred
})
export_df.to_csv('lstm_test_predictions.csv', index=False)
print("Test predictions exported to lstm_test_predictions.csv")

print("\n--- Data Split Information ---")
print(f"Train Start Date: {pd.to_datetime(date_times_train[0])}")
print(f"Train End Date:   {pd.to_datetime(date_times_train[-1])}")
print(f"Train Rows:       {len(date_times_train)}")

print(f"Test Start Date:  {pd.to_datetime(date_times_test[0])}")
print(f"Test End Date:    {pd.to_datetime(date_times_test[-1])}")
print(f"Test Rows:        {len(date_times_test)}")
print(f"Total Rows (Train + Test): {len(date_times_train) + len(date_times_test)}")
