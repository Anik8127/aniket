import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Model # Changed from keras.models
from tensorflow.keras.layers import LSTM, Dense, Input # Changed from keras.layers
import plotly.graph_objects as go
from sklearn.metrics import r2_score
import sys
import subprocess

# Ensure TensorFlow is installed
try:
    import tensorflow as tf
except ImportError:
    print("TensorFlow not found. Installing...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tensorflow"])
    import tensorflow as tf

# --- RSI calculation ---
def compute_rsi(series, period=14):
    """
    Computes the Relative Strength Index (RSI) for a given series.

    Args:
        series (pd.Series): The input series (e.g., 'Close' prices).
        period (int): The period over which to calculate RSI (default is 14).

    Returns:
        pd.Series: The RSI values.
    """
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Load data from the uploaded CSV file
data = pd.read_csv('/root/aniket/AARTIIND.csv')
data['datetime'] = pd.to_datetime(data['datetime'])

# Feature Engineering
# Calculate daily returns
data['Returns'] = data['c'].pct_change()
# Calculate 10-period Simple Moving Average (SMA)
data['SMA_10'] = data['c'].rolling(10).mean()
# Calculate 14-period Relative Strength Index (RSI)
data['RSI'] = compute_rsi(data['c'], 14)
# Drop rows with NaN values resulting from feature engineering
data.dropna(inplace=True)

# Normalize features (scale data to 0-1 range)
# 'c' (Close), 'v' (Volume), 'SMA_10', 'RSI' are used as features
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data[['c', 'v', 'SMA_10', 'RSI']])

def create_sequences(data, seq_length):
    """
    Creates sequences (lookback windows) for LSTM input and corresponding target values.

    Args:
        data (np.array): The scaled feature data.
        seq_length (int): The length of the lookback window.

    Returns:
        tuple: A tuple containing:
            - X (np.array): Input sequences for the LSTM model.
            - y (np.array): Target values (Close price) for each sequence.
    """
    X, y = [], []
    for i in range(len(data) - seq_length - 1):
        X.append(data[i:(i + seq_length)])
        y.append(data[i + seq_length, 0])  # Predict 'Close' price (index 0 of scaled_data)
    return np.array(X), np.array(y)

seq_length = 20  # Lookback window size (number of past time steps to consider)
X, y = create_sequences(scaled_data, seq_length)

# Train-test split (80% train, 20% test)
split = int(0.8 * len(X))
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

# --- Build and train LSTM model using Functional API to expose states ---
# Define input layer
inputs = Input(shape=(seq_length, X.shape[2]))

# First LSTM layer: 50 units, returns sequences for the next LSTM layer
lstm1_output = LSTM(50, return_sequences=True)(inputs)

# Second LSTM layer: 50 units. return_state=True to get hidden and cell states
# lstm2_output is the hidden state sequence if return_sequences=True, or final hidden state if False
# h_state is the final hidden state, c_state is the final cell state
lstm2_output, h_state, c_state = LSTM(50, return_state=True)(lstm1_output)

# Output Dense layer: 1 unit for predicting the single 'Close' price
# The Dense layer takes the output (hidden state) of the second LSTM layer
predictions = Dense(1)(lstm2_output)

# Create the model for training (outputs only predictions)
model = Model(inputs=inputs, outputs=predictions)
# Compile the model using Adam optimizer and Mean Squared Error (MSE) loss
model.compile(optimizer='adam', loss='mse')
# Train the model
model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test), verbose=0)

# Create a separate model for prediction that also outputs hidden and cell states
prediction_model = Model(inputs=inputs, outputs=[predictions, h_state, c_state])

# Predict on the test set
y_pred_scaled, hidden_states, cell_states = prediction_model.predict(X_test)

# Prepare date/time index for plotting
# Align date_times with the 'y' (target) values after sequence creation and dropping NaNs
date_times = data['datetime'].values[seq_length + 1 : len(y) + seq_length + 1]
row_numbers = np.arange(len(y))

# Split date_times and row_numbers for train and test sets
date_times_train = date_times[:split]
date_times_test = date_times[split:]
row_numbers_train = row_numbers[:split]
row_numbers_test = row_numbers[split:]

# Inverse transform predictions and targets to actual close prices
# To inverse transform, we need to create an array with the same number of features as the scaler was fitted on.
# We fill the other feature columns with zeros as we are only interested in the 'Close' price (index 0).
y_pred_full = np.concatenate([y_pred_scaled, np.zeros((len(y_pred_scaled), scaled_data.shape[1] - 1))], axis=1)
y_pred = scaler.inverse_transform(y_pred_full)[:, 0] # Get the inverse transformed 'Close' price

y_test_full = np.concatenate([y_test.reshape(-1, 1), np.zeros((len(y_test), scaled_data.shape[1] - 1))], axis=1)
y_test_inv = scaler.inverse_transform(y_test_full)[:, 0] # Get the inverse transformed actual 'Close' price

# Use actual close prices from the original dataset for plotting, aligned with the test set
actual_close_for_test = data['c'].values[seq_length + 1 + split : seq_length + 1 + split + len(y_test)]
actual_close_for_train = data['c'].values[seq_length + 1 : seq_length + 1 + split]

# Model score (R-squared for regression)
score = r2_score(y_test_inv, y_pred)

# --- Candle direction prediction ---
# Get the actual close price from the previous time step in the test set.
# This acts as the "open" price for the candle we are predicting the close for.
prev_close_test = X_test[:, -1, 0]  # Last close in each test input sequence (scaled)
prev_close_full = np.concatenate([prev_close_test.reshape(-1, 1), np.zeros((len(prev_close_test), scaled_data.shape[1] - 1))], axis=1)
prev_close_actual = scaler.inverse_transform(prev_close_full)[:, 0]

# Determine predicted candle direction: 1 for green (predicted close > previous actual close), 0 for red
predicted_direction = (y_pred > prev_close_actual).astype(int)
# Determine true candle direction: 1 for green (actual close > previous actual close), 0 for red
true_direction = (y_test_inv > prev_close_actual).astype(int)

# Calculate candle accuracy metrics
candle_accuracy = (predicted_direction == true_direction).mean()
green_mask = true_direction == 1 # Mask for actual green candles
red_mask = true_direction == 0  # Mask for actual red candles
green_candle_acc = (predicted_direction[green_mask] == 1).mean() if green_mask.sum() > 0 else np.nan
red_candle_acc = (predicted_direction[red_mask] == 0).mean() if red_mask.sum() > 0 else np.nan

# Predict next value (for the next 15-min candle after the test set)
# Take the last 'seq_length' data points from the scaled data
last_sequence = scaled_data[-seq_length:]
# Reshape for LSTM input (1 sample, seq_length time steps, X.shape[2] features)
last_sequence = np.reshape(last_sequence, (1, seq_length, X.shape[2]))
# Predict the next scaled close price and states using the prediction_model
predicted_scaled_next, _, _ = prediction_model.predict(last_sequence) # We only need the prediction here
# Inverse transform the predicted scaled price to get the actual price
predicted_price = scaler.inverse_transform(
    np.concatenate([predicted_scaled_next, np.zeros((1, scaled_data.shape[1] - 1))], axis=1)
)[0, 0]

# Get the current (last) actual close price from the input sequence
current_close_scaled = last_sequence[0, -1, 0]
current_close_actual = scaler.inverse_transform(
    np.concatenate([[current_close_scaled], np.zeros(scaled_data.shape[1] - 1)]).reshape(1, -1)
)[0, 0]

# Determine the predicted direction of the very next candle
next_candle = "GREEN" if predicted_price > current_close_actual else "RED"

print(f"Predicted Close Price (Next 15-min): {predicted_price:.2f}")
print(f"Current Close Price: {current_close_actual:.2f}")
print(f"Predicted Next Candle: {next_candle}")
print(f"Green Candle Accuracy: {green_candle_acc:.4f}")
print(f"Red Candle Accuracy: {red_candle_acc:.4f}")

print("\n--- LSTM Internal States (Average Values for Test Set) ---")
# Print average values of hidden and cell states for the test set
# Note: These are scaled internal representations, not directly interpretable as prices.
# Direct "Forget Gate", "Input Gate", "Candidate Values", "Output Gate" values are
# internal to the LSTM cell's operations and not easily exposed as layer outputs
# in standard Keras models without custom layer implementations.
print(f"Average Hidden State (Test Set): {np.mean(hidden_states):.4f}")
print(f"Average Cell State (Test Set): {np.mean(cell_states):.4f}")


# --- Plotly interactive graph for train and test data, with split marker and hover info ---
fig = go.Figure()

# Prepare hover text for train set
hover_text_train = [
    f"Train<br>Date: {pd.to_datetime(dt).strftime('%Y-%m-%d %H:%M')}<br>Row: {rn}<br>Actual: {ya:.2f}"
    for dt, rn, ya in zip(date_times_train, row_numbers_train, actual_close_for_train)
]

# Plot train actual (true close prices)
fig.add_trace(go.Scatter(
    x=date_times_train,
    y=actual_close_for_train,
    mode='lines',
    name='Train Actual',
    line=dict(color='green'),
    customdata=np.stack([row_numbers_train], axis=-1),
    hovertext=hover_text_train,
    hoverinfo="text"
))

def candle_color_str(val):
    """Helper function to return 'Green' or 'Red' string based on direction (1 or 0)."""
    return "Green" if val == 1 else "Red"

# Prepare hover text for test set - now includes predicted candle color and LSTM states
hover_text_test = []
for i in range(len(date_times_test)):
    dt = date_times_test[i]
    rn = row_numbers_test[i]
    yp = y_pred[i]
    ya = actual_close_for_test[i]
    pd_ = predicted_direction[i]
    td_ = true_direction[i]
    # Calculate average of the 50 units for hidden and cell states for hover display
    avg_h_state = np.mean(hidden_states[i])
    avg_c_state = np.mean(cell_states[i])

    hover_text_test.append(
        f"Test<br>Date: {pd.to_datetime(dt).strftime('%Y-%m-%d %H:%M')}<br>Row: {rn}<br>"
        f"Predicted: {yp:.2f} (Predicted Candle: {candle_color_str(pd_)})<br>"
        f"Actual: {ya:.2f} (Actual Candle: {candle_color_str(td_)})<br>"
        f"Avg Hidden State: {avg_h_state:.4f}<br>"
        f"Avg Cell State: {avg_c_state:.4f}"
    )

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

# Filter predicted data based on predicted candle direction
green_predicted_mask = (predicted_direction == 1)
red_predicted_mask = (predicted_direction == 0)

# Plot test predicted (Green Candles)
# These points represent predictions where the model expects a green candle
fig.add_trace(go.Scatter(
    x=date_times_test[green_predicted_mask],
    y=y_pred[green_predicted_mask],
    mode='markers', # Use markers to clearly show individual predicted points
    name='Predicted Green Candle',
    marker=dict(color='lime', size=6, symbol='circle', line=dict(width=1, color='DarkSlateGrey')), # Bright green markers
    customdata=np.stack([row_numbers_test[green_predicted_mask]], axis=-1),
    hovertext=np.array(hover_text_test)[green_predicted_mask],
    hoverinfo="text"
))

# Plot test predicted (Red Candles)
# These points represent predictions where the model expects a red candle
fig.add_trace(go.Scatter(
    x=date_times_test[red_predicted_mask],
    y=y_pred[red_predicted_mask],
    mode='markers', # Use markers
    name='Predicted Red Candle',
    marker=dict(color='red', size=6, symbol='circle', line=dict(width=1, color='DarkSlateGrey')), # Red markers
    customdata=np.stack([row_numbers_test[red_predicted_mask]], axis=-1),
    hovertext=np.array(hover_text_test)[red_predicted_mask],
    hoverinfo="text"
))

# Add vertical dotted line to show train/test split
fig.add_shape(
    type="line",
    x0=date_times_test[0], x1=date_times_test[0],
    y0=min(np.min(actual_close_for_train), np.min(actual_close_for_test)),
    y1=max(np.max(actual_close_for_train), np.max(actual_close_for_test)),
    line=dict(color="red", width=2, dash="dot"),
    name="Train/Test Split"
)

fig.update_layout(
    title=f"Actual vs Predicted Close Price (R² Score: {score:.4f}, Green Acc: {green_candle_acc:.4f}, Red Acc: {red_candle_acc:.4f})",
    xaxis_title="Date/Time",
    yaxis_title="Close Price",
    legend=dict(x=0, y=1),
    template="plotly_dark"
)
fig.show()

# --- Export test results to CSV ---
export_df = pd.DataFrame({
    'datetime': date_times_test,
    'actual_close': actual_close_for_test,
    'predicted_close': y_pred,
    'predicted_candle_direction': [candle_color_str(d) for d in predicted_direction]
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