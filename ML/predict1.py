import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense
import plotly.graph_objects as go
from sklearn.metrics import r2_score

# --- Add RSI calculation ---
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Load 15-minute OHLCV data (e.g., NIFTY 50 or a stock like RELIANCE)
data = pd.read_csv('/root/aniket/AARTIIND.csv')  # Use raw string for Windows path

# Feature Engineering
data['Returns'] = data['c'].pct_change()
data['SMA_10'] = data['c'].rolling(10).mean()
data['RSI'] = compute_rsi(data['c'], 14)
data.dropna(inplace=True)

# Normalize features (0 to 1)
scaler = MinMaxScaler()
scaled_data = scaler.fit_transform(data[['c', 'v', 'SMA_10', 'RSI']])  # Changed 'Volume' to 'v'

def create_sequences(data, seq_length):
    X, y = [], []
    for i in range(len(data) - seq_length - 1):
        X.append(data[i:(i + seq_length)])
        y.append(data[i + seq_length, 0])  # Predict 'Close' price
    return np.array(X), np.array(y)

seq_length = 20  # Lookback window (20 steps = 5 hours)
X, y = create_sequences(scaled_data, seq_length)

# Train-test split (80-20)
split = int(0.8 * len(X))
X_train, X_test = X[:split], X[split:]
y_train, y_test = y[:split], y[split:]

model = Sequential([
    LSTM(50, return_sequences=True, input_shape=(seq_length, X.shape[2])),
    LSTM(50),
    Dense(1)
])

model.compile(optimizer='adam', loss='mse')
model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test), verbose=0)

# Predict on test set
y_pred_scaled = model.predict(X_test)

# Ensure 't' is parsed as datetime
if 't' in data.columns:
    data['t'] = pd.to_datetime(data['t'])

# Prepare date/time index for plotting
date_index = data.index[seq_length + 1:]  # +1 because of pct_change/dropna
date_times = data.iloc[date_index]['t'].values if 't' in data.columns else np.arange(len(y))
row_numbers = np.arange(len(y))  # Time step numbers for all y

# Split date_times and row_numbers for train and test
date_times_train = date_times[:split]
date_times_test = date_times[split:]
row_numbers_train = row_numbers[:split]
row_numbers_test = row_numbers[split:]

# Inverse transform to actual price
y_pred_full = np.concatenate([y_pred_scaled, np.zeros((len(y_pred_scaled), scaled_data.shape[1] - 1))], axis=1)
y_pred = scaler.inverse_transform(y_pred_full)[:, 0]

y_test_full = np.concatenate([y_test.reshape(-1, 1), np.zeros((len(y_test), scaled_data.shape[1] - 1))], axis=1)
y_test_inv = scaler.inverse_transform(y_test_full)[:, 0]

# Model score (regression)
score = r2_score(y_test_inv, y_pred)

# --- Candle direction prediction ---
# For each test sample, compare previous close (y_true_prev) with predicted close (y_pred)
# If predicted close > previous close: green, else red

# Get previous close for each test sample (from the last value in the input sequence)
prev_close_test = X_test[:, -1, 0]  # Last close in each test input sequence (scaled)
# Inverse transform prev_close_test to actual price
prev_close_full = np.concatenate([prev_close_test.reshape(-1, 1), np.zeros((len(prev_close_test), scaled_data.shape[1] - 1))], axis=1)
prev_close_actual = scaler.inverse_transform(prev_close_full)[:, 0]

# Predicted direction: 1 for green, 0 for red
predicted_direction = (y_pred > prev_close_actual).astype(int)
# True direction: 1 for green, 0 for red
true_direction = (y_test_inv > prev_close_actual).astype(int)

# Candle prediction accuracy
candle_accuracy = (predicted_direction == true_direction).mean()

# Calculate red and green candle accuracy
green_mask = true_direction == 1
red_mask = true_direction == 0

green_candle_acc = (predicted_direction[green_mask] == 1).mean() if green_mask.sum() > 0 else np.nan
red_candle_acc = (predicted_direction[red_mask] == 0).mean() if red_mask.sum() > 0 else np.nan

# Predict next value (for next 15-min candle)
last_sequence = scaled_data[-seq_length:]
last_sequence = np.reshape(last_sequence, (1, seq_length, X.shape[2]))
predicted_scaled = model.predict(last_sequence)
predicted_price = scaler.inverse_transform(
    np.concatenate([predicted_scaled, np.zeros((1, scaled_data.shape[1] - 1))], axis=1)
)[0, 0]

# Get current close (last close in last_sequence, inverse transformed)
current_close_scaled = last_sequence[0, -1, 0]
current_close_actual = scaler.inverse_transform(
    np.concatenate([[current_close_scaled], np.zeros(scaled_data.shape[1] - 1)]).reshape(1, -1)
)[0, 0]

if predicted_price > current_close_actual:
    next_candle = "GREEN"
else:
    next_candle = "RED"

print(f"Predicted Close Price (Next 15-min): {predicted_price:.2f}")
print(f"Current Close Price: {current_close_actual:.2f}")
print(f"Predicted Next Candle: {next_candle}")
print(f"Green Candle Accuracy: {green_candle_acc:.4f}")
print(f"Red Candle Accuracy: {red_candle_acc:.4f}")

# Plotly interactive graph for train and test data, with split marker and hover info
fig = go.Figure()

# Plot train actual
fig.add_trace(go.Scatter(
    x=date_times_train,
    y=y_train if 'y_train' in locals() else [],
    mode='lines',
    name='Train Actual',
    line=dict(color='green'),
    customdata=np.stack([row_numbers_train], axis=-1),
    hovertemplate="Train<br>Date: %{x|%Y-%m-%d %H:%M}<br>Row: %{customdata[0]}<br>Actual: %{y:.2f}<extra></extra>"
))

# For color coding in hover: 1=green, 0=red
def candle_color(val):
    return "green" if val == 1 else "red"

# Prepare hover text for test set
hover_text_test = [
    f"Test<br>Date: {pd.to_datetime(dt).strftime('%Y-%m-%d %H:%M')}<br>Row: {rn}<br>"
    f"Predicted: {yp:.2f} ({candle_color(pd_)})<br>"
    f"Actual: {ya:.2f} ({candle_color(td_)})"
    for dt, rn, yp, ya, pd_, td_ in zip(
        date_times_test, row_numbers_test, y_pred, y_test_inv, predicted_direction, true_direction
    )
]

# Plot test actual
fig.add_trace(go.Scatter(
    x=date_times_test,
    y=y_test_inv,
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
fig.add_shape(
    type="line",
    x0=date_times_test[0], x1=date_times_test[0],
    y0=min(np.min(y_train), np.min(y_test_inv)), y1=max(np.max(y_train), np.max(y_test_inv)),
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