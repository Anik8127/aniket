import pandas as pd

# Load the JSON file
df = pd.read_json('/root/aniket/Research/client_margin/HistoricalPnl.ClientSnapshot.json')

# Remove '_id' column if it exists
if '_id' in df.columns:
    df = df.drop(columns=['_id'])

# Expand the 'margin' column into separate columns if it exists
if 'margin' in df.columns:
    margin_expanded = pd.json_normalize(df['margin'])
    df = df.drop(columns=['margin'])
    df = pd.concat([df, margin_expanded], axis=1)

# Rename 'timestamp' to 'datetime'
if 'timestamp' in df.columns:
    df = df.rename(columns={'timestamp': 'datetime'})

# Convert 'datetime' from UTC to IST
df['datetime'] = pd.to_datetime(df['datetime'], utc=True).dt.tz_convert('Asia/Kolkata')

# Set 'datetime' as the index
df = df.set_index('datetime')

# Save to CSV
df.to_csv('HistoricalPnl.ClientSnapshot.csv')