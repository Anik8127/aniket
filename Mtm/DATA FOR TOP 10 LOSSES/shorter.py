import pandas as pd
import json
import sys
import plotly.graph_objects as go

# ---- CONFIG ----
json_file = '/root/aniket/Mtm/DATA FOR TOP 10 LOSSES/DATA FOR TOP 10 LOSSES/mtm_MOZ_v2_N_PT.json'  # <-- Change this to your JSON file path
MARGIN = 700000

# ---- LOAD DATA ----
with open(json_file) as f:
    data = json.load(f)
closed_pnl_data = data.get('closedPnl', {})
if isinstance(closed_pnl_data, dict):
    max_len = max(len(v) for v in closed_pnl_data.values())
    closed_pnl_padded = {k: v + [None]*(max_len - len(v)) for k, v in closed_pnl_data.items()}
    df = pd.DataFrame(closed_pnl_padded)
else:
    df = pd.json_normalize(closed_pnl_data)

if 'Key' in df.columns:
    df['date'] = pd.to_datetime(df['Key'])
else:
    raise ValueError("No 'Key' column found in closed_pnl_df to extract date.")

# ---- FILTER THURSDAYS ----
df['weekday'] = df['date'].dt.day_name()
thursday_df = df[df['weekday'] == 'Thursday'].copy()

# ---- GROUP BY DATE ONLY AND SUM PnL ----
thursday_df['date_only'] = thursday_df['date'].dt.date
thursday_grouped = thursday_df.groupby('date_only', as_index=False).agg({'Pnl': 'sum'})

# ---- CALCULATE PnL_percentage ----
thursday_grouped['PnL_percentage'] = (thursday_grouped['Pnl'] / MARGIN) * 100

# ---- SAVE THURSDAY ROWS TO CSV ----
thursday_grouped.to_csv('thursday_rows.csv', index=False)

# ---- CALCULATE PROBABILITIES ----
total = len(thursday_grouped)
if total == 0:
    pos_prob = 0
    neg_prob = 0
else:
    pos_prob = (thursday_grouped['PnL_percentage'] > 0).sum() / total
    neg_prob = (thursday_grouped['PnL_percentage'] <= 0).sum() / total

# ---- PLOT PIE CHART ----
fig = go.Figure(
    data=[
        go.Pie(
            labels=['Positive Return', 'Negative Return'],
            values=[pos_prob, neg_prob],
            marker=dict(colors=['green', 'red']),
            textinfo='label+percent',
            hole=0.4
        )
    ]
)
fig.update_layout(
    title={
        'text': f'Probability of Positive/Negative Return: {json_file}',
        'font': {'size': 18, 'family': 'Arial', 'color': 'black'}
    },
    font={'family': 'Arial', 'size': 14, 'color': 'black'},
    template='plotly_white'
)
fig.show()