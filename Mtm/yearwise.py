import pandas as pd
import json
import plotly.graph_objects as go

# Load the JSON file containing both closedPnl and mtm
with open('/root/aniket/Mtm/DATA FOR TOP 10 LOSSES/DATA FOR TOP 10 LOSSES/mtm_DNS3_N25.json') as f:
    data = json.load(f)

# Extract closedPnl dictionary
closed_pnl_data = data.get('closedPnl', {})

# Convert closedPnl to DataFrame
if isinstance(closed_pnl_data, dict):
    max_len = max(len(v) for v in closed_pnl_data.values())
    closed_pnl_padded = {k: v + [None]*(max_len - len(v)) for k, v in closed_pnl_data.items()}
    closed_pnl_df = pd.DataFrame(closed_pnl_padded)
else:
    closed_pnl_df = pd.json_normalize(closed_pnl_data)

closed_pnl_df.to_csv('closed_pnl_output.csv', index=False)

# --- Create df_daily ---
if 'Key' in closed_pnl_df.columns:
    closed_pnl_df['date'] = pd.to_datetime(closed_pnl_df['Key']).dt.date
else:
    raise ValueError("No 'Key' column found in closed_pnl_df to extract date.")

df_daily = closed_pnl_df.groupby('date', as_index=False)['Pnl'].sum()
df_daily['Pnl_percentage'] = (df_daily['Pnl'] / 350000) * 100
df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year

# Dropdown options
years = sorted(df_daily['year'].unique())
top_n_options = [5, 10, 15, 20]
bottom_n_options = [5, 10, 15, 20]

# Prepare all traces for all (year, top_n, bottom_n) combinations
traces = []
visibility_matrix = []
trace_labels = []

for y_idx, year in enumerate(years):
    year_df = df_daily[df_daily['year'] == year]
    for t_idx, top_n in enumerate(top_n_options):
        for b_idx, bottom_n in enumerate(bottom_n_options):
            top_df = year_df.nlargest(top_n, 'Pnl_percentage')
            bottom_df = year_df.nsmallest(bottom_n, 'Pnl_percentage')
            plot_df = pd.concat([top_df, bottom_df]).drop_duplicates()
            plot_df['color'] = plot_df['Pnl_percentage'].apply(lambda x: 'green' if x > 0 else 'red')
            green_df = plot_df[plot_df['color'] == 'green'].sort_values('date')
            red_df = plot_df[plot_df['color'] == 'red'].sort_values('date')
            final_df = pd.concat([green_df, red_df])
            final_df['date_str'] = pd.to_datetime(final_df['date']).dt.strftime('%d%b%y')

            trace = go.Bar(
                x=final_df['date_str'],
                y=final_df['Pnl_percentage'],
                marker_color=final_df['color'],
                hovertemplate='Date: %{x}<br>Pnl Percentage: %{y:.2f}%<br>Pnl: %{customdata}',
                customdata=final_df['Pnl'],
                visible=False
            )
            traces.append(trace)
            visibility_matrix.append((y_idx, t_idx, b_idx))
            trace_labels.append((year, top_n, bottom_n))

# Set the default visible trace (first year, top_n=10, bottom_n=10)
default_y_idx = 0
default_t_idx = top_n_options.index(10)
default_b_idx = bottom_n_options.index(10)
default_trace_idx = visibility_matrix.index((default_y_idx, default_t_idx, default_b_idx))
for i, trace in enumerate(traces):
    trace.visible = (i == default_trace_idx)

# Dropdowns for year, top_n, bottom_n
def make_visibility_array(selected_y_idx, selected_t_idx, selected_b_idx):
    return [
        (y == selected_y_idx and t == selected_t_idx and b == selected_b_idx)
        for (y, t, b) in visibility_matrix
    ]

# Top n dropdown
topn_buttons = []
for t_idx, top_n in enumerate(top_n_options):
    vis = make_visibility_array(default_y_idx, t_idx, default_b_idx)
    topn_buttons.append(dict(
        label=str(top_n),
        method='update',
        args=[
            {'visible': vis},
            {}  # No title update
        ]
    ))

# Bottom n dropdown
bottomn_buttons = []
for b_idx, bottom_n in enumerate(bottom_n_options):
    vis = make_visibility_array(default_y_idx, default_t_idx, b_idx)
    bottomn_buttons.append(dict(
        label=str(bottom_n),
        method='update',
        args=[
            {'visible': vis},
            {}  # No title update
        ]
    ))

# Year dropdown
year_buttons = []
for y_idx, year in enumerate(years):
    vis = make_visibility_array(y_idx, default_t_idx, default_b_idx)
    year_buttons.append(dict(
        label=str(year),
        method='update',
        args=[
            {'visible': vis},
            {}  # No title update
        ]
    ))

fig = go.Figure(data=traces)

fig.update_layout(
    updatemenus=[
        dict(
            buttons=year_buttons,
            direction='down',
            showactive=True,
            x=1.0, y=1.15, xanchor='right', yanchor='top',
            pad={'r': 10, 't': 10},
            active=default_y_idx,
            name='Year'
        ),
        dict(
            buttons=topn_buttons,
            direction='down',
            showactive=True,
            x=0.85, y=1.15, xanchor='right', yanchor='top',
            pad={'r': 10, 't': 10},
            active=default_t_idx,
            name='Top N'
        ),
        dict(
            buttons=bottomn_buttons,
            direction='down',
            showactive=True,
            x=0.7, y=1.15, xanchor='right', yanchor='top',
            pad={'r': 10, 't': 10},
            active=default_b_idx,
            name='Bottom N'
        ),
    ],
    title={
        'text': f'Top & Bottom Daily Pnl Percentage DNS3_N25',
        'font': {'size': 20, 'family': 'Arial', 'color': 'white'}
    },
    xaxis_title={
        'text': '<b>Date</b>',
        'font': {'size': 16, 'family': 'Arial', 'color': 'white'}
    },
    yaxis_title={
        'text': '<b>Pnl Percentage</b>',
        'font': {'size': 16, 'family': 'Arial', 'color': 'white'}
    },
    xaxis_tickangle=-45,
    bargap=0.2,
    template='plotly_dark',
    xaxis_type='category',
    font={'family': 'Arial', 'size': 14, 'color': 'white'},
    xaxis={'tickfont': {'family': 'Arial', 'size': 14, 'color': 'white'}},
    yaxis={'tickfont': {'family': 'Arial', 'size': 14, 'color': 'white'}}
)

fig.write_html('top_bottom_n_daily_pnl_percentage_grouped.html')
fig.show()

