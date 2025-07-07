import pandas as pd
import json
import os
import glob
from dash import Dash, dcc, html, Output, Input
import plotly.graph_objects as go
from plotly.subplots import make_subplots

DATA_DIR = '/root/aniket/Mtm/DATA FOR TOP 10 LOSSES'

def get_margin(filename):
    if '_SS_' in filename:
        return 1000000
    elif '_N_' in filename:
        return 700000
    elif '_BN_' in filename:
        return 1000000
    else:
        return 350000

def load_and_prepare_df(file_path):
    with open(file_path) as f:
        data = json.load(f)
    closed_pnl_data = data.get('closedPnl', {})
    if isinstance(closed_pnl_data, dict):
        max_len = max(len(v) for v in closed_pnl_data.values())
        closed_pnl_padded = {k: v + [None]*(max_len - len(v)) for k, v in closed_pnl_data.items()}
        closed_pnl_df = pd.DataFrame(closed_pnl_padded)
    else:
        closed_pnl_df = pd.json_normalize(closed_pnl_data)
    if 'Key' in closed_pnl_df.columns:
        closed_pnl_df['date'] = pd.to_datetime(closed_pnl_df['Key']).dt.date
    else:
        raise ValueError("No 'Key' column found in closed_pnl_df to extract date.")
    return closed_pnl_df

# List all JSON files for dropdown (recursive search)
file_paths = sorted(glob.glob(os.path.join(DATA_DIR, '**', '*.json'), recursive=True))
file_options = [
    {'label': os.path.relpath(f, DATA_DIR), 'value': f}
    for f in file_paths
]

def get_all_years():
    years = set()
    for file_path in file_paths:
        df = load_and_prepare_df(file_path)
        df['year'] = pd.to_datetime(df['date']).dt.year
        years.update(df['year'].unique())
    years = sorted([int(y) for y in years if pd.notnull(y)])
    return ['All'] + years

app = Dash(__name__)

app.layout = html.Div([
    html.H2('Superplot: Top & Bottom Daily PnL Percentage for All Files'),
    html.Div([
        html.Label('Year:'),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(y), 'value': y} for y in get_all_years()],
            value='All',
            clearable=False,
            style={'width': '150px'}
        ),
        html.Label('Top-n:'),
        dcc.Dropdown(
            id='topn-dropdown',
            options=[{'label': str(n), 'value': n} for n in range(0, 21)],
            value=5,
            clearable=False,
            style={'width': '100px'}
        ),
    ], style={'display': 'flex', 'align-items': 'center', 'gap': '20px', 'margin-bottom': '20px'}),
    dcc.Graph(id='superplot-graph', style={'height': '100vh', 'width': '100%'}),
], style={'padding': '20px', 'font-family': 'Arial'})

@app.callback(
    Output('superplot-graph', 'figure'),
    Input('year-dropdown', 'value'),
    Input('topn-dropdown', 'value')
)
def update_superplot(year, topn):
    n_files = len(file_paths)
    fig = make_subplots(
        rows=n_files, cols=1,
        subplot_titles=[
            f"{os.path.basename(file_paths[i])} - Combined PnL%" for i in range(n_files)
        ],
        vertical_spacing=0.02
    )
    for idx, file_path in enumerate(file_paths):
        filename = os.path.basename(file_path)
        margin = get_margin(filename)
        df = load_and_prepare_df(file_path)
        pos_df = get_filtered_bars(df, margin, year, topn, positive=True).sort_values('date')
        neg_df = get_filtered_bars(df, margin, year, topn, positive=False).sort_values('date')
        combined_df = pd.concat([pos_df, neg_df], ignore_index=True)
        colors = ['green' if v > 0 else 'red' for v in combined_df['Pnl_percentage']]
        fig.add_bar(
            x=combined_df['date_str'],
            y=combined_df['Pnl_percentage'],
            marker_color=colors,
            name=f"{filename} Combined",
            row=idx+1, col=1,
            customdata=combined_df[['date_str', 'Pnl_percentage']],
            hovertemplate="Date: %{customdata[0]}<br>PnL %%: %{customdata[1]:.2f}<extra></extra>"
        )
        fig.update_xaxes(title_text="Date", row=idx+1, col=1)
        fig.update_yaxes(title_text="PnL %", row=idx+1, col=1)
    fig.update_layout(
        height=350*n_files,  # Increase this multiplier for even taller bars
        width=1200,
        showlegend=False,
        title_text="Superplot: Combined Daily PnL Percentage for Each File",
        font={'family': 'Arial', 'size': 12, 'color': 'black'},
        template=None,
        transition={'duration': 500}
    )
    return fig

def get_filtered_bars(df, margin, year, topn, positive=True):
    df_daily = df.groupby('date', as_index=False)['Pnl'].sum()
    df_daily['Pnl_percentage'] = (df_daily['Pnl'] / margin) * 100
    df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year
    if year != 'All':
        df_daily = df_daily[df_daily['year'] == int(year)]
    if positive:
        df_filtered = df_daily[df_daily['Pnl_percentage'] > 0]
        if topn > 0:
            df_filtered = df_filtered.nlargest(topn, 'Pnl_percentage')
    else:
        df_filtered = df_daily[df_daily['Pnl_percentage'] < 0]
        if topn > 0:
            df_filtered = df_filtered.nsmallest(topn, 'Pnl_percentage')
    df_filtered = df_filtered.sort_values('date')
    df_filtered['date_str'] = pd.to_datetime(df_filtered['date']).dt.strftime('%d%b%y')
    return df_filtered

if __name__ == '__main__':
    app.run(debug=True, port=8051)