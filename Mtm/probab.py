import pandas as pd
import json
import os
import glob
from dash import Dash, dcc, html, Output, Input, State
import plotly.graph_objects as go
from dash import dash_table

# Directory containing JSON files
DATA_DIR = '/root/aniket/Mtm/DATA FOR TOP 10 LOSSES'

# List all JSON files for dropdown (recursive search)
file_options = [
    {'label': os.path.relpath(f, DATA_DIR), 'value': f}
    for f in glob.glob(os.path.join(DATA_DIR, '**', '*.json'), recursive=True)
]

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

app = Dash(__name__)

app.layout = html.Div([
    html.H2('Probability of Positive/Negative Return', style={'color': 'white', 'font-family': 'Arial'}),
    html.Div([
        html.Label('Filename:', style={'color': 'white'}),
        dcc.Dropdown(
            id='file-dropdown',
            options=file_options,
            value=None,
            clearable=False,
            style={'width': '400px', 'color': 'black'}
        ),
        dcc.Checklist(
            id='show-table-check',
            options=[{'label': 'Show Table', 'value': 'show'}],
            value=[],
            style={'color': 'white', 'marginTop': '10px'}
        ),
    ], style={'background': '#222', 'padding': '10px'}),
    dcc.Graph(id='probability-pie', style={'height': '500px', 'background': '#222'}),
    html.Div(
        dash_table.DataTable(
            id='filtered-table',
            style_table={'overflowX': 'auto', 'background': '#222'},
            style_header={'backgroundColor': '#444', 'color': 'white'},
            style_cell={'backgroundColor': '#222', 'color': 'white', 'font-family': 'Arial'},
        ),
        id='table-container',
        style={'display': 'none'}
    ),
], style={'background': '#222', 'padding': '20px', 'font-family': 'Arial'})

@app.callback(
    Output('probability-pie', 'figure'),
    Output('filtered-table', 'data'),
    Output('filtered-table', 'columns'),
    Output('table-container', 'style'),
    Input('file-dropdown', 'value'),
    Input('show-table-check', 'value')
)
def update_probability_pie(file_path, show_table_value):
    if not file_path:
        return go.Figure(), [], [], {'display': 'none'}
    filename = os.path.basename(file_path)
    closed_pnl_df = load_and_prepare_df(file_path)
    closed_pnl_df['date'] = pd.to_datetime(closed_pnl_df['date'])
    closed_pnl_df['weekday'] = closed_pnl_df['date'].dt.day_name()
    closed_pnl_df['date_only'] = closed_pnl_df['date'].dt.date

    # Filtering logic based on filename
    if '_SS_' in filename:
        # Sensex: Fridays before 2025-01-01, Tuesdays from 2025-01-01 onwards
        cutoff = pd.to_datetime('2025-01-01')
        before_cutoff = closed_pnl_df[closed_pnl_df['date'] < cutoff]
        after_cutoff = closed_pnl_df[closed_pnl_df['date'] >= cutoff]
        friday_df = before_cutoff[before_cutoff['weekday'] == 'Friday']
        tuesday_df = after_cutoff[after_cutoff['weekday'] == 'Tuesday']
        filtered_df = pd.concat([friday_df, tuesday_df])
    elif '_N_' in filename:
        # Nifty: All Thursdays
        filtered_df = closed_pnl_df[closed_pnl_df['weekday'] == 'Thursday']
    elif '_BN_' in filename:
        # BankNifty: Thursdays before 2023-09-04, Wednesdays from 2023-09-04 to 2024-11-20
        cutoff1 = pd.to_datetime('2023-09-04')
        cutoff2 = pd.to_datetime('2024-11-20')
        before_cutoff1 = closed_pnl_df[closed_pnl_df['date'] < cutoff1]
        between_cutoffs = closed_pnl_df[(closed_pnl_df['date'] >= cutoff1) & (closed_pnl_df['date'] <= cutoff2)]
        thursday_df = before_cutoff1[before_cutoff1['weekday'] == 'Thursday']
        wednesday_df = between_cutoffs[between_cutoffs['weekday'] == 'Wednesday']
        filtered_df = pd.concat([thursday_df, wednesday_df])
    else:
        # Default: All Thursdays
        filtered_df = closed_pnl_df[closed_pnl_df['weekday'] == 'Thursday']

    # Group by date and sum PnL
    grouped = filtered_df.groupby('date_only', as_index=False).agg({'Pnl': 'sum'})

    # Add Weekday column for the grouped dates
    # Map date_only to weekday using the original filtered_df
    weekday_map = filtered_df.drop_duplicates('date_only').set_index('date_only')['weekday']
    grouped['Weekday'] = grouped['date_only'].map(weekday_map)

    # Calculate PnL percentage
    margin = get_margin(filename)
    grouped['PnL_percentage'] = (grouped['Pnl'] / margin) * 100
    grouped.to_csv('filtered_rows.csv', index=False)

    total = len(grouped)
    if total == 0:
        pos_prob = 0
        neg_prob = 0
    else:
        pos_prob = (grouped['PnL_percentage'] > 0).sum() / total
        neg_prob = (grouped['PnL_percentage'] <= 0).sum() / total

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
            'text': f'{filename}',
            'font': {'size': 18, 'family': 'Arial', 'color': 'white'}
        },
        font={'family': 'Arial', 'size': 14, 'color': 'white'},
        template='plotly_dark'
    )

    # Prepare table data and columns
    table_data = grouped.to_dict('records')
    table_columns = [{"name": i, "id": i} for i in grouped.columns]
    table_style = {'display': 'block'} if 'show' in show_table_value else {'display': 'none'}
    return fig, table_data, table_columns, table_style

if __name__ == '__main__':
    app.run(debug=True, port=8053)