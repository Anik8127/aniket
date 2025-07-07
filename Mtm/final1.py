import pandas as pd
import json
import os
import glob
from dash import Dash, dcc, html, Output, Input, State
import plotly.graph_objects as go

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
    html.H2('Top & Bottom Daily Pnl Percentage', style={'color': 'white', 'font-family': 'Arial'}),
    html.Div([
        html.Label('Filename:', style={'color': 'white'}),
        dcc.Dropdown(
            id='file-dropdown',
            options=file_options,
            value=None,
            clearable=False,
            style={'width': '400px', 'color': 'black'}
        ),
        html.Label('Year:', style={'color': 'white', 'margin-left': '30px'}),
        dcc.Dropdown(
            id='year-dropdown',
            options=[],  # Will be populated dynamically
            value='All',
            clearable=False,
            style={'width': '150px', 'color': 'black'}
        ),
        html.Label('Top N:', style={'color': 'white', 'margin-left': '30px'}),
        dcc.Dropdown(
            id='topn-dropdown',
            options=[{'label': str(n), 'value': n} for n in ['All'] + list(range(1, 101))],
            value='All',
            clearable=False,
            style={'width': '100px', 'color': 'black'}
        ),
        html.Label('Bottom N:', style={'color': 'white', 'margin-left': '30px'}),
        dcc.Dropdown(
            id='bottomn-dropdown',
            options=[{'label': str(n), 'value': n} for n in ['All'] + list(range(1, 101))],
            value='All',
            clearable=False,
            style={'width': '100px', 'color': 'black'}
        ),
    ], style={'display': 'flex', 'align-items': 'center', 'background': '#222', 'padding': '10px'}),
    dcc.Graph(id='bar-graph', style={'height': '700px', 'background': '#222'}),
], style={'background': '#222', 'padding': '20px', 'font-family': 'Arial'})

@app.callback(
    Output('year-dropdown', 'options'),
    Output('year-dropdown', 'value'),
    Input('file-dropdown', 'value')
)
def update_year_dropdown(file_path):
    if not file_path:
        return [], 'All'
    closed_pnl_df = load_and_prepare_df(file_path)
    df_daily = closed_pnl_df.groupby('date', as_index=False)['Pnl'].sum()
    df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year
    years = ['All'] + sorted(df_daily['year'].unique().tolist())
    return [{'label': str(y), 'value': y} for y in years], 'All'

def get_filtered_df(df_daily, year, top_n, bottom_n):
    df = df_daily.copy()
    if year != 'All':
        df = df[df['year'] == year]
    if top_n != 'All':
        top_df = df[df['Pnl_percentage'] > 0].nlargest(int(top_n), 'Pnl_percentage')
    else:
        top_df = df[df['Pnl_percentage'] > 0]
    if bottom_n != 'All':
        bottom_df = df[df['Pnl_percentage'] < 0].nsmallest(int(bottom_n), 'Pnl_percentage')
    else:
        bottom_df = df[df['Pnl_percentage'] < 0]
    plot_df = pd.concat([top_df, bottom_df]).drop_duplicates()
    plot_df['color'] = plot_df['Pnl_percentage'].apply(lambda x: 'green' if x > 0 else 'red')
    green_df = plot_df[plot_df['color'] == 'green'].sort_values('date')
    red_df = plot_df[plot_df['color'] == 'red'].sort_values('date')
    final_df = pd.concat([green_df, red_df])
    final_df['date_str'] = pd.to_datetime(final_df['date']).dt.strftime('%d%b%y')
    return final_df

@app.callback(
    Output('bar-graph', 'figure'),
    Input('file-dropdown', 'value'),
    Input('year-dropdown', 'value'),
    Input('topn-dropdown', 'value'),
    Input('bottomn-dropdown', 'value')
)
def update_graph(file_path, year, top_n, bottom_n):
    if not file_path:
        return go.Figure()
    filename = os.path.basename(file_path)
    margin = get_margin(filename)
    closed_pnl_df = load_and_prepare_df(file_path)
    df_daily = closed_pnl_df.groupby('date', as_index=False)['Pnl'].sum()
    df_daily['Pnl_percentage'] = (df_daily['Pnl'] / margin) * 100
    df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year
    final_df = get_filtered_df(df_daily, year, top_n, bottom_n)
    fig = go.Figure()
    fig.add_bar(
        x=final_df['date_str'],
        y=final_df['Pnl_percentage'],
        marker_color=final_df['color'],
        customdata=final_df['Pnl'],
        hovertemplate='Date: %{x}<br>Pnl Percentage: %{y:.2f}%<br>Pnl: %{customdata}'
    )
    fig.update_layout(
        title={
            'text': f'Top & Bottom Daily Pnl Percentage {filename}',
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
    return fig

if __name__ == '__main__':
    app.run(debug=True, port=8051)