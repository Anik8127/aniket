import pandas as pd
import json
from dash import Dash, dcc, html, Output, Input
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

# --- Create df_daily ---
if 'Key' in closed_pnl_df.columns:
    closed_pnl_df['date'] = pd.to_datetime(closed_pnl_df['Key']).dt.date
else:
    raise ValueError("No 'Key' column found in closed_pnl_df to extract date.")

df_daily = closed_pnl_df.groupby('date', as_index=False)['Pnl'].sum()
df_daily['Pnl_percentage'] = (df_daily['Pnl'] / 350000) * 100
df_daily['year'] = pd.to_datetime(df_daily['date']).dt.year

# Dropdown options
years = ['All'] + sorted(df_daily['year'].unique().tolist())
top_n_options = ['All'] + list(range(1, 101))      # 1 to 100
bottom_n_options = ['All'] + list(range(1, 101))   # 1 to 100

def get_filtered_df(year, top_n, bottom_n):
    df = df_daily.copy()
    if year != 'All':
        df = df[df['year'] == year]
    # Top N: Only positive Pnl_percentage
    if top_n != 'All':
        top_df = df[df['Pnl_percentage'] > 0].nlargest(int(top_n), 'Pnl_percentage')
    else:
        top_df = df[df['Pnl_percentage'] > 0]
    # Bottom N: Only negative Pnl_percentage
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

app = Dash(__name__)

app.layout = html.Div([
    html.H2('Top & Bottom Daily Pnl Percentage DNS3_N25', style={'color': 'white', 'font-family': 'Arial'}),
    html.Div([
        html.Label('Year:', style={'color': 'white'}),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(y), 'value': y} for y in years],
            value='All',
            clearable=False,
            style={'width': '150px', 'color': 'black'}
        ),
        html.Label('Top N:', style={'color': 'white', 'margin-left': '30px'}),
        dcc.Dropdown(
            id='topn-dropdown',
            options=[{'label': str(n), 'value': n} for n in top_n_options],
            value='All',
            clearable=False,
            style={'width': '100px', 'color': 'black'}
        ),
        html.Label('Bottom N:', style={'color': 'white', 'margin-left': '30px'}),
        dcc.Dropdown(
            id='bottomn-dropdown',
            options=[{'label': str(n), 'value': n} for n in bottom_n_options],
            value='All',
            clearable=False,
            style={'width': '100px', 'color': 'black'}
        ),
    ], style={'display': 'flex', 'align-items': 'center', 'background': '#222', 'padding': '10px'}),
    dcc.Graph(id='bar-graph', style={'height': '700px', 'background': '#222'}),
], style={'background': '#222', 'padding': '20px', 'font-family': 'Arial'})

@app.callback(
    Output('bar-graph', 'figure'),
    Input('year-dropdown', 'value'),
    Input('topn-dropdown', 'value'),
    Input('bottomn-dropdown', 'value')
)
def update_graph(year, top_n, bottom_n):
    final_df = get_filtered_df(year, top_n, bottom_n)
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
    return fig

if __name__ == '__main__':
    app.run(debug=True)


