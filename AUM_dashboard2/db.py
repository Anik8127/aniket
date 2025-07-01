import pandas as pd
import numpy as np
import plotly.express as px
import dash
from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc

# Load data
df = pd.read_csv('aum_data_transformed.csv')

# Ensure correct types
df['Date'] = pd.to_datetime(df['Date'])
df['Year'] = df['Date'].dt.year
df['Month'] = df['Date'].dt.month
df['AUM_in_Rs_Cr'] = pd.to_numeric(df['AUM_in_Rs_Cr'], errors='coerce')
df['AUM_Monthwise_Rs.in_Crores'] = pd.to_numeric(df['AUM_Monthwise_Rs.in_Crores'], errors='coerce')
df['Yearwise_Current_AUM(Rs. in Crores)'] = pd.to_numeric(df['Yearwise_Current_AUM(Rs. in Crores)'], errors='coerce')

# Clean up client_id: strip spaces and uppercase for matching
df['Client_id'] = df['Client_id'].astype(str).str.strip()

# List of broker names to check (case-insensitive)
broker_id_list = ['DIVYA PORTFOLIO', 'FINDOC', 'SMART EQUITY', 'DB']

# If client_id matches any in the list (case-insensitive), set Broker_Name to client_id value
df['Broker_Name'] = np.where(
    df['Client_id'].str.upper().isin([b.upper() for b in broker_id_list]),
    df['Client_id'],
    df['Broker_Name']
)

# Standardize FINDOC/Findoc in Broker_Name to FINDOC
df['Broker_Name'] = df['Broker_Name'].replace(
    to_replace=r'(?i)^findoc$', value='FINDOC', regex=True
)

# Helper: For each year, get the last available month and use its AUM for dashboard
def get_year_end_aum(df):
    result = []
    for year, group in df.groupby('Year'):
        # Find last available month
        last_month = group['Month'].max()
        last_rows = group[group['Month'] == last_month]
        # Sum AUM for that month for all clients
        aum = last_rows['AUM_Monthwise_Rs.in_Crores'].iloc[0] if not last_rows.empty else 0
        result.append({'Year': year, 'AUM': aum, 'Month': last_month})
    return pd.DataFrame(result)

year_end_aum = get_year_end_aum(df)

# For pie chart: get Broker-wise AUM for selected year (last available month)
def get_brokerwise_aum(df, year):
    group = df[df['Year'] == year]
    last_month = group['Month'].max()
    last_rows = group[group['Month'] == last_month]
    brokerwise = last_rows.groupby('Broker_Name')['AUM_in_Rs_Cr'].sum().reset_index()
    return brokerwise, last_month

# For bar chart: monthwise AUM
monthwise_aum = df.groupby('Date')['AUM_Monthwise_Rs.in_Crores'].first().reset_index()
monthwise_aum = monthwise_aum.sort_values('Date')

# Month-on-month and year-on-year change
monthwise_aum['MoM_Change'] = monthwise_aum['AUM_Monthwise_Rs.in_Crores'].diff()
monthwise_aum['YoY_Change'] = monthwise_aum['AUM_Monthwise_Rs.in_Crores'].diff(12)

# Dash app with dark theme
external_stylesheets = [dbc.themes.CYBORG]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

# Add custom CSS for animated gradient background
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
        body {
            min-height: 100vh;
            background: linear-gradient(-45deg, #232526, #414345, #283e51, #485563);
            background-size: 400% 400%;
            animation: gradientBG 15s ease infinite;
        }
        @keyframes gradientBG {
            0% {background-position: 0% 50%;}
            50% {background-position: 100% 50%;}
            100% {background-position: 0% 50%;}
        }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# Glassmorphism CSS
GLASS_STYLE = {
    'background': 'rgba(40, 40, 50, 0.35)',
    'boxShadow': '0 8px 32px 0 rgba(31, 38, 135, 0.37)',
    'backdropFilter': 'blur(8px)',
    'WebkitBackdropFilter': 'blur(8px)',
    'borderRadius': '20px',
    'border': '1px solid rgba(255, 255, 255, 0.18)',
    'padding': '30px',
    'marginTop': '30px',
    'marginBottom': '30px'
}

app.layout = dbc.Container([
    html.Div([
        html.H1("AUM Dashboard", className="text-center my-4", style={'color': '#fff'}),
        dbc.Row([
            dbc.Col([
                html.Label("Select Year:", style={'color': '#fff'}),
                dcc.Dropdown(
                    id='year-dropdown',
                    options=[{'label': str(y), 'value': y} for y in sorted(df['Year'].unique())],
                    value=sorted(df['Year'].unique())[-1],
                    clearable=False,
                    style={'color': '#000'}
                ),
            ], width=3),
            dbc.Col([
                html.Div(id='aum-callout', className='display-4 text-success text-center')
            ], width=9)
        ], align='center', className='mb-4'),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='brokerwise-pie')
            ], width=6),
            dbc.Col([
                dcc.Graph(id='aum-bar')
            ], width=6)
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='aum-line')
            ], width=12)
        ]),
        dbc.Row([
            dbc.Col([
                dcc.Graph(id='aum-yearwise-bar')
            ], width=12)
        ])
    ], style=GLASS_STYLE)
], fluid=True, style={'backgroundColor': '#222'})

# Callbacks
@app.callback(
    Output('brokerwise-pie', 'figure'),
    Output('aum-callout', 'children'),
    Input('year-dropdown', 'value')
)
def update_pie(year):
    brokerwise, last_month = get_brokerwise_aum(df, year)
    # Pie chart
    fig = px.pie(
        brokerwise, names='Broker_Name', values='AUM_in_Rs_Cr',
        title=f'Broker-wise AUM Distribution ({year}, Month: {last_month})',
        color_discrete_sequence=px.colors.sequential.Plasma_r,
        hole=0.4
    )
    fig.update_traces(textinfo='percent+label', hovertemplate='%{label}: %{value:.2f} Cr')
    fig.update_layout(paper_bgcolor='#222', plot_bgcolor='#222', font_color='#fff')
    # Callout value
    total_aum = brokerwise['AUM_in_Rs_Cr'].sum()
    callout = f"Current AUM for {year}: {total_aum:,.2f} Cr"
    return fig, callout

@app.callback(
    Output('aum-bar', 'figure'),
    Input('year-dropdown', 'value')
)
def update_bar(year):
    # Bar: Monthwise AUM for selected year
    data = monthwise_aum[monthwise_aum['Date'].dt.year == year]
    fig = px.bar(
        data, x='Date', y='AUM_Monthwise_Rs.in_Crores',
        title=f'Monthwise AUM (Rs. in Crores) - {year}',
        labels={'AUM_Monthwise_Rs.in_Crores': 'AUM (Cr)'},
        color='AUM_Monthwise_Rs.in_Crores',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    fig.update_layout(paper_bgcolor='#222', plot_bgcolor='#222', font_color='#fff')
    return fig

@app.callback(
    Output('aum-line', 'figure'),
    Input('year-dropdown', 'value')
)
def update_aum_line(selected_year):
    # Show all data from 2021 till today, ignore year filter
    fig = px.line(
        monthwise_aum, x='Date', y='AUM_Monthwise_Rs.in_Crores',
        title='AUM Change Monthwise (2021 - Present)',
        labels={'AUM_Monthwise_Rs.in_Crores': 'AUM (Cr)'},
        markers=True
    )
    fig.update_traces(line_color='#00d4ff')
    fig.update_layout(paper_bgcolor='#222', plot_bgcolor='#222', font_color='#fff')
    return fig

@app.callback(
    Output('aum-yearwise-bar', 'figure'),
    Input('year-dropdown', 'value')
)
def update_yearwise_bar(selected_year):
    # Bar: Yearwise AUM (not summed, just last available month per year)
    fig = px.bar(
        year_end_aum, x='Year', y='AUM',
        title='Yearwise AUM (Last Available Month, Not Summed)',
        labels={'AUM': 'AUM (Cr)'},
        color='AUM',
        color_continuous_scale=px.colors.sequential.Plasma
    )
    fig.update_layout(paper_bgcolor='#222', plot_bgcolor='#222', font_color='#fff')
    return fig

if __name__ == '__main__':
    app.run(debug=True)

