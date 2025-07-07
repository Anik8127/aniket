import pandas as pd
import numpy as np
import calendar
import re
import dash
from dash import dcc, html, Input, Output, State
import plotly.express as px

# Helper to get last date of month
def get_last_date(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day:02d}"

# Broker normalization
def normalize_broker(client_id, client_name):
    broker_list = ["DIVYA PORTFOLIO", "FINDOC", "DB", "LARES", "SMART EQUITY"]
    # Check client_id
    if isinstance(client_id, str) and client_id.strip().upper() in broker_list:
        broker = client_id.strip().upper()
    # Check client_name
    elif isinstance(client_name, str) and client_name.strip().upper() in broker_list:
        broker = client_name.strip().upper()
    else:
        broker = "Nirmal Bang"
    # Normalize spelling (force uppercase for all)
    broker = broker.upper()
    return broker

# Read the CSV as raw lines
with open('/root/aniket/AUM_dashboard2/aum data.csv', encoding='latin1') as f:
    lines = [line for line in f if line.strip() and not line.startswith('//')]

data_rows = []
year = None
header = []
client_names = []
months_map = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

i = 0
while i < len(lines):
    line = lines[i].strip()
    # Detect year header
    if re.match(r'^\d{4},', line):
        year = int(line.split(',')[0])
        header = [h.strip() for h in line.split(',')]
        client_names = [h.strip() for h in lines[i+1].strip().split(',')]
        i += 2
        continue
    # Data row
    if re.match(r'^[A-Za-z]{3}\d{2,4}|[A-Za-z]{3}-\d{2,4}', line):
        row = line.split(',')
        month_str = row[0].strip().upper()
        # Extract month and year
        if '-' in month_str:
            # e.g. Jan-25
            m, y = month_str.split('-')
            month = months_map[m[:3].upper()]
            row_year = int('20' + y) if len(y) == 2 else int(y)
        else:
            # e.g. Jan21
            m, y = month_str[:3], month_str[3:]
            month = months_map[m.upper()]
            row_year = int('20' + y) if len(y) == 2 else int(y)
        date = get_last_date(row_year, month)
        for idx in range(2, len(row)):
            client_id = header[idx] if idx < len(header) else ''
            client_name = client_names[idx] if idx < len(client_names) else ''
            val = row[idx].strip()
            if val == '':
                continue
            try:
                aum = float(val)
            except:
                continue
            aum_cr = round(aum * 100000 / 10000000, 2)
            broker = normalize_broker(client_id, client_name)
            data_rows.append({
                'Date': date,
                'Broker_Name': broker,
                'Client_id': client_id,
                'Client_name': client_name,
                'AUM_in_Rs(Cr)': aum_cr,
                'Year': row_year,
                'Month': month
            })
    i += 1

df = pd.DataFrame(data_rows)

# AUM_Monthwise_Rs.in_Crores
df['AUM_Monthwise_Rs.in_Crores'] = df.groupby(['Date', 'Client_id'])['AUM_in_Rs(Cr)'].transform('sum')

# Yearwise_Current_AUM(Rs. in Crores) - Only for December or latest month, sum for all clients
df['Yearwise_Current_AUM(Rs. in Crores)'] = 0.0

for year in df['Year'].unique():
    df_year = df[df['Year'] == year]
    # Prefer December, else latest month
    if 12 in df_year['Month'].values:
        target_month = 12
    else:
        target_month = df_year['Month'].max()
    mask = (df['Year'] == year) & (df['Month'] == target_month)
    total = round(df.loc[mask, 'AUM_in_Rs(Cr)'].sum(), 2)  # <-- round to 2 decimals
    df.loc[mask, 'Yearwise_Current_AUM(Rs. in Crores)'] = total

# Sort by Date
df = df.sort_values('Date')

# Select columns as per requirement
df_final = df[['Date', 'Broker_Name', 'Client_id', 'Client_name', 'AUM_in_Rs(Cr)', 'AUM_Monthwise_Rs.in_Crores', 'Yearwise_Current_AUM(Rs. in Crores)']]

# Save to CSV
df_final.to_csv('/root/aniket/AUM_dashboard2/aum_data_transformed.csv', index=False)
print("Transformation complete. Output saved to aum_data_transformed.csv")

# Load the transformed data
df = pd.read_csv('/root/aniket/AUM_dashboard2/aum_data_transformed.csv')

# Prepare year and month options
years = sorted(df['Date'].str[:4].astype(int).unique())
months = [
    {'label': calendar.month_name[m], 'value': m}
    for m in sorted(df['Date'].str[5:7].astype(int).unique())
]
current_year = max(years)
current_month = int(df[df['Date'].str[:4].astype(int) == current_year]['Date'].str[5:7].max())

external_stylesheets = [
    "https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700&display=swap"
]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "AUM Brokerwise Dashboard"

# --- Dark theme and glassmorphism styles ---
DARK_BG = "#181C24"
GLASS_BG = "rgba(40, 44, 52, 0.7)"
BORDER_COLOR = "#2D3748"
FONT_COLOR = "#F7FAFC"
PIE_COLORS = [
    "#00B8A9", "#F6416C", "#FFDE7D", "#43A047", "#6A89CC", "#F8B195", "#F67280", "#355C7D", "#C06C84", "#6C5B7B"
]

card_style = {
    'background': GLASS_BG,
    'border': f'2px solid {BORDER_COLOR}',
    'borderRadius': '18px',
    'boxShadow': '0 8px 32px 0 rgba(31, 38, 135, 0.37)',
    'backdropFilter': 'blur(8px)',
    'WebkitBackdropFilter': 'blur(8px)',
    'padding': '24px',
    'margin': '16px 0',
    'color': FONT_COLOR
}

app.layout = html.Div([
    dcc.Store(id='nonnb-toggle', data=False),
    html.H1("AUM Brokerwise Allocation Dashboard", style={
        'textAlign': 'center',
        'color': '#00B8A9',
        'fontFamily': 'Montserrat, sans-serif',
        'fontWeight': 'bold',
        'marginTop': 30,
        'marginBottom': 10,
        'letterSpacing': '2px'
    }),
    html.Div([
        html.Label("Select Year:", style={'color': FONT_COLOR, 'fontWeight': 'bold', 'fontFamily': 'Montserrat'}),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(y), 'value': y} for y in years],
            value=current_year,
            clearable=False,
            className='black-dropdown',  # <-- add this line
            style={
                'width': '120px',
                'marginRight': '20px',
                'background': '#fff',      # white background for contrast
                'color': '#111',           # black font color
                'fontFamily': 'Montserrat, sans-serif',
                'fontWeight': 'bold',
                'border': '1.5px solid #00B8A9',
                'borderRadius': '8px'
            }
        ),
        html.Label("Select Month:", style={'color': FONT_COLOR, 'fontWeight': 'bold', 'fontFamily': 'Montserrat'}),
        dcc.Dropdown(
            id='month-dropdown',
            options=months,
            value=current_month,
            clearable=False,
            className='black-dropdown',  # <-- add this line
            style={
                'width': '150px',
                'background': '#fff',      # white background for contrast
                'color': '#111',           # black font color
                'fontFamily': 'Montserrat, sans-serif',
                'fontWeight': 'bold',
                'border': '1.5px solid #00B8A9',
                'borderRadius': '8px'
            }
        ),
    ], style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': 30, 'gap': '10px'}),
    html.Div(id='aum-total-callout', style={
        **card_style,
        'width': 'fit-content',
        'fontSize': '1.5em',
        'fontWeight': 'bold',
        'textAlign': 'center',
        'margin': '0 auto 30px auto',
        'color': '#00B8A9'
    }),
    html.Div([
        html.Div([
            dcc.Graph(id='aum-pie-nb-nonnb', style={'height': '500px'}),
        ], style={**card_style, 'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
        html.Div([
            dcc.Graph(id='aum-pie-nonnb-brokers', style={'height': '500px'}),
        ], style={**card_style, 'width': '48%', 'display': 'inline-block', 'verticalAlign': 'top'}),
    ], style={'display': 'flex', 'justifyContent': 'center', 'gap': '2%'}),
    html.Hr(style={'borderColor': '#222', 'margin': '40px 0'}),
    html.H3("Client-wise AUM Allocation (Latest Month)", style={
        'textAlign': 'center',
        'color': '#FFDE7D',
        'fontFamily': 'Montserrat, sans-serif',
        'fontWeight': 'bold',
        'marginBottom': 0
    }),
    html.Div([
        dcc.Graph(id='aum-pie-clientwise', style={'height': '600px'}),
    ], style={**card_style, 'width': '70%', 'margin': 'auto'}),
    html.Div("© Your Company", style={
        'textAlign': 'center',
        'marginTop': 50,
        'color': '#888',
        'fontFamily': 'Montserrat, sans-serif'
    })
], style={
    'maxWidth': '1200px',
    'margin': 'auto',
    'fontFamily': 'Montserrat, sans-serif',
    'background': DARK_BG,
    'minHeight': '100vh',
    'paddingBottom': '40px'
})

@app.callback(
    Output('aum-total-callout', 'children'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value')]
)
def update_total_callout(selected_year, selected_month):
    dff = df[df['Date'].str[:4].astype(int) == selected_year]
    dff = dff[dff['Date'].str[5:7].astype(int) == selected_month]
    if dff.empty:
        return "No data for selected month."
    date_label = dff['Date'].max()
    total = dff['AUM_in_Rs(Cr)'].sum()
    return f"Total AUM (Monthly, {calendar.month_name[selected_month]} {selected_year}): ₹ {total:,.2f} Cr"

# --- Pie Plot 1: NB vs Non NB, with drilldown ---
@app.callback(
    [Output('aum-pie-nb-nonnb', 'figure'),
     Output('aum-pie-nonnb-brokers', 'figure')],
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value'),
     Input('nonnb-toggle', 'data')]
)
def update_nb_vs_nonnb_pie(selected_year, selected_month, show_nonnb):
    dff = df[
        (df['Date'].str[:4].astype(int) == selected_year) &
        (df['Date'].str[5:7].astype(int) == selected_month)
    ]
    nb_broker = "NIRMAL BANG"
    nonnb_brokers = ["FINDOC", "DIVYA PORTFOLIO", "DB", "LARES", "SMART EQUITY"]

    nb_aum = dff[dff['Broker_Name'] == nb_broker]['AUM_in_Rs(Cr)'].sum()
    nonnb_aum = dff[dff['Broker_Name'].isin(nonnb_brokers)]['AUM_in_Rs(Cr)'].sum()

    pie1_df = pd.DataFrame({
        'Type': ['NB', 'Non NB'],
        'AUM': [nb_aum, nonnb_aum]
    })
    fig1 = px.pie(
        pie1_df, names='Type', values='AUM',
        title=f'NB vs Non NB AUM ({calendar.month_name[selected_month]} {selected_year})',
        hole=0.4,
        color_discrete_sequence=PIE_COLORS
    )
    fig1.update_traces(textinfo='percent+label', textposition='outside')
    fig1.update_layout(
        legend_title_text='',
        legend=dict(orientation="h", y=-0.15, font=dict(color=FONT_COLOR)),
        paper_bgcolor=GLASS_BG,
        plot_bgcolor=GLASS_BG,
        font=dict(color=FONT_COLOR, family='Montserrat'),
        margin=dict(t=60, b=40, l=0, r=0)
    )

    if show_nonnb and nonnb_aum > 0:
        nonnb_df = dff[dff['Broker_Name'].isin(nonnb_brokers)]
        pie2_df = nonnb_df.groupby('Broker_Name')['AUM_in_Rs(Cr)'].sum().reset_index()
        broker_short_map = {
            "FINDOC": "FD",
            "DIVYA PORTFOLIO": "DP",
            "LARES": "LR",
            "DB": "DB",
            "SMART EQUITY": "SE"
        }
        pie2_df['Broker_Short'] = pie2_df['Broker_Name'].map(broker_short_map)
        fig2 = px.pie(
            pie2_df, names='Broker_Short', values='AUM_in_Rs(Cr)',
            title='Non NB Broker-wise AUM',
            hole=0.4,
            color_discrete_sequence=PIE_COLORS
        )
        fig2.update_traces(textinfo='percent+label', textposition='outside')
        fig2.update_layout(
            legend_title_text='Broker',
            legend=dict(orientation="h", y=-0.15, font=dict(color=FONT_COLOR)),
            paper_bgcolor=GLASS_BG,
            plot_bgcolor=GLASS_BG,
            font=dict(color=FONT_COLOR, family='Montserrat'),
            margin=dict(t=60, b=40, l=0, r=0)
        )
    else:
        fig2 = px.pie(
            names=[], values=[],
            title='Non NB Broker-wise AUM',
            hole=0.4,
            color_discrete_sequence=PIE_COLORS
        )
        fig2.update_layout(
            showlegend=False,
            annotations=[dict(text="Click 'Non NB' to see breakdown", x=0.5, y=0.5, font_size=16, showarrow=False, font=dict(color=FONT_COLOR))],
            paper_bgcolor=GLASS_BG,
            plot_bgcolor=GLASS_BG,
            font=dict(color=FONT_COLOR, family='Montserrat'),
            margin=dict(t=60, b=40, l=0, r=0)
        )

    return fig1, fig2

# --- Pie Plot 2: Client-wise AUM (latest month), group < 1 Cr as Others ---
@app.callback(
    Output('aum-pie-clientwise', 'figure'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value')]
)
def update_clientwise_pie(selected_year, selected_month):
    dff = df[
        (df['Date'].str[:4].astype(int) == selected_year) &
        (df['Date'].str[5:7].astype(int) == selected_month) &
        (df['Broker_Name'] == "NIRMAL BANG")
    ]
    client_df = dff.groupby('Client_name')['AUM_in_Rs(Cr)'].sum().reset_index()
    client_df['Group'] = client_df['AUM_in_Rs(Cr)'].apply(lambda x: 'Others' if x < 1 else None)
    client_df['Label'] = client_df.apply(lambda row: 'Others' if row['Group'] == 'Others' else row['Client_name'], axis=1)
    grouped = client_df.groupby('Label')['AUM_in_Rs(Cr)'].sum().reset_index()
    fig = px.pie(
        grouped,
        names='Label',
        values='AUM_in_Rs(Cr)',
        title=f'Client-wise AUM Allocation (NB Only) ({calendar.month_name[selected_month]} {selected_year})',
        hole=0.4,
        color_discrete_sequence=PIE_COLORS
    )
    fig.update_traces(textinfo='percent+label', textposition='outside')
    fig.update_layout(
        legend_title_text='Client',
        legend=dict(
            orientation="v",
            x=1.25,
            y=0.5,
            xanchor='left',
            yanchor='middle',
            font=dict(size=10, color=FONT_COLOR)
        ),
        showlegend=False,  # Always hide legend
        width=800,
        height=450,
        paper_bgcolor=GLASS_BG,
        plot_bgcolor=GLASS_BG,
        font=dict(color=FONT_COLOR, family='Montserrat'),
        margin=dict(t=60, b=40, l=0, r=0)
    )
    return fig

@app.callback(
    Output('nonnb-toggle', 'data'),
    [Input('aum-pie-nb-nonnb', 'clickData')],
    [State('nonnb-toggle', 'data')]
)
def toggle_nonnb(clickData, current_state):
    if clickData and clickData.get('points', [{}])[0].get('label') == 'Non NB':
        return not current_state  # Toggle
    return current_state  # No change

if __name__ == '__main__':
    app.run(debug=True)

