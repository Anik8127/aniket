import pandas as pd
import numpy as np
import calendar
import re
import dash
from dash import dcc, html, Input, Output
import plotly.express as px

# Helper to get last date of month
def get_last_date(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day:02d}"

# Broker normalization
def normalize_broker(client_id, client_name):
    # List of broker names to check (case-insensitive)
    broker_list = ["DIVYA PORTFOLIO", "FINDOC", "DB", "LARES", "SMART EQUITY"]
    # Check client_id
    if isinstance(client_id, str) and client_id.strip().upper() in broker_list:
        broker = client_id.strip().upper()
    # Check client_name
    elif isinstance(client_name, str) and client_name.strip().upper() in broker_list:
        broker = client_name.strip().upper()
    else:
        broker = "Nirmal Bang"
    # Normalize spelling
    if broker == "FINDOC":
        broker = "FINDOC"
    elif broker == "DIVYA PORTFOLIO":
        broker = "DIVYA PORTFOLIO"
    elif broker == "DB":
        broker = "DB"
    elif broker == "LARES":
        broker = "LARES"
    elif broker == "SMART EQUITY":
        broker = "SMART EQUITY"
    return broker.title() if broker not in ["FINDOC", "DB", "SMART EQUITY"] else broker

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

app = dash.Dash(__name__)
app.title = "AUM Brokerwise Dashboard"

app.layout = html.Div([
    html.H1("AUM Brokerwise Allocation Dashboard", style={'textAlign': 'center'}),
    html.Div([
        html.Label("Select Year:"),
        dcc.Dropdown(
            id='year-dropdown',
            options=[{'label': str(y), 'value': y} for y in years],
            value=current_year,
            clearable=False,
            style={'width': '120px', 'marginRight': '20px'}
        ),
        html.Label("Select Month:"),
        dcc.Dropdown(
            id='month-dropdown',
            options=months,
            value=current_month,
            clearable=False,
            style={'width': '150px'}
        ),
    ], style={'display': 'flex', 'justifyContent': 'center', 'marginBottom': 30, 'gap': '10px'}),
    html.Div(id='aum-total-callout', style={
        'background': '#f0f6ff',
        'border': '2px solid #1976d2',
        'borderRadius': '10px',
        'padding': '18px',
        'margin': '0 auto 30px auto',
        'width': 'fit-content',
        'fontSize': '1.5em',
        'fontWeight': 'bold',
        'color': '#1976d2',
        'textAlign': 'center',
        'boxShadow': '0 2px 8px #1976d233'
    }),
    dcc.Graph(id='aum-pie-chart'),
    html.Div("© Your Company", style={'textAlign': 'center', 'marginTop': 50, 'color': '#888'})
], style={'maxWidth': '900px', 'margin': 'auto', 'fontFamily': 'Arial'})

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

@app.callback(
    Output('aum-pie-chart', 'figure'),
    [Input('year-dropdown', 'value'),
     Input('month-dropdown', 'value')]
)
def update_pie_chart(selected_year, selected_month):
    dff = df[df['Date'].str[:4].astype(int) == selected_year]
    dff = dff[dff['Date'].str[5:7].astype(int) == selected_month]
    # Aggregate per broker per date, then sum for the month
    pie_df = dff.groupby(['Broker_Name', 'Date'])['AUM_in_Rs(Cr)'].sum().groupby('Broker_Name').sum().reset_index()
    fig = px.pie(
        pie_df,
        names='Broker_Name',
        values='AUM_in_Rs(Cr)',
        title=f'Broker-wise Monthly AUM Allocation ({calendar.month_name[selected_month]} {selected_year})',
        hole=0.4
    )
    fig.update_traces(textinfo='percent+label')
    fig.update_layout(legend_title_text='Broker', legend=dict(orientation="h", y=-0.15))
    return fig

if __name__ == '__main__':
    app.run(debug=True)