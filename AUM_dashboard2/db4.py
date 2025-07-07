import pandas as pd
import numpy as np
import calendar
import re
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Helper to get last date of month
def get_last_date(year, month):
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-{last_day:02d}"

# Broker normalization
def normalize_broker(client_id, client_name):
    broker_list = ["DIVYA PORTFOLIO", "FINDOC", "DB", "LARES", "SMART EQUITY"]
    if isinstance(client_id, str) and client_id.strip().upper() in broker_list:
        broker = client_id.strip().upper()
    elif isinstance(client_name, str) and client_name.strip().upper() in broker_list:
        broker = client_name.strip().upper()
    else:
        broker = "Nirmal Bang"
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
    if re.match(r'^\d{4},', line):
        year = int(line.split(',')[0])
        header = [h.strip() for h in line.split(',')]
        client_names = [h.strip() for h in lines[i+1].strip().split(',')]
        i += 2
        continue
    if re.match(r'^[A-Za-z]{3}\d{2,4}|[A-Za-z]{3}-\d{2,4}', line):
        row = line.split(',')
        month_str = row[0].strip().upper()
        if '-' in month_str:
            m, y = month_str.split('-')
            month = months_map[m[:3].upper()]
            row_year = int('20' + y) if len(y) == 2 else int(y)
        else:
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
    if 12 in df_year['Month'].values:
        target_month = 12
    else:
        target_month = df_year['Month'].max()
    mask = (df['Year'] == year) & (df['Month'] == target_month)
    total = round(df.loc[mask, 'AUM_in_Rs(Cr)'].sum(), 2)
    df.loc[mask, 'Yearwise_Current_AUM(Rs. in Crores)'] = total

df = df.sort_values('Date')

# Prepare year and month options
years = sorted(df['Date'].str[:4].astype(int).unique())
# Use month names for dropdown
month_numbers = sorted(df['Date'].str[5:7].astype(int).unique())
month_name_map = {num: calendar.month_name[num] for num in month_numbers}
months = [{'label': calendar.month_name[num], 'value': calendar.month_name[num]} for num in month_numbers]
current_year = max(years)
current_month_num = int(df[df['Date'].str[:4].astype(int) == current_year]['Date'].str[5:7].max())
current_month_name = calendar.month_name[current_month_num]

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("AUM Brokerwise Allocation Dashboard")

# Sidebar for selection
selected_year = st.sidebar.selectbox("Select Year", years, index=len(years)-1)
selected_month_name = st.sidebar.selectbox("Select Month", [m['label'] for m in months], index=[m['label'] for m in months].index(current_month_name))
selected_month = [num for num, name in month_name_map.items() if name == selected_month_name][0]

# Total AUM Callout
dff = df[(df['Date'].str[:4].astype(int) == selected_year) & (df['Date'].str[5:7].astype(int) == selected_month)]
if dff.empty:
    st.warning("No data for selected month.")
else:
    date_label = dff['Date'].max()
    total = dff['AUM_in_Rs(Cr)'].sum()
    st.markdown(f"<div style='background:rgba(40,44,52,0.7);padding:16px;border-radius:12px;width:fit-content;margin:auto;color:#00B8A9;font-size:1.5em;font-weight:bold;text-align:center;'>Total AUM (Monthly, {calendar.month_name[selected_month]} {selected_year}): ₹ {total:,.2f} Cr</div>", unsafe_allow_html=True)

# --- Pie Plot 1: NB vs Non NB, with drilldown ---
nb_broker = "NIRMAL BANG"
nonnb_brokers = ["FINDOC", "DIVYA PORTFOLIO", "DB", "LARES", "SMART EQUITY"]

nb_aum = dff[dff['Broker_Name'] == nb_broker]['AUM_in_Rs(Cr)'].sum()
nonnb_aum = dff[dff['Broker_Name'].isin(nonnb_brokers)]['AUM_in_Rs(Cr)'].sum()

pie1_df = pd.DataFrame({
    'Type': ['NB', 'Non NB'],
    'AUM': [nb_aum, nonnb_aum]
})
PIE_COLORS = [
    "#00B8A9", "#F6416C", "#FFDE7D", "#43A047", "#6A89CC", "#F8B195", "#F67280", "#355C7D", "#C06C84", "#6C5B7B"
]
fig1 = px.pie(
    pie1_df, names='Type', values='AUM',
    title=f'NB vs Non NB AUM ({calendar.month_name[selected_month]} {selected_year})',
    hole=0.4,
    color_discrete_sequence=PIE_COLORS
)
fig1.update_traces(textinfo='percent+label', textposition='outside')
fig1.update_layout(
    legend_title_text='',
    legend=dict(orientation="h", y=-0.15, font=dict(color="#F7FAFC")),
    paper_bgcolor="rgba(40, 44, 52, 0.7)",
    plot_bgcolor="rgba(40, 44, 52, 0.7)",
    font=dict(color="#F7FAFC", family='Montserrat'),
    margin=dict(t=60, b=40, l=0, r=0)
)

# Drilldown for Non NB
show_nonnb = st.checkbox("Show Non NB Broker-wise AUM Drilldown", value=False)
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
        legend=dict(orientation="h", y=-0.15, font=dict(color="#F7FAFC")),
        paper_bgcolor="rgba(40, 44, 52, 0.7)",
        plot_bgcolor="rgba(40, 44, 52, 0.7)",
        font=dict(color="#F7FAFC", family='Montserrat'),
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
        annotations=[dict(text="Check the box to see breakdown", x=0.5, y=0.5, font_size=16, showarrow=False, font=dict(color="#F7FAFC"))],
        paper_bgcolor="rgba(40, 44, 52, 0.7)",
        plot_bgcolor="rgba(40, 44, 52, 0.7)",
        font=dict(color="#F7FAFC", family='Montserrat'),
        margin=dict(t=60, b=40, l=0, r=0)
    )

col1, col2 = st.columns(2)
with col1:
    st.plotly_chart(fig1, use_container_width=True)
with col2:
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# --- Pie Plot 2: Client-wise AUM (latest month), group < 1 Cr as Others ---

st.header("Client-wise AUM Allocation (Latest Month)")
show_labels = st.checkbox("Show Labels on Pie Chart", value=True)

dff_nb = dff[dff['Broker_Name'] == "NIRMAL BANG"]
client_df = dff_nb.groupby('Client_name')['AUM_in_Rs(Cr)'].sum().reset_index()
client_df['Group'] = client_df['AUM_in_Rs(Cr)'].apply(lambda x: 'Others' if x < 1 else None)
client_df['Label'] = client_df.apply(lambda row: 'Others' if row['Group'] == 'Others' else row['Client_name'], axis=1)
grouped = client_df.groupby('Label')['AUM_in_Rs(Cr)'].sum().reset_index()

# Calculate total for annotation
total_client_aum = grouped['AUM_in_Rs(Cr)'].sum()

fig3 = px.pie(
    grouped,
    names='Label',
    values='AUM_in_Rs(Cr)',
    title=f'Client-wise AUM Allocation (NB Only) ({calendar.month_name[selected_month]} {selected_year})<br><sup>Total: ₹ {total_client_aum:,.2f} Cr</sup>',
    hole=0.6,
    color_discrete_sequence=PIE_COLORS
)
fig3.update_traces(
    textinfo='percent+label' if show_labels else 'none',
    textposition='outside',
    textfont_size=12,
    hovertemplate='<b>%{label}</b><br>AUM: ₹ %{value:,.2f} Cr<br>Percent: %{percent}'
)
fig3.update_layout(
    showlegend=False,
    width=800,
    height=650,
    paper_bgcolor="rgba(40, 44, 52, 0.7)",
    plot_bgcolor="rgba(40, 44, 52, 0.7)",
    font=dict(color="#F7FAFC", family='Montserrat'),
    margin=dict(t=90, b=180, l=40, r=120)
)

st.plotly_chart(fig3, use_container_width=True)

# --- AUM Trend Analysis Plot ---

st.markdown("---")
st.header("AUM Trend Analysis")

# Timeframe Dropdown
timeframe = st.selectbox(
    "Timeframe",
    options=["Month", "Year"],
    index=0,
    key="trend_timeframe"
)

# Chart Type Dropdown
chart_type = st.selectbox(
    "Chart Type",
    options=["BAR PLOT", "LINE PLOT", "BOTH"],
    index=0,
    key="trend_chart_type"
)

plot_df = df.copy()
plot_df['Year'] = plot_df['Date'].str[:4].astype(int)
plot_df['Month'] = plot_df['Date'].str[5:7].astype(int)
plot_df['Year-Month'] = plot_df['Year'].astype(str) + '-' + plot_df['Month'].astype(str).str.zfill(2)

if timeframe == "Month":
    x_title = "Month"
    monthwise_df = plot_df.groupby('Year-Month')['AUM_in_Rs(Cr)'].sum().reset_index()
    x_vals = monthwise_df['Year-Month']
    y_vals = monthwise_df['AUM_in_Rs(Cr)']
    hover_month = [calendar.month_name[int(m.split('-')[1])] for m in x_vals]
elif timeframe == "Year":
    x_title = "Year"
    years = sorted(plot_df['Year'].unique())
    year_data = []
    
    for year in years:
        year_df = plot_df[plot_df['Year'] == year]
        dec_data = year_df[year_df['Month'] == 12]
        if not dec_data.empty:
            last_month_data = dec_data
            month_name = "Dec"
        else:
            latest_month = year_df['Month'].max()
            last_month_data = year_df[year_df['Month'] == latest_month]
            month_name = calendar.month_name[latest_month]
        
        total_aum = last_month_data['AUM_in_Rs(Cr)'].sum()
        year_data.append({
            'Year': year,
            'AUM': total_aum,
            'Month': month_name
        })
    
    year_df = pd.DataFrame(year_data)
    x_vals = year_df['Year']
    y_vals = year_df['AUM']
    hover_month = year_df['Month']

fig4 = go.Figure()

if chart_type == "LINE PLOT":
    fig4.add_trace(go.Scatter(
        x=x_vals,
        y=y_vals,
        mode='lines+markers',
        line=dict(color='#00B8A9', width=3, shape='spline'),
        marker=dict(size=7, color='#FFDE7D'),
        name="Total AUM",
        hovertemplate=f"{x_title}: %{{x}}<br>AUM: ₹ %{{y:,.2f}} Cr<br>Month: %{{text}}",
        text=hover_month
    ))
elif chart_type == "BAR PLOT":
    fig4.add_trace(go.Bar(
        x=x_vals,
        y=y_vals,
        marker_color='#F6416C',
        name="Total AUM",
        hovertemplate=f"{x_title}: %{{x}}<br>AUM: ₹ %{{y:,.2f}} Cr<br>Month: %{{text}}",
        text=hover_month,
        textposition='none'  # Remove labels inside bars
    ))
elif chart_type == "BOTH":
    fig4.add_trace(go.Bar(
        x=x_vals,
        y=y_vals,
        marker_color='#F6416C',
        name="Total AUM (Bar)",
        hovertemplate=f"{x_title}: %{{x}}<br>AUM: ₹ %{{y:,.2f}} Cr<br>Month: %{{text}}",
        text=hover_month,
        textposition='none'  # Remove labels inside bars
    ))
    fig4.add_trace(go.Scatter(
        x=x_vals,
        y=y_vals,
        mode='lines+markers',
        line=dict(color='#00B8A9', width=3, shape='spline'),
        marker=dict(size=7, color='#FFDE7D'),
        name="Total AUM (Line)",
        hovertemplate=f"{x_title}: %{{x}}<br>AUM: ₹ %{{y:,.2f}} Cr<br>Month: %{{text}}",
        text=hover_month
    ))

fig4.update_layout(
    plot_bgcolor="#111",
    paper_bgcolor="#111",
    font=dict(color="#FFF", family='Montserrat'),
    xaxis=dict(
        title=x_title,
        showgrid=True,
        gridcolor="#FFF",
        gridwidth=0.5,
        zeroline=False,
        tickmode='array' if timeframe == "Year" else 'auto',
        tickvals=x_vals if timeframe == "Year" else None,
        ticktext=[str(int(x)) for x in x_vals] if timeframe == "Year" else None
    ),
    yaxis=dict(
        title="AUM (₹ Cr)",
        showgrid=True,
        gridcolor="#FFF",
        gridwidth=0.5,
        zeroline=False
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    ),
    margin=dict(t=80, b=80, l=60, r=40),
    transition=dict(duration=500, easing='cubic-in-out'),
    hovermode="x unified",
    title=dict(
        text=f"AUM Trend by {x_title} (Last Month Value)",
        x=0.5,
        font=dict(size=20)
    )
)

st.plotly_chart(fig4, use_container_width=True)

st.markdown("<div style='text-align:center; margin-top:50px; color:#888; font-family:Montserrat, sans-serif;'>© Your Company</div>", unsafe_allow_html=True)