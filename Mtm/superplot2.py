import streamlit as st
import pandas as pd
import json
import os
import glob
import altair as alt

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

# List all JSON files
file_paths = sorted(glob.glob(os.path.join(DATA_DIR, '**', '*.json'), recursive=True))

def get_all_years():
    years = set()
    for file_path in file_paths:
        df = load_and_prepare_df(file_path)
        df['year'] = pd.to_datetime(df['date']).dt.year
        years.update(df['year'].unique())
    years = sorted([int(y) for y in years if pd.notnull(y)])
    return ['All'] + years

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

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Superplot: Combined Daily PnL Percentage for each Strategy")

years = get_all_years()
year = st.sidebar.selectbox("Year", years, index=0)
topn = st.sidebar.selectbox("Top-n", list(range(0, 21)), index=5)

for file_path in file_paths:
    filename = os.path.basename(file_path)
    margin = get_margin(filename)
    df = load_and_prepare_df(file_path)

    # Filter for positive and negative PnL separately
    pos_df = get_filtered_bars(df, margin, year, topn, positive=True)
    neg_df = get_filtered_bars(df, margin, year, topn, positive=False)

    # Calculate averages
    avg_pos = pos_df['Pnl_percentage'].mean() if not pos_df.empty else 0.0
    avg_neg = neg_df['Pnl_percentage'].mean() if not neg_df.empty else 0.0

    # Combine and prepare for plotting
    plot_df = pd.concat([pos_df, neg_df], ignore_index=True)
    plot_df['color'] = plot_df['Pnl_percentage'].apply(lambda x: 'green' if x > 0 else 'red')
    custom_x_order = list(pos_df['date_str']) + list(neg_df['date_str'])

    # Create Altair chart
    chart = alt.Chart(plot_df).mark_bar().encode(
        x=alt.X('date_str:N', title='Date', sort=custom_x_order),
        y=alt.Y('Pnl_percentage:Q', title='PnL %'),
        color=alt.Color('color:N', scale=None, legend=None),
        tooltip=['date_str', alt.Tooltip('Pnl_percentage:Q', format=".2f", title="PnL %")]
    ).properties(
        width=1000,
        height=300,
        title=f"{filename} - Combined PnL %"
    ).interactive()

    # Display chart and stats
    st.altair_chart(chart, use_container_width=True)

    st.markdown(
        f"""
        <div style="margin-bottom: 30px;">
            <strong>Average Positive PnL %:</strong> <span style="color:green;">{avg_pos:.2f}%</span> &nbsp;&nbsp;
            <strong>Average Negative PnL %:</strong> <span style="color:red;">{avg_neg:.2f}%</span>
        </div>
        """,
        unsafe_allow_html=True
    )
