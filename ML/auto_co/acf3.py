import streamlit as st
import pandas as pd
import numpy as np
import os, gc
from datetime import timedelta, time
from statsmodels.tsa.stattools import adfuller, acf, pacf
import plotly.graph_objects as go

# --- CONFIGURATION AND STYLE ---
st.set_page_config(page_title="Autocorr Dashboard", layout="wide")
st.markdown("""
<style>
[data-testid="stAppViewContainer"] {background: rgba(25, 27, 38, 0.92);}
.block-container {background: rgba(50, 50, 60, 0.94)!important; border-radius: 20px!important; padding: 2rem!important; box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.22); backdrop-filter: blur(10px);}
[data-testid="stSidebar"] {background: rgba(23, 24, 35, 0.93)!important; border-radius: 12px 0 0 12px;}
</style>
""", unsafe_allow_html=True)

# -- SETTINGS --
FOLDER = "/root/aniket/CandleData_1min"   # Change if needed
TRADING_START = time(9, 30)
TRADING_END = time(15, 29)
MAX_RESL_MINUTES = 22500
ACF_MAX_LEN = 1500

# -- SIDEBAR: File and Date Selection --
st.sidebar.header("Data File Selection")
if not os.path.isdir(FOLDER):
    st.error(f"Data folder not found: {FOLDER}")
    st.stop()
file_list = sorted([f for f in os.listdir(FOLDER) if f.endswith(".csv")])
file_choice = st.sidebar.selectbox("CSV File", options=file_list)
if not file_choice:
    st.stop()

@st.cache_resource(show_spinner=False)
def load_data(filepath):
    df = pd.read_csv(filepath, usecols=["datetime", "c"], parse_dates=["datetime"])
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    return df

try:
    df = load_data(os.path.join(FOLDER, file_choice))
except Exception as e:
    st.error(f"Failed to load file: {e}")
    st.stop()
if df.empty or "c" not in df.columns:
    st.error("CSV must have 'datetime' and 'c' columns.")
    st.stop()

min_date, max_date = df.index.date.min(), df.index.date.max()
st.sidebar.markdown(f"**Data range:** {min_date} to {max_date}")

# --- Date Filter Sidebar ---
st.sidebar.header("Date Filter")
date_range = st.sidebar.date_input(
    "Choose Date Range",
    value=(max_date - timedelta(days=60), max_date),
    min_value=min_date,
    max_value=max_date
)
try:
    start_date, end_date = sorted(date_range)
except:
    st.warning("Please select a valid start/end date.")
    st.stop()

def filter_df_by_date(df, start_date, end_date):
    df_filtered = df[(df.index.date >= start_date) & (df.index.date <= end_date)]
    df_filtered = df_filtered.between_time(TRADING_START, TRADING_END)
    return df_filtered[~df_filtered['c'].isna()]

filtered_df = filter_df_by_date(df, start_date, end_date)

downloads = {}

# RESAMPLING SIDEBAR CONTROL
filtered_minutes = len(filtered_df)
max_resample = min(MAX_RESL_MINUTES, filtered_minutes) if filtered_minutes > 0 else 1
st.sidebar.header("Resample Frequency")
resample_min = st.sidebar.slider(
    "Resample interval (minutes)",
    min_value=1,
    max_value=max_resample if max_resample > 0 else 1,
    value=1,
    step=1,
    help="Change the frequency of bars for analysis (1 = original, higher means lower frequency)"
)

# Defensive custom resampling function
def custom_resample(series, interval):
    n = len(series)
    if n == 0:
        return pd.Series(dtype=series.dtype)
    last_idxs = [min(i+interval-1, n-1) for i in range(0, n, interval)]
    last_idx_dt = [series.index[j] for j in last_idxs]
    vals = [series.iloc[j] for j in last_idxs]
    return pd.Series(vals, index=last_idx_dt, name=series.name)

# Resample as requested by user
if filtered_df.empty:
    resampled_close = pd.Series(dtype="float64")
else:
    if resample_min == 1:
        resampled_close = filtered_df['c'].copy()
    else:
        # Try pandas resample for up to ~one day (390 minutes, regular market session)
        if resample_min <= 390:
            resampled_close = filtered_df['c'].resample(f"{resample_min}T").last().dropna()
        else:
            resampled_close = custom_resample(filtered_df['c'], resample_min).dropna()

downloads['resampled_prices.csv'] = (
    resampled_close.reset_index().rename(columns={'c':'closing_price'})
)

# Calculate returns on resampled data
if resampled_close.empty or len(resampled_close) < 2:
    returns = pd.Series(dtype='float64')
else:
    returns = resampled_close.pct_change().dropna()

downloads['returns.csv'] = (
    pd.DataFrame({
        'datetime': returns.index,
        'returns': returns.values
    })
)

# ---- MAIN TABS LAYOUT ----
tabs = st.tabs([
    f"📈 Prices ({resample_min}-min)", 
    f"📉 Returns ({resample_min}-min)", 
    "🔬 ACF/PACF", 
    "⬇️ Downloads"
])

# ---- PRICES TAB ----
with tabs[0]:
    st.subheader(f"Closing Prices (Resampled: every {resample_min} min)")
    if resampled_close.empty:
        st.info("No data for this range and resampling interval.")
    else:
        close_x = np.arange(len(resampled_close))
        close_xticks = {idx: dt.strftime('%Y-%m-%d %H:%M') for idx, dt in enumerate(resampled_close.index)}
        fig_close = go.Figure(go.Scatter(
            x=close_x, y=resampled_close.values, mode='lines',
            line=dict(color="#36C0F2", width=2), line_shape='spline',
            hovertemplate='Index: %{x}<br>Date: %{customdata}<br>Closing Price: %{y:.2f}',
            customdata=[dt.strftime('%Y-%m-%d %H:%M') for dt in resampled_close.index]
        ))
        fig_close.update_layout(
            template='plotly_dark', title="Closing Prices (sequential, smooth)",
            xaxis_title="Index (sequential)", yaxis_title="Closing Price",
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis=dict(
                tickmode='array',
                tickvals=list(close_xticks.keys())[::max(1, len(close_xticks)//10)],
                ticktext=list(close_xticks.values())[::max(1, len(close_xticks)//10)],
            )
        )
        st.plotly_chart(fig_close, use_container_width=True)

# ---- RETURNS TAB ----
with tabs[1]:
    st.subheader(f"Returns Over Time (Resampled: every {resample_min} min)")
    if returns.empty:
        st.info("No valid returns for this setting.")
    else:
        returns_x = np.arange(len(returns))
        returns_xticks = {idx: dt.strftime('%Y-%m-%d %H:%M') for idx, dt in enumerate(returns.index)}
        fig_returns = go.Figure(go.Scatter(
            x=returns_x, y=returns.values, mode='lines',
            line=dict(color='#FFBB55', width=2), line_shape='spline', connectgaps=True,
            hovertemplate='Index: %{x}<br>Date: %{customdata}<br>Return: %{y:.4%}',
            customdata=[dt.strftime('%Y-%m-%d %H:%M') for dt in returns.index]
        ))
        fig_returns.update_layout(
            template='plotly_dark', title="Returns (sequential, smooth)",
            xaxis_title="Index (sequential)", yaxis_title="Return",
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis=dict(
                tickmode='array',
                tickvals=list(returns_xticks.keys())[::max(1, len(returns_xticks)//10)],
                ticktext=list(returns_xticks.values())[::max(1, len(returns_xticks)//10)],
            )
        )
        st.plotly_chart(fig_returns, use_container_width=True)

# ---- ACF/PACF TAB ----
with tabs[2]:
    st.subheader("Stationarity and Autocorrelation (on displayed returns)")
    if returns.empty or returns.isna().all():
        st.info("Not enough valid returns for ACF/PACF.")
    else:
        if len(returns) > ACF_MAX_LEN:
            series_acf = returns[-ACF_MAX_LEN:]
        else:
            series_acf = returns.copy()
        returns_len = len(series_acf)
        max_possible_lag = min(50, max(2, returns_len // 4))

        # Stationarity test
        try:
            adf_result = adfuller(series_acf)
            adf_pvalue = adf_result[1]
            stationary = adf_pvalue < 0.05
        except Exception:
            stationary = False
            adf_pvalue = np.nan

        st.markdown(
            f"- **ADF p-value**: `{adf_pvalue:.4g}` → {'✅ Stationary' if stationary else '❌ Not stationary'}"
            if not np.isnan(adf_pvalue)
            else "- Stationarity test could not be performed."
        )

        if stationary:
            lag = st.slider("Select lags for ACF/PACF", 2, max_possible_lag, min(20, max_possible_lag), 1)
            try:
                acf_vals = acf(series_acf, nlags=lag)
                pacf_vals = pacf(series_acf, nlags=lag, method='ywm')
                lags = np.arange(len(acf_vals))

                # ACF plot
                fig_acf = go.Figure(go.Bar(
                    x=lags, y=acf_vals, marker_color='#6096FD',
                    hovertemplate="Lag: %{x}<br>ACF: %{y:.3f}<extra></extra>"
                ))
                fig_acf.update_layout(
                    template='plotly_dark', title=f"ACF (up to lag {lag})",
                    xaxis_title="Lag", yaxis_title="ACF"
                )
                st.plotly_chart(fig_acf, use_container_width=True)
                downloads['acf.csv'] = pd.DataFrame({'lag': lags, 'acf': acf_vals})

                # PACF plot
                fig_pacf = go.Figure(go.Bar(
                    x=lags, y=pacf_vals, marker_color='#C05AFF',
                    hovertemplate="Lag: %{x}<br>PACF: %{y:.3f}<extra></extra>"
                ))
                fig_pacf.update_layout(
                    template='plotly_dark', title=f"PACF (up to lag {lag})",
                    xaxis_title="Lag", yaxis_title="PACF"
                )
                st.plotly_chart(fig_pacf, use_container_width=True)
                downloads['pacf.csv'] = pd.DataFrame({'lag': lags, 'pacf': pacf_vals})
            except Exception as e:
                st.info(f"Could not compute ACF/PACF: {e}.")
            finally:
                del acf_vals, pacf_vals, lags
                gc.collect()
        else:
            st.info("Returns are not stationary; ACF/PACF suppressed.")
    gc.collect()

# ---- DOWNLOADS TAB ----
with tabs[3]:
    st.subheader("Download Data")
    if not downloads:
        st.info("No data available for download.")
    else:
        for fname, df_ in downloads.items():
            if isinstance(df_, pd.DataFrame) or isinstance(df_, pd.Series):
                st.download_button(
                    f"⬇️ Download {fname}",
                    data=df_.to_csv(index=False).encode(),
                    file_name=fname,
                    mime="text/csv"
                )

# -- CAPTION / FOOTER ---
st.caption("""
<i>
• All plots and computations operate only on filtered & resampled data.<br>
• Data transformations are efficient and exceptions handled gracefully.<br>
• Download your filtered, resampled, returns, ACF, PACF data easily from the Downloads tab.<br>
• Adjust "Resample interval" in the sidebar for different analyses.
</i>
""", unsafe_allow_html=True)

# -- MEMORY CLEAN UP
del df, filtered_df, downloads, file_list, resampled_close, returns
gc.collect()
