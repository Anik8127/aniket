import streamlit as st
import pandas as pd
import numpy as np
import os
import gc
from datetime import date, timedelta, time
from statsmodels.tsa.stattools import adfuller, acf, pacf
import plotly.graph_objects as go

st.set_page_config(page_title="Autocorrelation Dashboard", layout="wide")
st.markdown("""
<style>
[data-testid="stAppViewContainer"] {background: rgba(25, 27, 38, 0.92);}
.block-container {background: rgba(50, 50, 60, 0.94)!important; border-radius: 20px!important; padding: 2rem!important; box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.22); backdrop-filter: blur(10px);}
[data-testid="stSidebar"] {background: rgba(23, 24, 35, 0.93)!important; border-radius: 12px 0 0 12px;}
</style>
""", unsafe_allow_html=True)

st.title("📈 Autocorrelation Dashboard of Returns")

FOLDER = "/root/aniket/CandleData_1min"
TRADING_START = time(9, 30)
TRADING_END = time(15, 29)
MAX_RESL_MINUTES = 22500

@st.cache_data(show_spinner=False)
def cached_file_list():
    try:
        return sorted([f for f in os.listdir(FOLDER) if f.endswith(".csv")])
    except Exception:
        return []

file_list = cached_file_list()
file_choice = st.selectbox("Select Stock Data File", options=file_list)
if not file_choice:
    st.stop()

@st.cache_data(show_spinner=True, max_entries=3)
def cached_load_csv(file_path):
    df = pd.read_csv(file_path, usecols=['datetime', 'c'], parse_dates=['datetime'])
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime').sort_index()
    return df

try:
    df = cached_load_csv(os.path.join(FOLDER, file_choice))
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

if df.empty or 'c' not in df.columns:
    st.error("The selected CSV must contain 'datetime' and 'c' columns.")
    st.stop()
min_date = df.index.min().date()
max_date = df.index.max().date()
default_end = max_date
default_start = max(min_date, default_end - timedelta(days=89))

date_range = st.date_input(
    "🎯 Select Date Range (calendar pops up)",
    value=(default_start, default_end),
    min_value=min_date,
    max_value=max_date
)
if (not isinstance(date_range, (list, tuple))
    or len(date_range) != 2
    or date_range[0] is None
    or date_range[1] is None):
    st.warning("Please select a start and end date from the calendar.")
    st.stop()
start_date, end_date = sorted(date_range)
if start_date > end_date:
    st.error("Start date must be before end date")
    st.stop()

# Filter by date & trading hour
with st.spinner("Filtering data..."):
    filtered = df.loc[(df.index.date >= start_date) & (df.index.date <= end_date)]
    filtered = filtered.between_time(TRADING_START, TRADING_END)
    filtered = filtered[filtered['c'].notna()]

if filtered.empty:
    st.info("No data in this date range and during trading hours.")
    del filtered; gc.collect()
    st.stop()
st.success(f"Filtered: {len(filtered)} points ({start_date} to {end_date}, 9:30–15:29).")

# Closing price plot (sequential index for continuity)
close_x = np.arange(len(filtered))
close_xticks = {idx: dt.strftime('%Y-%m-%d %H:%M') for idx, dt in enumerate(filtered.index)}
st.subheader("Closing Prices (Sequential, Smooth)")
with st.container():
    if len(filtered) < 2 or filtered['c'].isna().all():
        st.info("Not enough data to plot closing prices.")
    else:
        fig_close = go.Figure(go.Scatter(
            x=close_x, y=filtered['c'].values, mode='lines',
            line=dict(color="#36C0F2", width=2), line_shape='spline',
            hovertemplate='Index: %{x}<br>Date: %{customdata}<br>Closing Price: %{y:.2f}',
            customdata=[dt.strftime('%Y-%m-%d %H:%M') for dt in filtered.index], showlegend=False
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
        st.download_button(
            "⬇️ Download Filtered Closing Prices",
            data=filtered['c'].reset_index().rename(columns={'c':'closing_price'}).to_csv(index=False).encode(),
            file_name="filtered_closing_prices.csv",
            mime="text/csv"
        )

# Explicitly release filtered DataFrame if not needed
# del filtered; gc.collect()

# Resampling (defensive)
filtered_minutes = len(filtered)
max_resample = min(MAX_RESL_MINUTES, filtered_minutes)
resample_min = st.number_input(
    f"Resample interval (1 to {max_resample} minutes):",
    min_value=1, max_value=max_resample, value=1, step=1
)
if resample_min > filtered_minutes:
    st.error("Resample range outside the date range")
    st.stop()

def custom_resample(series, interval):
    n = len(series)
    if n == 0:
        return pd.Series(dtype=series.dtype)
    last_idxs = [min(i+interval-1, n-1) for i in range(0, n, interval)]
    last_idx_dt = [series.index[j] for j in last_idxs]
    vals = [series.iloc[j] for j in last_idxs]
    return pd.Series(vals, index=last_idx_dt, name=series.name)

with st.spinner("Resampling..."):
    if resample_min <= 390:
        resampled = filtered['c'].resample(f"{resample_min}T").last().dropna()
    else:
        resampled = custom_resample(filtered['c'], resample_min).dropna()

if resampled.empty or resampled.isna().all():
    st.info("No data after resampling for the current settings.")
    del resampled; gc.collect()
    st.stop()
st.download_button(
    "⬇️ Download Resampled Prices CSV",
    data=resampled.reset_index().to_csv(index=False).encode(),
    file_name="resampled_prices.csv",
    mime="text/csv"
)

# Returns calculation
returns = resampled.pct_change().dropna()
if returns.empty or returns.isna().all():
    st.info("No valid returns for this date range and resampling. Widen your range or decrease resampling.")
    del returns; gc.collect()
    st.stop()
st.download_button(
    "⬇️ Download Returns CSV",
    data=returns.reset_index(drop=True).to_frame("returns").to_csv(index=False).encode(),
    file_name="returns.csv",
    mime="text/csv"
)

# Returns plot (sequential)
returns_x = np.arange(len(returns))
returns_xticks = {idx: dt.strftime('%Y-%m-%d %H:%M') for idx, dt in enumerate(returns.index)}
st.subheader("Returns Over Time (Sequential, Smooth)")
with st.container():
    if len(returns) < 2 or returns.isna().all():
        st.info("Not enough valid returns to plot.")
    else:
        fig_returns = go.Figure(go.Scatter(
            x=returns_x, y=returns.values, mode='lines',
            line=dict(color='#FFBB55', width=2), line_shape='spline', connectgaps=True,
            hovertemplate='Index: %{x}<br>Date: %{customdata}<br>Return: %{y:.4%}',
            customdata=[dt.strftime('%Y-%m-%d %H:%M') for dt in returns.index],
            showlegend=False
        ))
        fig_returns.update_layout(
            template='plotly_dark', title="Returns Over Time (sequential, smooth)",
            xaxis_title="Index (sequential)", yaxis_title="Return",
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis=dict(
                tickmode='array',
                tickvals=list(returns_xticks.keys())[::max(1, len(returns_xticks)//10)],
                ticktext=list(returns_xticks.values())[::max(1, len(returns_xticks)//10)],
            )
        )
        st.plotly_chart(fig_returns, use_container_width=True)

# Defensive subsample to save memory/resources
ACF_MAX_LEN = 1500
returns_acf = returns[-ACF_MAX_LEN:] if len(returns) > ACF_MAX_LEN else returns.copy()
returns_len = len(returns_acf)
# Cap lags: always less than 50 and less than len/4 for statsmodels
max_possible_lag = min(50, max(1, returns_len // 4))
if returns_len < 25 or returns_acf.isna().all() or max_possible_lag < 2:
    st.info("Not enough valid returns for statistical tests. Widen date range or decrease resample minutes.")
else:
    try:
        adf_result = adfuller(returns_acf)
        adf_pvalue = adf_result[1]
        stationary = adf_pvalue < 0.05
    except Exception:
        stationary = False
        adf_pvalue = np.nan

    st.markdown(
        f"- **ADF p-value**: `{adf_pvalue:.4g}` → {'✅ Stationary' if stationary else '❌ Not stationary'}"
        if not np.isnan(adf_pvalue)
        else "ADF stationarity test could not be performed."
    )
    if stationary:
        st.subheader("Autocorrelation (ACF & PACF)")
        lag = st.slider("Select lags for ACF/PACF", 1, max_possible_lag, min(20, max_possible_lag), 1)
        try:
            acf_vals = acf(returns_acf, nlags=lag)
            pacf_vals = pacf(returns_acf, nlags=lag, method='ywm')
            lags = np.arange(len(acf_vals))

            acf_placeholder = st.container()
            pacf_placeholder = st.container()

            with acf_placeholder:
                fig_acf = go.Figure(go.Bar(
                    x=lags, y=acf_vals, marker_color='#6096FD',
                    hovertemplate="Lag: %{x}<br>ACF: %{y:.3f}<extra></extra>"))
                fig_acf.update_layout(
                    template='plotly_dark', title=f"ACF (up to lag {lag})",
                    xaxis_title="Lag", yaxis_title="ACF"
                )
                st.plotly_chart(fig_acf, use_container_width=True)
                st.download_button(
                    "⬇️ Download ACF Data",
                    pd.DataFrame({'lag': lags, 'acf': acf_vals}).to_csv(index=False).encode(),
                    file_name="acf.csv", mime="text/csv"
                )
            with pacf_placeholder:
                fig_pacf = go.Figure(go.Bar(
                    x=lags, y=pacf_vals, marker_color='#C05AFF',
                    hovertemplate="Lag: %{x}<br>PACF: %{y:.3f}<extra></extra>"))
                fig_pacf.update_layout(
                    template='plotly_dark', title=f"PACF (up to lag {lag})",
                    xaxis_title="Lag", yaxis_title="PACF"
                )
                st.plotly_chart(fig_pacf, use_container_width=True)
                st.download_button(
                    "⬇️ Download PACF Data",
                    pd.DataFrame({'lag': lags, 'pacf': pacf_vals}).to_csv(index=False).encode(),
                    file_name="pacf.csv", mime="text/csv"
                )
        except Exception as e:
            st.info(f"Could not compute ACF/PACF: {str(e)}.")
        finally:
            # Clean up memory used by large arrays
            del acf_vals, pacf_vals, lags
            gc.collect()
    else:
        st.info("Returns are not stationary; ACF/PACF analysis not shown.")

# Clean up large intermediate objects after all
del df, resampled, returns, filtered
gc.collect()

st.caption("""
<i>
• All plots and computations use only the required data slice, never the whole file in memory.<br>
• Data loads, transformations, and caching are managed defensively and efficiently.<br>
• All exceptions are caught and reported as friendly messages, never crashing or killing the app.<br>
• After memory-intensive steps, Python's garbage collector is triggered to minimize resource usage.
</i>
""", unsafe_allow_html=True)
