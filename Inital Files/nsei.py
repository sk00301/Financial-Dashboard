import os
import pickle
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yfinance as yf
import streamlit as st
from streamlit_lightweight_charts import renderLightweightCharts

# Create directory for local data storage
DATA_DIR = Path("stock_data_cache")
os.makedirs(DATA_DIR, exist_ok=True)

# Page configuration
st.set_page_config(page_title="NIFTY 50 Analysis", layout="wide")
st.title("📊 NIFTY 50 (^NSEI) - 100 Week Moving Average Analysis")

# Settings panel
with st.expander("⚙️ Analysis Settings", expanded=True):
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Moving Average Period")
        st.info("**Fixed at 100 Weekly Closes**")
        wma_period = 100  # 100 weeks
    
    with col2:
        st.markdown("### Analysis Info")
        st.info("Analyzing NIFTY 50 Index (^NSEI)\nUsing Weekly Close Data")

# Helper functions
def get_cache_filepath(symbol):
    """Get the filepath for a stock's cached data"""
    return DATA_DIR / f"{symbol}_data.pkl"

def load_cached_data(symbol):
    """Load cached stock data from local storage"""
    filepath = get_cache_filepath(symbol)
    if filepath.exists():
        try:
            with open(filepath, 'rb') as f:
                cached = pickle.load(f)
            return cached['data'], cached['last_update']
        except:
            return None, None
    return None, None

def save_cached_data(symbol, df, last_update):
    """Save stock data to local cache"""
    filepath = get_cache_filepath(symbol)
    try:
        with open(filepath, 'wb') as f:
            pickle.dump({'data': df, 'last_update': last_update}, f)
    except Exception as e:
        pass

def get_stock_data_incremental(symbol, wma_period):
    """
    Fetch stock data with FULL HISTORICAL DATA for charts
    - Fetches daily data first
    - Converts to weekly data
    - Calculates 100-week moving average on weekly closes
    """
    try:
        ticker = symbol
        today = datetime.now().date()
        
        # Try to load cached data
        cached_df, last_update = load_cached_data(symbol)
        
        # Determine if we need to fetch data
        if cached_df is not None and last_update is not None:
            # Check if cache has enough historical data (at least 10 years)
            cache_span_days = (cached_df.index[-1] - cached_df.index[0]).days
            
            # If cache doesn't have enough history, refetch everything
            if cache_span_days < 3650:  # Less than 10 years
                stock = yf.Ticker(ticker)
                df_daily = stock.history(period='max')
                if not df_daily.empty:
                    save_cached_data(symbol, df_daily, today)
            else:
                # Cache has good history, check if it needs update
                days_old = (today - last_update).days
                
                if days_old == 0:
                    # Cache is from today, use it directly
                    df_daily = cached_df
                elif days_old <= 7:
                    # Cache is recent, fetch incremental data
                    try:
                        stock = yf.Ticker(ticker)
                        start_date = last_update + timedelta(days=1)
                        new_data = stock.history(start=start_date.strftime('%Y-%m-%d'))
                        
                        if not new_data.empty:
                            # Combine cached data with new data
                            df_daily = pd.concat([cached_df, new_data])
                            df_daily = df_daily[~df_daily.index.duplicated(keep='last')]
                            df_daily = df_daily.sort_index()
                            save_cached_data(symbol, df_daily, today)
                        else:
                            df_daily = cached_df
                    except:
                        df_daily = cached_df
                else:
                    # Cache is too old, fetch fresh data
                    stock = yf.Ticker(ticker)
                    df_daily = stock.history(period='max')
                    if not df_daily.empty:
                        save_cached_data(symbol, df_daily, today)
        else:
            # No cache, fetch maximum available history
            stock = yf.Ticker(ticker)
            df_daily = stock.history(period='max')
            if not df_daily.empty:
                save_cached_data(symbol, df_daily, today)
        
        if df_daily.empty:
            return None, None
        
        # Convert daily data to weekly data (using Friday as week end, or last trading day of week)
        df_weekly = df_daily.resample('W-FRI').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        if len(df_weekly) < wma_period:
            return df_daily, None
        
        # Calculate 100-week moving average on weekly closes
        df_weekly['MA_100W'] = df_weekly['Close'].rolling(window=wma_period).mean()
        
        # Also calculate some additional MAs for context on weekly data
        df_weekly['MA_50W'] = df_weekly['Close'].rolling(window=50).mean()
        df_weekly['MA_20W'] = df_weekly['Close'].rolling(window=20).mean()
        
        # Map weekly MA to daily data for visualization
        # For each daily date, use the weekly MA from that week
        df_daily['MA_100W'] = df_daily.index.to_series().apply(
            lambda x: df_weekly[df_weekly.index <= x]['MA_100W'].iloc[-1] 
            if len(df_weekly[df_weekly.index <= x]) > 0 and pd.notna(df_weekly[df_weekly.index <= x]['MA_100W'].iloc[-1])
            else None
        )
        
        return df_daily, df_weekly
        
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None, None

def create_tradingview_chart(df, symbol, wma_period, chart_key):
    """Create TradingView-style interactive chart with 100-week MA"""
    
    # Prepare candlestick data
    candle_data = []
    for idx, row in df.iterrows():
        candle_data.append({
            'time': int(idx.timestamp()),
            'open': float(row['Open']),
            'high': float(row['High']),
            'low': float(row['Low']),
            'close': float(row['Close'])
        })
    
    # Prepare 100-Week MA line data
    wma_data = []
    for idx, row in df.iterrows():
        if pd.notna(row.get('MA_100W')):
            wma_data.append({
                'time': int(idx.timestamp()),
                'value': float(row['MA_100W'])
            })
    
    # TradingView-style chart configuration
    chart_options = {
        "layout": {
            "background": {"type": "solid", "color": "#0e1117"},
            "textColor": "#d1d4dc",
        },
        "grid": {
            "vertLines": {"color": "rgba(42, 46, 57, 0.5)"},
            "horzLines": {"color": "rgba(42, 46, 57, 0.5)"},
        },
        "crosshair": {
            "mode": 1,
            "vertLine": {
                "width": 1,
                "color": "rgba(224, 227, 235, 0.8)",
                "style": 0,
                "labelBackgroundColor": "#2962FF",
                "visible": True,
                "labelVisible": True,
            },
            "horzLine": {
                "width": 1,
                "color": "rgba(224, 227, 235, 0.8)",
                "style": 0,
                "labelBackgroundColor": "#2962FF",
                "visible": True,
                "labelVisible": True,
            },
        },
        "timeScale": {
            "borderColor": "rgba(197, 203, 206, 0.4)",
            "timeVisible": True,
            "secondsVisible": False,
            "rightOffset": 12,
            "barSpacing": 10,
            "minBarSpacing": 0.5,
        },
        "rightPriceScale": {
            "borderColor": "rgba(197, 203, 206, 0.4)",
            "scaleMargins": {
                "top": 0.1,
                "bottom": 0.1,
            },
            "autoScale": True,
        },
        "handleScroll": {
            "mouseWheel": True,
            "pressedMouseMove": True,
        },
        "handleScale": {
            "axisPressedMouseMove": True,
            "mouseWheel": True,
            "pinch": True,
        },
        "width": 0,
        "height": 800,
    }
    
    # Series configurations
    series = [
        {
            "type": "Candlestick",
            "data": candle_data,
            "options": {
                "upColor": "#26a69a",
                "downColor": "#ef5350",
                "borderVisible": False,
                "wickUpColor": "#26a69a",
                "wickDownColor": "#ef5350",
                "priceScaleId": "right",
                "title": symbol,
            }
        },
        {
            "type": "Line",
            "data": wma_data,
            "options": {
                "color": "#ffa500",
                "lineWidth": 3,
                "title": "100 Week MA",
                "priceScaleId": "right",
                "lastValueVisible": True,
                "priceLineVisible": True,
            }
        }
    ]
    
    # Render chart
    renderLightweightCharts([
        {
            "chart": chart_options,
            "series": series
        }
    ], chart_key)

def create_chart_controls(df, symbol):
    """Create interactive time range controls"""
    
    st.markdown("#### ⏱️ Time Range")
    
    # Time range buttons
    col1, col2, col3, col4, col5, col6, col7, col8, col9 = st.columns(9)
    
    ranges = {
        '1M': 30,
        '3M': 90,
        '6M': 180,
        '1Y': 365,
        '2Y': 730,
        '5Y': 1825,
        '10Y': 3650,
        '15Y': 5475,
        'ALL': None
    }
    
    cols = [col1, col2, col3, col4, col5, col6, col7, col8, col9]
    for col, (label, days) in zip(cols, ranges.items()):
        with col:
            if st.button(label, use_container_width=True, key=f'range_{symbol}_{label}'):
                st.session_state[f'{symbol}_range'] = label
    
    # Get selected range
    selected_range = st.session_state.get(f'{symbol}_range', '1Y')
    
    # Filter data based on range
    end_date = df.index[-1]
    
    if selected_range != 'ALL':
        days = ranges[selected_range]
        start_date = end_date - timedelta(days=days)
        if start_date < df.index[0]:
            start_date = df.index[0]
        filtered_df = df[df.index >= start_date]
    else:
        filtered_df = df
        start_date = df.index[0]
    
    # Custom date range
    with st.expander("📅 Custom Date Range"):
        col1, col2 = st.columns(2)
        
        default_start = start_date.date() if start_date >= df.index[0] else df.index[0].date()
        
        with col1:
            custom_start = st.date_input(
                "Start Date",
                value=default_start,
                min_value=df.index[0].date(),
                max_value=df.index[-1].date(),
                key=f'start_{symbol}'
            )
        with col2:
            custom_end = st.date_input(
                "End Date",
                value=end_date.date(),
                min_value=df.index[0].date(),
                max_value=df.index[-1].date(),
                key=f'end_{symbol}'
            )
        
        if st.button("Apply Custom Range", key=f'apply_{symbol}'):
            if custom_start <= custom_end:
                filtered_df = df[(df.index.date >= custom_start) & (df.index.date <= custom_end)]
                st.success(f"✅ Range: {custom_start} to {custom_end}")
            else:
                st.error("❌ Invalid date range")
    
    return filtered_df, selected_range

def display_chart_stats(df):
    """Display current price statistics"""
    latest = df.iloc[-1]
    previous = df.iloc[-2] if len(df) > 1 else latest
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    price_change = latest['Close'] - previous['Close']
    price_change_pct = (price_change / previous['Close']) * 100 if previous['Close'] != 0 else 0
    
    with col1:
        st.metric(
            label="Current Price",
            value=f"₹{latest['Close']:.2f}",
            delta=f"{price_change:.2f} ({price_change_pct:.2f}%)"
        )
    
    with col2:
        st.metric(label="Open", value=f"₹{latest['Open']:.2f}")
    
    with col3:
        st.metric(label="High", value=f"₹{latest['High']:.2f}")
    
    with col4:
        st.metric(label="Low", value=f"₹{latest['Low']:.2f}")
    
    with col5:
        st.metric(label="Volume", value=f"{latest['Volume']:,.0f}")

def display_period_analysis(df, symbol):
    """Display additional analysis for selected period"""
    
    st.markdown("#### 📊 Period Analysis")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        period_high = df['High'].max()
        period_low = df['Low'].min()
        current_price = df['Close'].iloc[-1]
        
        range_position = ((current_price - period_low) / (period_high - period_low)) * 100
        
        st.metric("Period High", f"₹{period_high:.2f}")
        st.metric("Period Low", f"₹{period_low:.2f}")
        st.metric("Range Position", f"{range_position:.1f}%")
    
    with col2:
        avg_volume = df['Volume'].mean()
        current_volume = df['Volume'].iloc[-1]
        volume_ratio = (current_volume / avg_volume) * 100
        
        st.metric("Avg Volume", f"{avg_volume:,.0f}")
        st.metric("Current Volume", f"{current_volume:,.0f}")
        st.metric("Volume Ratio", f"{volume_ratio:.1f}%")
    
    with col3:
        volatility = df['Close'].pct_change().std() * 100
        avg_daily_range = ((df['High'] - df['Low']) / df['Low']).mean() * 100
        
        st.metric("Volatility (Std)", f"{volatility:.2f}%")
        st.metric("Avg Daily Range", f"{avg_daily_range:.2f}%")
    
    with col4:
        period_start = df['Close'].iloc[0]
        period_end = df['Close'].iloc[-1]
        period_change = ((period_end - period_start) / period_start) * 100
        
        st.metric("Period Change", f"{period_change:.2f}%")
        st.metric("Total Days", f"{len(df)}")

def clear_local_cache():
    """Clear all locally cached stock data"""
    import shutil
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
        os.makedirs(DATA_DIR, exist_ok=True)
        return True
    return False

# Main Analysis
st.markdown("---")

# Analyze button
if st.button("🔍 Analyze NIFTY 50", type="primary", use_container_width=True):
    with st.spinner(f"Fetching NIFTY 50 data with 100-week moving average..."):
        df_daily, df_weekly = get_stock_data_incremental("^CNXFMCG", wma_period)
        
        if df_daily is not None:
            st.session_state['nsei_daily_data'] = df_daily
            st.session_state['nsei_weekly_data'] = df_weekly
            st.session_state['wma_period'] = wma_period
            st.success("✅ Analysis complete!")
        else:
            st.error("❌ Failed to fetch data. Please try again.")

# Display results
if 'nsei_daily_data' in st.session_state and st.session_state.get('wma_period') == wma_period:
    df_daily = st.session_state['nsei_daily_data']
    df_weekly = st.session_state.get('nsei_weekly_data')
    
    # Summary metrics using weekly data
    st.markdown("## 📊 Analysis Results")
    
    if df_weekly is not None:
        latest_week = df_weekly.iloc[-1]
        current_weekly_close = latest_week['Close']
        ma_100w_value = latest_week['MA_100W']
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Latest Weekly Close", f"₹{current_weekly_close:.2f}")
        
        with col2:
            st.metric("100 Week MA", f"₹{ma_100w_value:.2f}" if pd.notna(ma_100w_value) else "N/A")
        
        with col3:
            if pd.notna(ma_100w_value):
                pct_diff = ((current_weekly_close - ma_100w_value) / ma_100w_value) * 100
                status = "Below" if current_weekly_close < ma_100w_value else "Above"
                st.metric(f"% {status} 100W MA", f"{abs(pct_diff):.2f}%")
            else:
                st.metric("% vs 100W MA", "N/A")
        
        with col4:
            if pd.notna(ma_100w_value):
                if current_weekly_close < ma_100w_value:
                    st.error("🔴 Below 100W MA")
                else:
                    st.success("🟢 Above 100W MA")
            else:
                st.info("⚪ N/A")
        
        st.markdown("---")
        
        # Display recent weekly data
        st.markdown("### 📅 Recent Weekly Data")
        
        # Show last 10 weeks
        recent_weeks = df_weekly.tail(10).copy()
        recent_weeks['Week Ending'] = recent_weeks.index.strftime('%Y-%m-%d')
        recent_weeks['% vs 100W MA'] = ((recent_weeks['Close'] - recent_weeks['MA_100W']) / recent_weeks['MA_100W'] * 100).round(2)
        
        display_cols = ['Week Ending', 'Open', 'High', 'Low', 'Close', 'MA_100W', '% vs 100W MA', 'Volume']
        recent_weeks_display = recent_weeks[display_cols].iloc[::-1]  # Reverse to show most recent first
        
        st.dataframe(
            recent_weeks_display.style.format({
                'Open': '₹{:.2f}',
                'High': '₹{:.2f}',
                'Low': '₹{:.2f}',
                'Close': '₹{:.2f}',
                'MA_100W': '₹{:.2f}',
                '% vs 100W MA': '{:.2f}%',
                'Volume': '{:,.0f}'
            }),
            use_container_width=True,
            height=400
        )
    else:
        st.warning("Not enough data to calculate 100-week moving average. Need at least 100 weeks of data.")
    
    st.markdown("---")
    
    # Display current daily stats
    st.markdown("#### 📊 Current Daily Statistics")
    display_chart_stats(df_daily)
    
    st.markdown("---")
    
    # Chart controls
    filtered_df, selected_range = create_chart_controls(df_daily, "NSEI")
    
    st.markdown("---")
    
    # Render chart
    st.markdown(f"### 📊 NIFTY 50 - Daily Chart with 100 Week MA ({selected_range})")
    st.markdown(f"**{len(df_daily)} days of historical data available | Showing {len(filtered_df)} days**")
    
    create_tradingview_chart(
        filtered_df,
        "NIFTY 50",
        wma_period,
        f'chart_nsei_{selected_range}'
    )
    
    st.markdown("---")
    
    # Period analysis
    display_period_analysis(filtered_df, "NSEI")
    
    st.markdown("---")
    
    # Cache management
    st.markdown("### 🗄️ Cache Management")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh Data"):
            if 'nsei_daily_data' in st.session_state:
                del st.session_state['nsei_daily_data']
            if 'nsei_weekly_data' in st.session_state:
                del st.session_state['nsei_weekly_data']
            st.success("Session cleared! Click 'Analyze NIFTY 50' to fetch fresh data.")
    
    with col2:
        if st.button("🗑️ Clear Cache"):
            if clear_local_cache():
                if 'nsei_daily_data' in st.session_state:
                    del st.session_state['nsei_daily_data']
                if 'nsei_weekly_data' in st.session_state:
                    del st.session_state['nsei_weekly_data']
                st.success("Cache cleared!")
            else:
                st.warning("No cache to clear")

elif st.session_state.get('wma_period') != wma_period and 'nsei_daily_data' in st.session_state:
    st.info(f"⚠️ Period changed. Click 'Analyze NIFTY 50' to re-run analysis.")