import streamlit as st
import pandas as pd
import yfinance as yf
import time
import plotly.graph_objects as go
from pathlib import Path
import os
import pickle
from datetime import datetime, timedelta
from streamlit_lightweight_charts import renderLightweightCharts
from concurrent.futures import ThreadPoolExecutor, as_completed

# Page configuration - must be first Streamlit command
st.set_page_config(
    page_title="NSE Stocks & Index Ratio Analysis",
    page_icon="📉",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for dark theme with white text
st.markdown("""
    <style>
    /* Dark theme colors */
    :root {
        --background-color: #0e1117;
        --secondary-background-color: #262730;
        --text-color: #ffffff;
    }
    
    /* Remove default padding and set dark background for ENTIRE page */
    .stApp {
        background-color: #0e1117 !important;
        color: #ffffff;
    }
    
    /* Remove top padding/margin and ensure dark background */
    .main .block-container {
        padding-top: 1rem;
        background-color: #0e1117;
    }
    
    /* Main content area */
    .main {
        background-color: #0e1117 !important;
        color: #ffffff;
    }
    
    /* Header area - this fixes the white top margin */
    header {
        background-color: #0e1117 !important;
    }
    
    /* Top toolbar area */
    [data-testid="stToolbar"] {
        background-color: #0e1117 !important;
    }
    
    /* Status/connection widget area */
    [data-testid="stStatusWidget"] {
        background-color: #0e1117 !important;
    }
    
    /* Entire app background including top area */
    html, body, [data-testid="stAppViewContainer"] {
        background-color: #0e1117 !important;
    }
    
    /* Streamlit header container */
    [data-testid="stHeader"] {
        background-color: #0e1117 !important;
    }
    
    /* All text white EXCEPT tables */
    p, span, div:not([data-testid="stDataFrame"] *), label, li {
        color: #ffffff !important;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #262730;
    }
    
    [data-testid="stSidebar"] * {
        color: #ffffff !important;
    }
    
    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 28px;
        color: #ffffff !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #ffffff !important;
    }
    
    [data-testid="stMetricDelta"] {
        color: #ffffff !important;
    }
    
    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
    }
    
    /* Info boxes */
    .stAlert {
        background-color: #262730;
        color: #ffffff !important;
    }
    
    .stAlert * {
        color: #ffffff !important;
    }
    
    /* Success/Error/Warning boxes */
    .stSuccess, .stError, .stWarning, .stInfo {
        color: #ffffff !important;
    }
    
    .stSuccess *, .stError *, .stWarning *, .stInfo * {
        color: #ffffff !important;
    }
    
    /* Buttons */
    .stButton > button {
        background-color: #ff4b4b;
        color: #ffffff !important;
        border: none;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        font-weight: 600;
    }
    
    .stButton > button:hover {
        background-color: #ff6b6b;
        border: none;
        color: #ffffff !important;
    }
    
    /* Download button */
    .stDownloadButton > button {
        background-color: #4CAF50;
        color: #ffffff !important;
    }
    
    .stDownloadButton > button:hover {
        background-color: #45a049;
        color: #ffffff !important;
    }
    
    /* Dataframe - let it use default/custom styling */
    [data-testid="stDataFrame"] {
        background-color: #262730;
    }
    
    /* Select box - improved contrast */
    .stSelectbox {
        color: #ffffff !important;
    }
    
    .stSelectbox > div > div {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    .stSelectbox label {
        color: #ffffff !important;
    }
    
    /* Dropdown menu items */
    .stSelectbox [data-baseweb="select"] {
        background-color: #262730 !important;
    }
    
    .stSelectbox [data-baseweb="select"] > div {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    /* Dropdown options */
    [role="option"] {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    [role="option"]:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    /* Dropdown chevron/arrow icon */
    .stSelectbox svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }
    
    /* Number input - improved contrast */
    .stNumberInput {
        color: #ffffff !important;
    }
    
    .stNumberInput > div > div > input {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    .stNumberInput label {
        color: #ffffff !important;
    }
    
    /* Number input buttons (+/-) */
    .stNumberInput button {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    .stNumberInput button:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    .stNumberInput button svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }
    
    /* Slider */
    .stSlider {
        color: #ffffff !important;
    }
    
    .stSlider > div > div > div {
        color: #ffffff !important;
    }
    
    .stSlider label {
        color: #ffffff !important;
    }
    
    .stSlider [data-testid="stTickBarMin"],
    .stSlider [data-testid="stTickBarMax"] {
        color: #ffffff !important;
    }
    
    /* Slider thumb */
    .stSlider [role="slider"] {
        background-color: #ff4b4b !important;
    }
    
    /* Slider track */
    .stSlider [data-baseweb="slider"] > div > div {
        background-color: #3d3d3d !important;
    }
    
    /* Progress bar */
    .stProgress > div > div > div {
        background-color: #ff4b4b;
    }
    
    /* Spinner text */
    .stSpinner > div {
        color: #ffffff !important;
    }
    
    /* Divider */
    hr {
        border-color: #3d3d3d;
    }
    
    /* Markdown */
    .stMarkdown {
        color: #ffffff !important;
    }
    
    /* Code blocks */
    code {
        color: #ffffff !important;
        background-color: #262730;
    }
    
    /* Links */
    a {
        color: #4da6ff !important;
    }
    
    a:hover {
        color: #80bfff !important;
    }
    
    /* Input fields */
    input {
        color: #ffffff !important;
        background-color: #262730 !important;
        border: 1px solid #3d3d3d !important;
    }
    
    input:focus {
        border-color: #ff4b4b !important;
        outline: none !important;
    }
    
    /* Placeholder text */
    ::placeholder {
        color: #888888 !important;
    }
    
    /* ============================================
       EXPANDER - COMPREHENSIVE DARK THEME FIX
       ============================================ */
    
    /* Target the expander container */
    [data-testid="stExpander"] {
        background-color: #0e1117 !important;
        border: 1px solid #3d3d3d !important;
        border-radius: 5px !important;
    }
    
    /* Expander header (the clickable button) - DEFAULT STATE */
    [data-testid="stExpander"] details summary {
        background-color: #262730 !important;
        color: #ffffff !important;
        padding: 12px 16px !important;
        border-radius: 5px !important;
    }
    
    /* Expander header - HOVER STATE */
    [data-testid="stExpander"] details summary:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    /* Expander header - OPEN/EXPANDED STATE */
    [data-testid="stExpander"] details[open] summary {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
        border-bottom: 2px solid #ff4b4b !important;
        border-radius: 5px 5px 0 0 !important;
    }
    
    /* ALL text inside expander header */
    [data-testid="stExpander"] summary * {
        color: #ffffff !important;
    }
    
    /* Expander arrow/icon */
    [data-testid="stExpander"] summary svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }
    
    /* Expander content area (what shows when expanded) */
    [data-testid="stExpander"] details[open] > div {
        background-color: #0e1117 !important;
        padding: 16px !important;
        border-radius: 0 0 5px 5px !important;
    }
    
    /* NUCLEAR OPTION - Force all expander elements to dark */
    .streamlit-expanderHeader,
    [class*="expanderHeader"],
    details summary {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    details[open] summary {
        background-color: #1a1a1a !important;
        color: #ffffff !important;
    }
    
    .streamlit-expanderContent,
    [class*="expanderContent"],
    details > div {
        background-color: #0e1117 !important;
        color: #ffffff !important;
    }
    
    /* Override any white backgrounds */
    [data-testid="stExpander"] * {
        color: #ffffff !important;
    }
    
    [data-testid="stExpander"] details summary:not([data-testid]) {
        background-color: #262730 !important;
    }
    
    [data-testid="stExpander"] details[open] summary:not([data-testid]) {
        background-color: #1a1a1a !important;
    }
    
    /* Icons in general */
    svg {
        fill: #ffffff !important;
    }
    
    /* Popover/dropdown menus */
    [data-baseweb="popover"] {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    /* List items in dropdowns */
    [data-baseweb="menu"] {
        background-color: #262730 !important;
    }
    
    [data-baseweb="menu"] li {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    [data-baseweb="menu"] li:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    /* Tooltip */
    [data-baseweb="tooltip"] {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    /* Help icon */
    [data-testid="stTooltipIcon"] {
        color: #ffffff !important;
    }
    
    [data-testid="stTooltipIcon"] svg {
        fill: #ffffff !important;
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: #262730;
        color: #ffffff !important;
        border-radius: 5px 5px 0 0;
        padding: 10px 20px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #ff4b4b;
        color: #ffffff !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background-color: #3d3d3d;
    }
    
    /* Date input styling */
    .stDateInput {
        color: #ffffff !important;
    }
    
    .stDateInput > div > div > input {
        background-color: #262730 !important;
        color: #ffffff !important;
        border: 1px solid #3d3d3d !important;
    }
    
    .stDateInput label {
        color: #ffffff !important;
    }
    
    /* Date picker calendar styling */
    [data-baseweb="calendar"] {
        background-color: #262730 !important;
        border: 1px solid #3d3d3d !important;
    }
    
    /* Calendar header (month/year selector) - FIX for top bar */
    [data-baseweb="calendar"] header {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    /* Calendar top control bar */
    [data-baseweb="calendar"] [data-baseweb="calendar-header"] {
        background-color: #262730 !important;
    }
    
    /* Month/Year selector buttons area - the gray bar at top */
    [data-baseweb="calendar"] > div:first-child {
        background-color: #262730 !important;
    }
    
    /* Day of week header row (Su, Mo, Tu, etc) - the second gray bar */
    [data-baseweb="calendar"] thead {
        background-color: #262730 !important;
    }
    
    [data-baseweb="calendar"] thead tr {
        background-color: #262730 !important;
    }
    
    /* Calendar month/year text */
    [data-baseweb="calendar"] [role="heading"] {
        color: #ffffff !important;
    }
    
    /* Calendar navigation buttons */
    [data-baseweb="calendar"] button {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    [data-baseweb="calendar"] button:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    /* Calendar day cells */
    [data-baseweb="calendar"] [role="button"] {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    [data-baseweb="calendar"] [role="button"]:hover {
        background-color: #3d3d3d !important;
        color: #ffffff !important;
    }
    
    /* Selected date */
    [data-baseweb="calendar"] [aria-selected="true"] {
        background-color: #ff4b4b !important;
        color: #ffffff !important;
    }
    
    /* Today's date indicator */
    [data-baseweb="calendar"] [data-highlighted="true"] {
        background-color: #4d4d4d !important;
        color: #ffffff !important;
    }
    
    /* Day of week labels (Mon, Tue, etc) */
    [data-baseweb="calendar"] [role="columnheader"] {
        color: #ffffff !important;
        background-color: #262730 !important;
    }
    
    
    /* Month/Year dropdown in calendar */
    [data-baseweb="popover"] [data-baseweb="select"] {
        background-color: #262730 !important;
        color: #ffffff !important;
    }
    
    /* Calendar container background */
    [data-baseweb="popover"] > div {
        background-color: #262730 !important;
    }
    
    /* NUCLEAR OPTION - Force dark background on EVERYTHING in calendar */
    [data-baseweb="calendar"],
    [data-baseweb="calendar"] *,
    [data-baseweb="calendar"] *::before,
    [data-baseweb="calendar"] *::after {
        background-color: #262730 !important;
    }
    
    /* Re-apply specific overrides for interactive elements */
    [data-baseweb="calendar"] [role="button"]:hover {
        background-color: #3d3d3d !important;
    }
    
    [data-baseweb="calendar"] [aria-selected="true"] {
        background-color: #ff4b4b !important;
    }
    
    [data-baseweb="calendar"] [data-highlighted="true"]:not([aria-selected="true"]) {
        background-color: #4d4d4d !important;
    }
            
    <style>
    /* Hide macro indicator button text - cards serve as visual */
    [key^="macro_btn_"] > button, button[kind="secondary"] {
        opacity: 0 !important;
        height: 0px !important;
        padding: 0 !important;
        margin: 0 !important;
        margin-top: -8px !important;
        border: none !important;
        pointer-events: auto !important;
    }
    </style>
""", unsafe_allow_html=True)

# Title and description
st.markdown("""
<h1 style='text-align: center; color: #ffffff; margin-bottom: 0;'>
📉 NSE Analysis Dashboard
</h1>
<p style='text-align: center; color: #b0b0b0; font-size: 18px; margin-top: 5px;'>
Real-time stock analysis and index ratio calculator
</p>
""", unsafe_allow_html=True)

st.markdown("---")

# Initialize session state for active tab
if 'active_tab' not in st.session_state:
    st.session_state.active_tab = "stocks_below_dma"

# Sidebar Navigation
with st.sidebar:
    st.markdown("## 📊 Navigation")
    
    # Navigation buttons
    if st.button("📊 Stocks Below 200 DMA", use_container_width=True, type="primary" if st.session_state.active_tab == "stocks_below_dma" else "secondary", key="nav_stocks"):
        st.session_state.active_tab = "stocks_below_dma"
        st.rerun()
    
    if st.button("📈 Index Ratio Analysis", use_container_width=True, type="primary" if st.session_state.active_tab == "index_ratio" else "secondary", key="nav_index"):
        st.session_state.active_tab = "index_ratio"
        st.rerun()
    
    if st.button("💹 FNO Trading Activity", use_container_width=True, type="primary" if st.session_state.active_tab == "fno_trading" else "secondary", key="nav_fno"):
        st.session_state.active_tab = "fno_trading"
        st.rerun()

    if st.button("📊 Index Analysis", use_container_width=True,
                type="primary" if st.session_state.active_tab == "index_analysis" else "secondary",
                key="nav_index_analysis"):
        st.session_state.active_tab = "index_analysis"
        st.rerun()

    if st.button("📡 High Frequency Macro", use_container_width=True,
                type="primary" if st.session_state.active_tab == "macro_indicators" else "secondary",
                key="nav_macro"):
        st.session_state.active_tab = "macro_indicators"
        st.rerun()


# =============================================================================
# TAB 1: STOCKS BELOW DMA/WMA WITH TRADINGVIEW CHARTS (FULL HISTORICAL DATA)
# =============================================================================

import os
import pickle
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import yfinance as yf
import streamlit as st
from streamlit_lightweight_charts import renderLightweightCharts
from concurrent.futures import ThreadPoolExecutor, as_completed

DATA_dir = Path(__file__).parent / "Data"
# Create directory for local data storage
Macrodata_dir = DATA_dir / "Macro Data"
DATA_DIR = DATA_dir / "stock_data_cache"
os.makedirs(DATA_DIR, exist_ok=True)
INDEX_DATA_DIR = DATA_dir / "NSE_Indices_Data"
MONETARY_DATA_FILE = DATA_dir / "Components of Money stock.XLSX"
GLOBAL_DATA_DIR = DATA_dir / "Global_Indices_Data"


# File path for below DMA tracking data
BELOW_DMA_FILE = DATA_dir / "below_dma(2004).csv"

@st.cache_data(ttl=3600)
def load_available_indices(directory):
    """Load available index files from the directory"""
    try:
        path = Path(directory)
        if not path.exists():
            return []
            
            # Get all CSV files
        csv_files = list(path.glob("*.csv"))
            
        # Extract index names (filename without extension)
        indices = [f.stem for f in csv_files]
            
        return sorted(indices)
    except Exception as e:
        st.error(f"Error loading indices: {e}")
        return []

def parse_mixed_dates(date_series):
    """Try multiple date formats to handle mixed date columns"""
    formats = ['%d-%m-%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y', '%Y/%m/%d']
    
    result = pd.Series([pd.NaT] * len(date_series), index=date_series.index)
    remaining_mask = pd.Series([True] * len(date_series), index=date_series.index)
    
    for fmt in formats:
        if not remaining_mask.any():
            break
        parsed = pd.to_datetime(
            date_series[remaining_mask], 
            format=fmt, 
            errors='coerce'
        )
        success_mask = parsed.notna()
        result[remaining_mask] = result[remaining_mask].where(~success_mask, parsed[success_mask])
        remaining_mask[remaining_mask] = ~success_mask
    
    return result

if st.session_state.active_tab == "stocks_below_dma":
    st.header("Stocks Trading Below Moving Average")
    
    # Settings panel moved inside this tab
    with st.expander("⚙️ Stock Analysis Settings", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### Select MA Type")
            ma_type = st.selectbox(
                "",
                options=["DMA (Daily)", "WMA (Weekly)"],
                index=0,
                help="Choose between Daily Moving Average or Weekly Moving Average",
                label_visibility="collapsed",
                key="ma_type_select"
            )
        
        with col2:
            st.markdown("### Select Period")
            if ma_type == "DMA (Daily)":
                ma_period = st.selectbox(
                    "",
                    options=[7, 30, 50, 100, 200, 365],
                    index=4,  # Default to 200
                    help="Choose the moving average period for analysis",
                    label_visibility="collapsed",
                    key="ma_period_select"
                )
            else:  # WMA
                ma_period = st.selectbox(
                    "",
                    options=[25, 50, 100],
                    index=1,  # Default to 50
                    help="Choose the weekly moving average period for analysis",
                    label_visibility="collapsed",
                    key="wma_period_select"
                )
        
        with col3:
            st.markdown("### Historical Tracking")
            show_tracking_chart = st.checkbox(
                "Show Below 200 DMA Trend",
                value=False,
                help="Display historical chart of stocks below 200 DMA from CSV file",
                key="show_tracking_chart"
            )
    
    # Simple standalone tracking chart - completely separate section
    if show_tracking_chart:
        st.markdown("---")
        st.subheader("📊 Historical Trend: Stocks Below 200 DMA")
        
        try:
            # Read the CSV file
            df_tracking = pd.read_csv(BELOW_DMA_FILE)
            
            # Parse date with explicit format (DD-MM-YYYY)
            df_tracking['date'] = parse_mixed_dates(df_tracking['date'])
            
            # Drop any rows where date parsing failed (NaT values)
            initial_count = len(df_tracking)
            df_tracking = df_tracking.dropna(subset=['date'])
            dropped_count = initial_count - len(df_tracking)
            
            if dropped_count > 0:
                st.warning(f"⚠️ Dropped {dropped_count} rows with invalid dates")
            
            # Sort by date
            df_tracking = df_tracking.sort_values('date').reset_index(drop=True)
            
            if len(df_tracking) == 0:
                st.error("❌ No valid data found after parsing dates")
            else:
                # Show info about data
                st.info(f"📅 Data from {df_tracking['date'].min().strftime('%Y-%m-%d')} to {df_tracking['date'].max().strftime('%Y-%m-%d')} ({len(df_tracking)} total days)")
                
                # Time range selector - more prominent
                st.markdown("#### 📅 Select Time Range")
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                
                time_ranges = {'1M': 30, '3M': 90, '6M': 180, '1Y': 365, '2Y': 730, 'ALL': None}
                
                if 'tracking_range' not in st.session_state:
                    st.session_state.tracking_range = '1Y'  # Default to 1 year for better performance
                
                with col1:
                    if st.button('1M', use_container_width=True, key='tr_1m', type='primary' if st.session_state.tracking_range == '1M' else 'secondary'):
                        st.session_state.tracking_range = '1M'
                        st.rerun()
                with col2:
                    if st.button('3M', use_container_width=True, key='tr_3m', type='primary' if st.session_state.tracking_range == '3M' else 'secondary'):
                        st.session_state.tracking_range = '3M'
                        st.rerun()
                with col3:
                    if st.button('6M', use_container_width=True, key='tr_6m', type='primary' if st.session_state.tracking_range == '6M' else 'secondary'):
                        st.session_state.tracking_range = '6M'
                        st.rerun()
                with col4:
                    if st.button('1Y', use_container_width=True, key='tr_1y', type='primary' if st.session_state.tracking_range == '1Y' else 'secondary'):
                        st.session_state.tracking_range = '1Y'
                        st.rerun()
                with col5:
                    if st.button('2Y', use_container_width=True, key='tr_2y', type='primary' if st.session_state.tracking_range == '2Y' else 'secondary'):
                        st.session_state.tracking_range = '2Y'
                        st.rerun()
                with col6:
                    if st.button('ALL', use_container_width=True, key='tr_all', type='primary' if st.session_state.tracking_range == 'ALL' else 'secondary'):
                        st.session_state.tracking_range = 'ALL'
                        st.rerun()
                
                # Filter data based on selected range
                selected_range = st.session_state.tracking_range
                if selected_range == 'ALL':
                    filtered_df = df_tracking.copy()
                else:
                    days = time_ranges[selected_range]
                    cutoff = datetime.now() - timedelta(days=days)
                    filtered_df = df_tracking[df_tracking['date'] >= cutoff].copy()
                
                # Remove any NaT that might have slipped through
                filtered_df = filtered_df.dropna(subset=['date'])
                
                if len(filtered_df) == 0:
                    st.warning(f"⚠️ No data points in selected range ({selected_range})")
                else:
                    st.success(f"✅ Showing {len(filtered_df)} data points for **{selected_range}** range")
                    
                    # Prepare chart data - with explicit NaT check
                    chart_data = []
                    for _, row in filtered_df.iterrows():
                        if pd.notna(row['date']) and pd.notna(row['pct_below_200dma']):
                            chart_data.append({
                                'time': row['date'].strftime('%Y-%m-%d'),
                                'value': float(row['pct_below_200dma'])
                            })
                    
                    if len(chart_data) == 0:
                        st.error("❌ No valid chart data after filtering")
                    else:
                        # Display chart
                        renderLightweightCharts([{
                            "chart": {
                                "layout": {
                                    "background": {"type": "solid", "color": "#0e1117"},
                                    "textColor": "#d1d4dc"
                                },
                                "grid": {
                                    "vertLines": {"color": "rgba(42, 46, 57, 0.5)"},
                                    "horzLines": {"color": "rgba(42, 46, 57, 0.5)"}
                                },
                                "crosshair": {
                                    "mode": 0,
                                    "vertLine": {
                                        "color": "rgba(224, 227, 235, 0.8)",
                                        "labelBackgroundColor": "#2962FF"
                                    },
                                    "horzLine": {
                                        "color": "rgba(224, 227, 235, 0.8)",
                                        "labelBackgroundColor": "#2962FF"
                                    }
                                },
                                "timeScale": {
                                    "borderColor": "rgba(197, 203, 206, 0.4)",
                                    "timeVisible": True,
                                    "secondsVisible": False
                                },
                                "rightPriceScale": {
                                    "borderColor": "rgba(197, 203, 206, 0.4)",
                                    "scaleMargins": {"top": 0.1, "bottom": 0.1}
                                },
                                "width": 0,
                                "height": 500
                            },
                            "series": [{
                                "type": "Area",
                                "data": chart_data,
                                "options": {
                                    "topColor": "rgba(255, 82, 82, 0.56)",
                                    "bottomColor": "rgba(255, 82, 82, 0.04)",
                                    "lineColor": "rgba(255, 82, 82, 1)",
                                    "lineWidth": 2,
                                    "priceLineVisible": False,
                                    "lastValueVisible": True,
                                    "title": "% Below 200 DMA"
                                }
                            }]
                        }], f'tracking_chart_{selected_range}')
                        
                        # Summary stats
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Current %", f"{filtered_df['pct_below_200dma'].iloc[-1]:.2f}%")
                        with col2:
                            st.metric("Average %", f"{filtered_df['pct_below_200dma'].mean():.2f}%")
                        with col3:
                            st.metric("Max %", f"{filtered_df['pct_below_200dma'].max():.2f}%")
                        with col4:
                            st.metric("Min %", f"{filtered_df['pct_below_200dma'].min():.2f}%")
            
        except FileNotFoundError:
            st.warning(f"⚠️ Tracking file not found: {BELOW_DMA_FILE}")
            st.info("💡 The file should be located at: `Data/stock_data_cache/below_dma.csv`")
        except Exception as e:
            st.error(f"❌ Error loading tracking data: {str(e)}")
            import traceback
            with st.expander("🔍 Error Details"):
                st.code(traceback.format_exc())
        
        st.markdown("---")
        
        st.markdown("---")
    
    
    @st.cache_data(ttl=86400)  # Cache for 24 hours
    def load_nse_tickers():
        """Load NSE ticker symbols from CSV"""
        try:
            df = pd.read_csv(DATA_dir / "nse_tickers.csv")
            if 'symbol' in df.columns:
                symbols = df['symbol'].tolist()
                return symbols
            else:
                return []
        except FileNotFoundError:
            return []
        except Exception as e:
            return []

    def get_cache_filepath(symbol):
        """Get the filepath for a stock's cached data"""
        return os.path.join(DATA_DIR, f"{symbol}_data.pkl")

    def load_cached_data(symbol):
        """Load cached stock data from local storage"""
        filepath = get_cache_filepath(symbol)
        if os.path.exists(filepath):
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
            pass  # Silently fail if cache save fails

    def convert_daily_to_weekly(df):
        """Convert daily data to weekly data"""
        # Resample to weekly data (Friday close)
        weekly = pd.DataFrame()
        weekly['Open'] = df['Open'].resample('W-FRI').first()
        weekly['High'] = df['High'].resample('W-FRI').max()
        weekly['Low'] = df['Low'].resample('W-FRI').min()
        weekly['Close'] = df['Close'].resample('W-FRI').last()
        weekly['Volume'] = df['Volume'].resample('W-FRI').sum()
        
        # Drop NaN rows
        weekly = weekly.dropna()
        
        return weekly

    def get_stock_data_incremental(symbol, ma_period, ma_type):
        """
        Fetch stock data with FULL HISTORICAL DATA (15 years) for charts
        - Loads historical data from cache if available
        - Only fetches new data since last update
        - Combines cached + new data for complete dataset
        - Supports both DMA and WMA
        """
        try:
            ticker = f"{symbol}.NS"
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
                    # Use 'max' to get maximum available history
                    df = stock.history(period='max')
                    if not df.empty:
                        save_cached_data(symbol, df, today)
                else:
                    # Cache has good history, check if it needs daily update
                    days_old = (today - last_update).days
                    
                    if days_old == 0:
                        # Cache is from today, use it directly
                        df = cached_df
                    elif days_old <= 7:
                        # Cache is recent, fetch incremental data
                        try:
                            stock = yf.Ticker(ticker)
                            # Fetch only new data since last update
                            start_date = last_update + timedelta(days=1)
                            new_data = stock.history(start=start_date.strftime('%Y-%m-%d'))
                            
                            if not new_data.empty:
                                # Combine cached data with new data
                                df = pd.concat([cached_df, new_data])
                                # Remove duplicates based on index
                                df = df[~df.index.duplicated(keep='last')]
                                df = df.sort_index()
                                
                                # Save updated data
                                save_cached_data(symbol, df, today)
                            else:
                                # No new data, use cached
                                df = cached_df
                        except:
                            # If incremental fetch fails, use cached data
                            df = cached_df
                    else:
                        # Cache is too old, fetch fresh data with max history
                        stock = yf.Ticker(ticker)
                        df = stock.history(period='max')
                        if not df.empty:
                            save_cached_data(symbol, df, today)
            else:
                # No cache, fetch maximum available history
                stock = yf.Ticker(ticker)
                df = stock.history(period='max')
                if not df.empty:
                    save_cached_data(symbol, df, today)
            
            if df.empty:
                return None
            
            # Check minimum data requirement for daily first
            if len(df) < ma_period:
                return None
            
            # Convert to weekly if needed - ONLY for WMA mode
            if ma_type == "WMA (Weekly)":
                # Store original daily data before conversion
                df_daily = df.copy()
                df = convert_daily_to_weekly(df)
                
                if df.empty or len(df) < ma_period:
                    return None
                
                # Calculate moving averages on weekly data
                df[f'MA_{ma_period}'] = df['Close'].rolling(window=ma_period).mean()
                df['MA_50'] = df['Close'].rolling(window=50).mean()
                df['MA_20'] = df['Close'].rolling(window=20).mean()
                
                # Store daily data as attribute for charting
                df.attrs['daily_data'] = df_daily
                df.attrs['ma_type'] = ma_type
            else:
                # For DMA, calculate on daily data directly (FASTER - no copying needed)
                df[f'MA_{ma_period}'] = df['Close'].rolling(window=ma_period).mean()
                df['MA_50'] = df['Close'].rolling(window=50).mean()
                df['MA_20'] = df['Close'].rolling(window=20).mean()
                df.attrs['ma_type'] = ma_type
            
            return df
            
        except Exception as e:
            return None
        
    def convert_daily_to_weekly(df):
        """Convert daily data to weekly data - OPTIMIZED"""
        # Use agg instead of separate resample calls (single pass)
        weekly = df.resample('W-FRI').agg({
            'Open': 'first',
            'High': 'max', 
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum'
        }).dropna()
        
        return weekly
    
    
    
    def analyze_single_stock(symbol, ma_period, ma_type):
        """Analyze a single stock - helper function for parallel processing"""
        df = get_stock_data_incremental(symbol, ma_period, ma_type)
        
        if df is not None and not df[f'MA_{ma_period}'].isna().all():
            latest = df.iloc[-1]
            current_price = latest['Close']
            ma_value = latest[f'MA_{ma_period}']
            
            if pd.notna(ma_value) and current_price < ma_value:
                # Calculate percentage below MA
                pct_below = ((current_price - ma_value) / ma_value) * 100
                
                # Get additional metrics
                period_high = df['High'].tail(ma_period).max()
                period_low = df['Low'].tail(ma_period).min()
                volume = latest['Volume']
                ma_50 = df['Close'].rolling(window=50).mean().iloc[-1]
                
                ma_label = f"{ma_period} {'WMA' if ma_type == 'WMA (Weekly)' else 'DMA'}"
                
                return {
                    'Symbol': symbol,
                    'Current Price': round(current_price, 2),
                    ma_label: round(ma_value, 2),
                    f'% Below {ma_label}': round(pct_below, 2),
                    '50 MA': round(ma_50, 2) if pd.notna(ma_50) else None,
                    'Volume': int(volume),
                    f'{ma_period}-Period High': round(period_high, 2), 
                    f'{ma_period}-Period Low': round(period_low, 2),
                    'Data': df  # Store full dataframe for charting
                }
        return None

    def analyze_stocks(symbols, ma_period, ma_type, max_stocks=None):
        """Analyze stocks and find those below MA - OPTIMIZED WITH PARALLEL PROCESSING"""
        results = []
        total = len(symbols) if max_stocks is None else min(max_stocks, len(symbols))
        symbols_to_analyze = symbols[:total]
        
        # Create progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_text = st.empty()
        
        # Track cache statistics
        cache_hits = 0
        cache_misses = 0
        
        # Use ThreadPoolExecutor for parallel downloads
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(analyze_single_stock, symbol, ma_period, ma_type): symbol 
                for symbol in symbols_to_analyze
            }
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                
                # Update cache statistics (simplified for demo)
                cache_file = get_cache_filepath(symbol)
                if os.path.exists(cache_file):
                    cache_hits += 1
                else:
                    cache_misses += 1
                
                status_text.text(f"Analyzing stocks... ({completed}/{total} completed)")
                stats_text.text(f"📊 Cache hits: {cache_hits} | Cache misses: {cache_misses}")
                progress_bar.progress(completed / total)
                
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    pass
        
        progress_bar.empty()
        status_text.empty()
        stats_text.empty()
        
        # Show final cache statistics
        st.success(f"✅ Analysis complete! Cache efficiency: {cache_hits}/{total} stocks loaded from cache ({(cache_hits/total*100):.1f}%)")
        
        return results

    def create_red_gradient_style(df_results, ma_label):
        """Create a custom red gradient for % Below MA column"""
        import numpy as np
        
        # Get the % Below MA values
        pct_col = f'% Below {ma_label}'
        values = df_results[pct_col].values
        
        # Normalize values between 0 and 1
        min_val = values.min()
        max_val = values.max()
        
        # Create color map from light red to dark red
        def get_color(val):
            if min_val == max_val:
                normalized = 0.5
            else:
                normalized = (val - max_val) / (min_val - max_val)
            
            r = int(255 - (255 - 139) * normalized)
            g = int(204 - 204 * normalized)
            b = int(204 - 204 * normalized)
            
            text_color = '#ffffff' if normalized > 0.5 else '#000000'
            
            return f'background-color: rgb({r}, {g}, {b}); color: {text_color};'
        
        # Create format dict
        format_dict = {
            'Current Price': '₹{:.2f}',
            ma_label: '₹{:.2f}',
            '50 MA': '₹{:.2f}',
            pct_col: '{:.2f}%',
            'Volume': '{:,.0f}',
        }
        
        # Add format for high/low columns
        for col in df_results.columns:
            if 'High' in col or 'Low' in col:
                format_dict[col] = '₹{:.2f}'
        
        # Apply styles
        styles = df_results.style.apply(
            lambda x: [get_color(val) if x.name == pct_col else '' for val in x],
            axis=0
        ).format(format_dict)
        
        return styles

    def create_tradingview_chart(df, symbol, ma_period, ma_type, chart_key):
        """Create TradingView-style interactive chart with FULL historical data"""
        
        # ── Step 1: Get correct dataframe and calculate MAs ──────────────────────
        if ma_type == "WMA (Weekly)" and hasattr(df, 'attrs') and 'daily_data' in df.attrs:
            chart_df = df.attrs['daily_data'].copy()
            
            # Calculate daily MAs BEFORE reset_index
            chart_df['MA_20'] = chart_df['Close'].rolling(window=20).mean()
            chart_df['MA_50'] = chart_df['Close'].rolling(window=50).mean()
            
            # Strip timezone from both indexes
            weekly_index = df.index.tz_localize(None) if df.index.tzinfo else df.index
            daily_index  = chart_df.index.tz_localize(None) if chart_df.index.tzinfo else chart_df.index
            
            # Build weekly MA series
            weekly_ma = pd.Series(
                df[f'MA_{ma_period}'].values,
                index=weekly_index
            ).dropna()
            
            # Vectorized merge_asof to map weekly MA to daily dates
            daily_dates  = pd.DataFrame({'date': daily_index})
            weekly_dates = pd.DataFrame({'date': weekly_ma.index, 'wma': weekly_ma.values})
            
            merged = pd.merge_asof(
                daily_dates.sort_values('date'),
                weekly_dates.sort_values('date'),
                on='date',
                direction='backward'
            )
            chart_df[f'MA_{ma_period}_Weekly'] = merged['wma'].values

        else:
            chart_df = df.copy()
            # Ensure MA columns exist for DMA mode
            if f'MA_{ma_period}' not in chart_df.columns:
                chart_df[f'MA_{ma_period}'] = chart_df['Close'].rolling(window=ma_period).mean()
            if 'MA_50' not in chart_df.columns:
                chart_df['MA_50'] = chart_df['Close'].rolling(window=50).mean()
            if 'MA_20' not in chart_df.columns:
                chart_df['MA_20'] = chart_df['Close'].rolling(window=20).mean()

        # ── Step 2: Vectorized data preparation ──────────────────────────────────
        chart_df = chart_df.reset_index()
        date_col = chart_df.columns[0]  # handles 'Date', 'Datetime', 'index' etc.
        chart_df = chart_df.rename(columns={date_col: 'date'})
        
        # Ensure date column is proper datetime
        chart_df['date'] = pd.to_datetime(chart_df['date'])
        
        # Strip timezone
        if chart_df['date'].dt.tz is not None:
            chart_df['date'] = chart_df['date'].dt.tz_localize(None)
        
        # Unix timestamp column
        chart_df['ts'] = (chart_df['date'].astype('int64') // 10**9).astype(int)

        # Candlestick data
        candle_data = (
            chart_df[['ts', 'Open', 'High', 'Low', 'Close']]
            .rename(columns={'ts': 'time', 'Open': 'open',
                            'High': 'high', 'Low': 'low', 'Close': 'close'})
            .to_dict('records')
        )

        # Primary MA line
        ma_col = (
            f'MA_{ma_period}_Weekly'
            if ma_type == "WMA (Weekly)" and f'MA_{ma_period}_Weekly' in chart_df.columns
            else f'MA_{ma_period}'
        )
        ma_data = (
            chart_df.loc[chart_df[ma_col].notna(), ['ts', ma_col]]
            .rename(columns={'ts': 'time', ma_col: 'value'})
            .to_dict('records')
        )

        # 50 MA line
        ma50_data = []
        if 'MA_50' in chart_df.columns:
            ma50_data = (
                chart_df.loc[chart_df['MA_50'].notna(), ['ts', 'MA_50']]
                .rename(columns={'ts': 'time', 'MA_50': 'value'})
                .to_dict('records')
            )

        # 20 MA line
        ma20_data = []
        if 'MA_20' in chart_df.columns:
            ma20_data = (
                chart_df.loc[chart_df['MA_20'].notna(), ['ts', 'MA_20']]
                .rename(columns={'ts': 'time', 'MA_20': 'value'})
                .to_dict('records')
            )

        # Volume histogram
        chart_df['color'] = '#26a69a'
        chart_df.loc[chart_df['Close'] < chart_df['Open'], 'color'] = '#ef5350'
        volume_data = (
            chart_df[['ts', 'Volume', 'color']]
            .rename(columns={'ts': 'time', 'Volume': 'value'})
            .to_dict('records')
        )

        # ── Step 3: Chart config ──────────────────────────────────────────────────
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
                    "width": 1, "color": "rgba(224, 227, 235, 0.8)",
                    "style": 0, "labelBackgroundColor": "#2962FF",
                    "visible": True, "labelVisible": True,
                },
                "horzLine": {
                    "width": 1, "color": "rgba(224, 227, 235, 0.8)",
                    "style": 0, "labelBackgroundColor": "#2962FF",
                    "visible": True, "labelVisible": True,
                },
            },
            "timeScale": {
                "borderColor": "rgba(197, 203, 206, 0.4)",
                "timeVisible": True, "secondsVisible": False,
                "rightOffset": 12, "barSpacing": 10, "minBarSpacing": 0.5,
                "fixLeftEdge": False, "fixRightEdge": False,
                "lockVisibleTimeRangeOnResize": True,
                "rightBarStaysOnScroll": True,
                "borderVisible": True, "visible": True,
            },
            "rightPriceScale": {
                "borderColor": "rgba(197, 203, 206, 0.4)",
                "scaleMargins": {"top": 0.05, "bottom": 0.15},
                "autoScale": True,
            },
            "handleScroll": {
                "mouseWheel": True, "pressedMouseMove": True,
                "horzTouchDrag": True, "vertTouchDrag": False,
            },
            "handleScale": {
                "axisPressedMouseMove": True, "mouseWheel": True, "pinch": True,
            },
            "kineticScroll": {"touch": True, "mouse": False},
            "width": 0,
            "height": 800,
        }

        ma_label = f"{ma_period} {'WMA' if ma_type == 'WMA (Weekly)' else 'DMA'}"

        # ── Step 4: Build series ──────────────────────────────────────────────────
        series = [
            {
                "type": "Candlestick",
                "data": candle_data,
                "options": {
                    "upColor": "#26a69a", "downColor": "#ef5350",
                    "borderVisible": False,
                    "wickUpColor": "#26a69a", "wickDownColor": "#ef5350",
                    "priceScaleId": "right", "title": symbol,
                }
            },
            {
                "type": "Line",
                "data": ma_data,
                "options": {
                    "color": "#ffa500", "lineWidth": 2,
                    "title": ma_label, "priceScaleId": "right",
                    "lastValueVisible": True, "priceLineVisible": True,
                }
            }
        ]

        if ma50_data:
            series.append({
                "type": "Line", "data": ma50_data,
                "options": {
                    "color": "#00bfff", "lineWidth": 1.5, "lineStyle": 2,
                    "title": "50 DMA", "priceScaleId": "right",
                    "lastValueVisible": True, "priceLineVisible": False,
                }
            })

        if ma20_data:
            series.append({
                "type": "Line", "data": ma20_data,
                "options": {
                    "color": "#9c27b0", "lineWidth": 1, "lineStyle": 2,
                    "title": "20 DMA", "priceScaleId": "right",
                    "lastValueVisible": False, "priceLineVisible": False,
                }
            })

        series.append({
            "type": "Histogram", "data": volume_data,
            "options": {
                "color": "#26a69a",
                "priceFormat": {"type": "volume"},
                "priceScaleId": "volume",
                "scaleMargins": {"top": 0.7, "bottom": 0},
            }
        })

        # ── Step 5: Render ────────────────────────────────────────────────────────
        renderLightweightCharts([{"chart": chart_options, "series": series}], chart_key)

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
        
        # Get the chart dataframe (daily data for display)
        if hasattr(df, 'attrs') and 'daily_data' in df.attrs:
            chart_df = df.attrs['daily_data']
        else:
            chart_df = df
        
        # Filter data based on range
        end_date = chart_df.index[-1]
        
        if selected_range != 'ALL':
            days = ranges[selected_range]
            start_date = end_date - timedelta(days=days)
            # Make sure start_date is not before the first available date
            if start_date < chart_df.index[0]:
                start_date = chart_df.index[0]
            filtered_df = chart_df[chart_df.index >= start_date]
        else:
            filtered_df = chart_df
            start_date = chart_df.index[0]
        
        # Custom date range
        with st.expander("📅 Custom Date Range"):
            col1, col2 = st.columns(2)
            
            # Ensure the default values are within the available data range
            default_start = start_date.date() if start_date >= chart_df.index[0] else chart_df.index[0].date()
            
            with col1:
                custom_start = st.date_input(
                    "Start Date",
                    value=default_start,
                    min_value=chart_df.index[0].date(),
                    max_value=chart_df.index[-1].date(),
                    key=f'start_{symbol}'
                )
            with col2:
                custom_end = st.date_input(
                    "End Date",
                    value=end_date.date(),
                    min_value=chart_df.index[0].date(),
                    max_value=chart_df.index[-1].date(),
                    key=f'end_{symbol}'
                )
            
            if st.button("Apply Custom Range", key=f'apply_{symbol}'):
                if custom_start <= custom_end:
                    filtered_df = chart_df[(chart_df.index.date >= custom_start) & (chart_df.index.date <= custom_end)]
                    st.success(f"✅ Range: {custom_start} to {custom_end}")
                else:
                    st.error("❌ Invalid date range")
        
        return filtered_df, selected_range

    def display_chart_stats(df):
        """Display current price statistics"""
        # Use daily data if available
        if hasattr(df, 'attrs') and 'daily_data' in df.attrs:
            display_df = df.attrs['daily_data']
        else:
            display_df = df
            
        latest = display_df.iloc[-1]
        previous = display_df.iloc[-2] if len(display_df) > 1 else latest
        
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
        
        # Use daily data if available
        if hasattr(df, 'attrs') and 'daily_data' in df.attrs:
            display_df = df.attrs['daily_data']
        else:
            display_df = df
        
        st.markdown("#### 📊 Period Analysis")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            period_high = display_df['High'].max()
            period_low = display_df['Low'].min()
            current_price = display_df['Close'].iloc[-1]
            
            range_position = ((current_price - period_low) / (period_high - period_low)) * 100
            
            st.metric("Period High", f"₹{period_high:.2f}")
            st.metric("Period Low", f"₹{period_low:.2f}")
            st.metric("Range Position", f"{range_position:.1f}%")
        
        with col2:
            avg_volume = display_df['Volume'].mean()
            current_volume = display_df['Volume'].iloc[-1]
            volume_ratio = (current_volume / avg_volume) * 100
            
            st.metric("Avg Volume", f"{avg_volume:,.0f}")
            st.metric("Current Volume", f"{current_volume:,.0f}")
            st.metric("Volume Ratio", f"{volume_ratio:.1f}%")
        
        with col3:
            volatility = display_df['Close'].pct_change().std() * 100
            avg_daily_range = ((display_df['High'] - display_df['Low']) / display_df['Low']).mean() * 100
            
            st.metric("Volatility (Std)", f"{volatility:.2f}%")
            st.metric("Avg Daily Range", f"{avg_daily_range:.2f}%")
        
        with col4:
            # Price change over period
            period_start = display_df['Close'].iloc[0]
            period_end = display_df['Close'].iloc[-1]
            period_change = ((period_end - period_start) / period_start) * 100
            
            st.metric("Period Change", f"{period_change:.2f}%")
            st.metric("Total Days", f"{len(display_df)}")

    def clear_local_cache():
        """Clear all locally cached stock data"""
        import shutil
        if os.path.exists(DATA_DIR):
            shutil.rmtree(DATA_DIR)
            os.makedirs(DATA_DIR, exist_ok=True)
            return True
        return False

    def get_cache_info():
        """Get information about local cache"""
        if not os.path.exists(DATA_DIR):
            return 0, 0
        
        files = [f for f in os.listdir(DATA_DIR) if f.endswith('.pkl')]
        total_size = sum(os.path.getsize(os.path.join(DATA_DIR, f)) for f in files)
        return len(files), total_size / (1024 * 1024)  # Size in MB

    # Load tickers
    symbols = load_nse_tickers()

    if not symbols:
        st.error("❌ nse_tickers.csv not found or invalid. Please ensure the file exists with a 'symbol' column.")
    else:
        # Display cache information in expander
        cached_stocks, cache_size_mb = get_cache_info()
        
        with col2:
            st.markdown("### Loaded Data")
            st.success(f"✅ Loaded {len(symbols)} NSE tickers")

        
        with col3:
            st.markdown("### Analysis Parameters")
            max_stocks = st.number_input(
                "Max stocks to analyze", 
                min_value=10, 
                max_value=len(symbols), 
                value=min(100, len(symbols)),
                step=10,
                help="With local caching, analyzing more stocks is much faster!",
                key="max_stocks_input"
            )

            ma_label = f"{ma_period} {'WMA' if ma_type == 'WMA (Weekly)' else 'DMA'}"
            min_pct_below = st.slider(
                f"Min % below {ma_label}",
                min_value=-50.0,
                max_value=0.0,
                value=-20.0,
                step=1.0,
                help=f"Filter stocks that are at least this % below their {ma_label}",
                key="min_pct_slider"
            )
        
        # Cache Management
        st.markdown("### 🗄️ Cache Management")
        col_cache1, col_cache2, col_cache3 = st.columns([1, 1, 2])
        
        with col_cache1:
            if st.button("🔄 Refresh Data", key="refresh_data_btn"):
                st.cache_data.clear()
                st.success("Session cache cleared!")
        
        with col_cache2:
            if st.button("🗑️ Clear All Cache", key="clear_cache_btn"):
                if clear_local_cache():
                    st.cache_data.clear()
                    st.success("All caches cleared!")
                else:
                    st.warning("No cache to clear")
        
        with col_cache3:
            # Analysis button
            analyze_button = st.button("🔍 Analyze Stocks", type="primary", use_container_width=True, key="analyze_btn")
        
        # Optimizations info
        st.markdown("### ⚡ Optimizations:")
        opt_col1, opt_col2 = st.columns(2)
        with opt_col1:
            st.markdown("""
            * ✅ Parallel processing
            * ✅ Local caching
            * ✅ Incremental updates
            * ✅ Historical Data
            """)
        with opt_col2:
            st.markdown(f"""
            * ✅ TradingView charts
            * ✅ MA Type: {ma_type}
            * ✅ MA Period: {ma_period}
            """)
        
        st.markdown("### 📊 Chart Features:")
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.markdown("""
            * 🎯 Zoom in/out
            * 🎯 Drag scrolling
            """)
        with chart_col2:
            st.markdown("""
            * 🎯 Time ranges (1M-15Y)
            * 🎯 Multiple MAs
            """)
    
    st.markdown("---")

    if not symbols:
        pass  # Error already shown above
    else:
        # Run analysis
        if analyze_button:
            with st.spinner(f"Analyzing stocks with {ma_label}..."):
                results = analyze_stocks(symbols, ma_period, ma_type, int(max_stocks))
                st.session_state['results'] = results
                st.session_state['ma_period'] = ma_period
                st.session_state['ma_type'] = ma_type
                st.session_state['analysis_done'] = True

        # Display results
        if (st.session_state.get('results') is not None and 
            st.session_state.get('ma_period') == ma_period and
            st.session_state.get('ma_type') == ma_type):
            
            results = st.session_state['results']
            
            # Filter by minimum percentage
            ma_label = f"{ma_period} {'WMA' if ma_type == 'WMA (Weekly)' else 'DMA'}"
            pct_col = f'% Below {ma_label}'
            filtered_results = [r for r in results if r[pct_col] <= min_pct_below]
            
            # Summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Analyzed", int(max_stocks))
            
            with col2:
                st.metric(f"Below {ma_label}", len(filtered_results))
            
            with col3:
                if filtered_results:
                    avg_below = sum(r[pct_col] for r in filtered_results) / len(filtered_results)
                    st.metric("Avg % Below", f"{avg_below:.2f}%")
                else:
                    st.metric("Avg % Below", "N/A")
            
            with col4:
                if filtered_results:
                    most_below = min(r[pct_col] for r in filtered_results)
                    st.metric("Most Below", f"{most_below:.2f}%")
                else:
                    st.metric("Most Below", "N/A")
            
            st.markdown("---")
            
            # Display results table
            if filtered_results:
                st.subheader(f"📊 {len(filtered_results)} Stocks Below {ma_label}")
                
                # Create DataFrame without the 'Data' column for display
                display_data = [{k: v for k, v in r.items() if k != 'Data'} for r in filtered_results]
                df_results = pd.DataFrame(display_data)
                
                # Sort by % Below MA
                df_results = df_results.sort_values(pct_col)
                
                # Apply custom red gradient styling
                styled_df = create_red_gradient_style(df_results, ma_label)
                
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=400
                )
                
                # Color legend
                st.markdown("""
                <div style='padding: 10px; background-color: #262730; border-radius: 5px; margin: 10px 0;'>
                    <b style='color: #ffffff;'>Color Legend:</b> 
                    <span style='color: #ffcccc;'>■</span> <span style='color: #ffffff;'>Light Red (-10% to -20%)</span> → 
                    <span style='color: #ff6666;'>■</span> <span style='color: #ffffff;'>Medium Red (-30% to -40%)</span> → 
                    <span style='color: #8b0000;'>■</span> <span style='color: #ffffff;'>Dark Red (-50% or lower)</span>
                </div>
                """, unsafe_allow_html=True)
                
                # Download button
                csv = df_results.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results as CSV",
                    data=csv,
                    file_name=f"stocks_below_{ma_label.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
                
                st.markdown("---")
                
                # Detailed view section with TradingView charts
                st.subheader("📈 Interactive TradingView Chart")
                st.markdown(f"**Full Historical Data (15 Years) - Zoom & Pan Freely - {ma_type}**")
                
                # Create a selectbox for choosing stocks to view
                stock_symbols = [r['Symbol'] for r in filtered_results]
                selected_stock = st.selectbox(
                    "Select a stock to view chart:",
                    stock_symbols,
                    help="Choose a stock to analyze with full historical data"
                )
                
                if selected_stock:
                    # Find the stock data
                    stock_data = next((r for r in filtered_results if r['Symbol'] == selected_stock), None)
                    
                    if stock_data and 'Data' in stock_data:
                        df_full = stock_data['Data']
                        
                        # Display current stats
                        st.markdown("#### 📊 Current Statistics")
                        display_chart_stats(df_full)
                        
                        st.markdown("---")
                        
                        # Chart controls - Full width
                        filtered_df, selected_range = create_chart_controls(df_full, selected_stock)
                        
                        st.markdown("---")
                        
                        # Render FULL SCREEN chart - No columns, maximum height
                        st.markdown(f"### 📊 {selected_stock} - Price Chart ({selected_range})")
                        
                        # Get chart dataframe
                        if hasattr(df_full, 'attrs') and 'daily_data' in df_full.attrs:
                            chart_info_df = df_full.attrs['daily_data']
                        else:
                            chart_info_df = df_full
                        
                        st.markdown(f"**{len(chart_info_df)} days of historical data available | Showing {len(filtered_df)} days**")
                        
                        # Create full-width chart with maximum height
                        create_tradingview_chart(
                            df_full,
                            selected_stock,
                            ma_period,
                            ma_type,
                            f'chart_{selected_stock}_{selected_range}'
                        )
                        
                        st.markdown("---")
                        
                        # Period analysis below the chart
                        display_period_analysis(df_full, selected_stock)
                        
            else:
                st.warning("No stocks found below the specified threshold. Try adjusting the filter.")
        
        elif ((st.session_state.get('ma_period') != ma_period or 
               st.session_state.get('ma_type') != ma_type) and 
              st.session_state.get('results') is not None):
            st.info(f"⚠️ MA settings changed to {ma_type} with period {ma_period}. Click 'Analyze Stocks' to re-run analysis with new settings.")


# =============================================================================
# TAB 2: INDEX RATIO ANALYSIS (ENHANCED WITH MONETARY DATA SUPPORT)
# =============================================================================

elif st.session_state.active_tab == "index_ratio":
    st.header("Index Ratio Analysis")
    st.markdown("Calculate and visualize historical ratios between NSE indices and monetary aggregates")
    
    # Sidebar for index ratio settings
    st.sidebar.markdown("---")
    st.sidebar.header("📊 Index Ratio Settings")
    
    @st.cache_data(ttl=3600)
    def load_monetary_columns(file_path):
        """Load available columns from monetary data file with better error handling"""
        try:
            file_path_obj = Path(file_path)
            
            # Check if file exists
            if not file_path_obj.exists():
                st.warning(f"⚠️ Monetary data file not found: {file_path}")
                return []
            
            # Check file permissions
            if not os.access(file_path, os.R_OK):
                st.error(f"❌ No read permission for: {file_path}")
                st.info("💡 Tip: Close the Excel file if it's open and try again")
                return []
            
            # Try reading with different engines
            df = None
            errors = []
            
            # Try openpyxl first (default for .xlsx)
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e1:
                errors.append(f"openpyxl: {str(e1)}")
                
                # Try xlrd as fallback
                try:
                    df = pd.read_excel(file_path, engine='xlrd')
                except Exception as e2:
                    errors.append(f"xlrd: {str(e2)}")
            
            if df is None:
                st.error("❌ Failed to read monetary data file")
                with st.expander("Show error details"):
                    for err in errors:
                        st.write(f"• {err}")
                st.info("💡 **Possible solutions:**\n"
                       "1. Close the Excel file if it's open\n"
                       "2. Check file permissions\n"
                       "3. Try saving as .csv instead\n"
                       "4. Install/update openpyxl: `pip install openpyxl --upgrade`")
                return []
            
            # Get columns excluding the date column
            date_keywords = ['year/month', 'date', 'timestamp', 'month', 'year']
            columns = [col for col in df.columns 
                      if col.lower() not in date_keywords and not col.lower().startswith('unnamed')]
            
            return columns
            
        except Exception as e:
            st.error(f"Error loading monetary data columns: {e}")
            st.info("💡 **Try this:**\n"
                   "1. Close the Excel file\n"
                   "2. Make sure the file path is correct\n"
                   "3. Check file permissions")
            return []
    
    @st.cache_data(ttl=3600)
    def load_index_data(index_name, directory):
        """Load data for a specific index"""
        try:
            file_path = Path(directory) / f"{index_name}.csv"
            
            if not file_path.exists():
                return None
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if required columns exist
            required_cols = ['TIMESTAMP', 'CLOSE_INDEX_VAL']
            if not all(col in df.columns for col in required_cols):
                st.error(f"Missing required columns in {index_name}.csv")
                return None
            
            # Convert timestamp to datetime
            df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
            
            # Sort by timestamp
            df = df.sort_values('TIMESTAMP')
            
            # Set timestamp as index
            df.set_index('TIMESTAMP', inplace=True)
            
            # Rename for consistency
            df = df.rename(columns={'CLOSE_INDEX_VAL': 'VALUE'})
            
            return df[['VALUE']]
            
        except Exception as e:
            st.error(f"Error loading {index_name}: {e}")
            return None
    
    def parse_year_month_date(date_str):
        """Parse dates in 'Apr-21', 'May-21' format to datetime"""
        try:
            # Handle string input
            if isinstance(date_str, str):
                # Split by dash
                parts = date_str.split('-')
                if len(parts) == 2:
                    month_str, year_str = parts
                    
                    # Convert month abbreviation to number
                    month_map = {
                        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12,
                        'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
                        'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
                    }
                    
                    month_num = month_map.get(month_str, None)
                    if month_num is None:
                        return None
                    
                    # Convert year (21 -> 2021)
                    year_num = int(year_str)
                    if year_num < 100:
                        # Assume 2000s for years < 100
                        year_num = 2000 + year_num
                    
                    # Create datetime (use first day of month)
                    return pd.Timestamp(year=year_num, month=month_num, day=1)
            
            # If already a datetime, return as is
            if isinstance(date_str, (pd.Timestamp, datetime)):
                return date_str
            
            return None
            
        except Exception as e:
            return None
    
    @st.cache_data(ttl=3600)
    def load_monetary_data(file_path, column_name):
        """Load data for a specific monetary column with robust date parsing"""
        try:
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                st.error(f"File not found: {file_path}")
                return None
            
            # Try reading with different engines
            df = None
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e1:
                try:
                    df = pd.read_excel(file_path, engine='xlrd')
                except Exception as e2:
                    st.error(f"Failed to read Excel file: {e2}")
                    st.info("💡 Close the Excel file if it's open and try again")
                    return None
            
            if df is None:
                return None
            
            # Find the date column (case-insensitive)
            date_col = None
            date_keywords = ['year/month', 'date', 'timestamp', 'month', 'year', 'yearmonth']
            
            for col in df.columns:
                if col.lower().replace('/', '').replace(' ', '') in [k.replace('/', '').replace(' ', '') for k in date_keywords]:
                    date_col = col
                    break
            
            if date_col is None:
                st.error("❌ No date column found in monetary data")
                st.write("**Available columns:**", df.columns.tolist())
                st.info("💡 Looking for columns like: Year/Month, Date, Timestamp")
                return None
            
            st.success(f"✅ Found date column: '{date_col}'")
            
            # Check if the requested column exists
            if column_name not in df.columns:
                st.error(f"❌ Column '{column_name}' not found in monetary data")
                st.write("**Available columns:**", df.columns.tolist())
                return None
            
            # Select only date and value columns
            df = df[[date_col, column_name]].copy()
            
            # Remove rows with NaN values in either column
            df = df.dropna()
            
            # Parse dates using custom function
            st.write("**Parsing dates...**")
            df['parsed_date'] = df[date_col].apply(parse_year_month_date)
            
            # Check how many dates were successfully parsed
            parsed_count = df['parsed_date'].notna().sum()
            total_count = len(df)
            
            st.write(f"**Parsed {parsed_count} out of {total_count} dates**")
            
            if parsed_count == 0:
                st.error("❌ Failed to parse any dates!")
                st.write("**Sample date values:**", df[date_col].head(10).tolist())
                
                # Try fallback parsing methods
                st.write("**Trying alternative date parsing methods...**")
                
                # Method 1: pandas to_datetime with various formats
                for fmt in ['%b-%y', '%B-%y', '%m-%y', '%b-%Y', '%B-%Y']:
                    try:
                        df['parsed_date'] = pd.to_datetime(df[date_col], format=fmt)
                        if df['parsed_date'].notna().sum() > 0:
                            st.success(f"✅ Successfully parsed with format: {fmt}")
                            break
                    except:
                        continue
                
                # Method 2: Try inferring the format
                if df['parsed_date'].isna().all():
                    try:
                        df['parsed_date'] = pd.to_datetime(df[date_col], infer_datetime_format=True)
                        if df['parsed_date'].notna().sum() > 0:
                            st.success("✅ Successfully parsed with inferred format")
                    except Exception as e:
                        st.error(f"All parsing methods failed: {e}")
                        return None
            
            # Remove rows where date parsing failed
            df = df[df['parsed_date'].notna()].copy()
            
            if df.empty:
                st.error("❌ No valid dates after parsing")
                return None
            
            # Set parsed date as index
            df.set_index('parsed_date', inplace=True)
            df.index.name = 'Date'
            
            # Sort by date
            df = df.sort_index()
            
            # Rename column for consistency
            df = df.rename(columns={column_name: 'VALUE'})
            
            # Keep only the VALUE column
            df = df[['VALUE']]
            
            # Convert to numeric if needed
            if not pd.api.types.is_numeric_dtype(df['VALUE']):
                st.write("**Converting values to numeric...**")
                df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
                df = df.dropna()
            
            st.success(f"✅ Successfully loaded {len(df)} data points for '{column_name}'")
            st.write(f"**Date range:** {df.index.min().strftime('%Y-%m-%d')} to {df.index.max().strftime('%Y-%m-%d')}")
            
            return df
            
        except Exception as e:
            st.error(f"❌ Error loading monetary data: {e}")
            import traceback
            with st.expander("Show detailed error"):
                st.code(traceback.format_exc())
            return None
        
    @st.cache_data(ttl=3600)
    def load_global_index_data(index_name, directory):
        """Load data for a specific global index/ETF (supports multiple formats)"""
        try:
            file_path = Path(directory) / f"{index_name}.csv"
            
            if not file_path.exists():
                return None
            
            df = pd.read_csv(file_path)
            
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()

            # -------------------------
            # 1️⃣ Detect Date Column
            # -------------------------
            DATE_KEYWORDS = ['timestamp', 'date', 'datetime', 'time', 'DATE', 'TIMESTAMP', 'Date', 'Timestamp']
            
            date_col = None
            for col in df.columns:
                if col.strip().lower() in DATE_KEYWORDS:
                    date_col = col
                    break
            
            # Fallback: pick first column that looks like dates
            if date_col is None:
                for col in df.columns:
                    sample = df[col].dropna().head(5)
                    try:
                        pd.to_datetime(sample, dayfirst=True)
                        date_col = col
                        break
                    except:
                        continue
            
            if date_col is None:
                st.error(f"No date column found in {index_name}.csv. Columns: {df.columns.tolist()}")
                return None

            # -------------------------
            # 2️⃣ Detect Price Column
            # -------------------------
            PRICE_KEYWORDS = ['close', 'price', 'adj close', 'adj_close', 'closing', 
                            'last', 'settlement', 'value', 'nav', 'rate']
            
            value_col = None
            # First: exact match (case-insensitive)
            for col in df.columns:
                if col.strip().lower() in PRICE_KEYWORDS:
                    value_col = col
                    break
            
            # Fallback: partial match
            if value_col is None:
                for col in df.columns:
                    for keyword in PRICE_KEYWORDS:
                        if keyword in col.strip().lower():
                            value_col = col
                            break
                    if value_col:
                        break
            
            # Last resort: first numeric column that isn't the date column
            if value_col is None:
                for col in df.columns:
                    if col == date_col:
                        continue
                    cleaned = (
                        df[col].astype(str)
                        .str.replace(',', '', regex=False)
                        .str.strip()
                    )
                    numeric = pd.to_numeric(cleaned, errors='coerce')
                    if numeric.notna().sum() > len(df) * 0.8:  # >80% valid numbers
                        value_col = col
                        break
            
            if value_col is None:
                st.error(f"No price column found in {index_name}.csv. Columns: {df.columns.tolist()}")
                return None

            # -------------------------
            # 3️⃣ Convert Date
            # -------------------------
            df[date_col] = pd.to_datetime(
                df[date_col],
                dayfirst=True,
                infer_datetime_format=True,
                errors='coerce'
            )

            df = df.dropna(subset=[date_col])
            df = df.sort_values(date_col)
            df.set_index(date_col, inplace=True)
            df.index.name = 'Date'

            # -------------------------
            # 4️⃣ Clean Numeric Values
            # -------------------------
            df[value_col] = (
                df[value_col]
                .astype(str)
                .str.replace(',', '', regex=False)
                .str.replace('$', '', regex=False)
                .str.replace('%', '', regex=False)
                .str.strip()
            )
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
            df = df.dropna(subset=[value_col])

            # -------------------------
            # 5️⃣ Standardize Column Name
            # -------------------------
            df = df.rename(columns={value_col: 'VALUE'})

            return df[['VALUE']]

        except Exception as e:
            st.error(f"Error loading global index {index_name}: {e}")
            import traceback
            with st.expander("Show detailed error"):
                st.code(traceback.format_exc())
            return None

    
    def calculate_ratio(df1, df2, name1, name2):
        """Calculate ratio between two datasets"""
        try:
            # Show date ranges for debugging
            st.write(f"**{name1} date range:** {df1.index.min()} to {df1.index.max()}")
            st.write(f"**{name2} date range:** {df2.index.min()} to {df2.index.max()}")
            
            # Merge on timestamp (inner join to get common dates)
            merged = pd.merge(
                df1, 
                df2, 
                left_index=True, 
                right_index=True, 
                suffixes=('_1', '_2'),
                how='inner'
            )
            
            st.write(f"**Found {len(merged)} overlapping dates**")
            
            if merged.empty:
                st.error("❌ No overlapping dates found between the two series")
                st.info("💡 The date ranges don't overlap. Try adjusting the date range.")
                return None
            
            # Calculate ratio
            merged['RATIO'] = merged['VALUE_1'] / merged['VALUE_2']
            
            # Rename columns for clarity
            merged = merged.rename(columns={
                'VALUE_1': name1,
                'VALUE_2': name2
            })
            
            st.success(f"✅ Successfully calculated ratio for {len(merged)} data points")
            
            return merged
            
        except Exception as e:
            st.error(f"❌ Error calculating ratio: {e}")
            import traceback
            with st.expander("Show detailed error"):
                st.code(traceback.format_exc())
            return None
    
    def plot_ratio_chart_enhanced(ratio_df, name1, name2):
        """Create enhanced interactive chart for ratio with statistics"""
        fig = go.Figure()
        
        # Calculate statistics
        mean_ratio = ratio_df['RATIO'].mean()
        std_ratio = ratio_df['RATIO'].std()
        
        # Add ratio line
        fig.add_trace(go.Scatter(
            x=ratio_df.index,
            y=ratio_df['RATIO'],
            mode='lines',
            name=f'{name1} / {name2}',
            line=dict(color='#00bfff', width=2.5)
        ))
        
        # Add mean line
        fig.add_hline(
            y=mean_ratio,
            line_dash="dash",
            line_color="#ffa500",
            annotation_text=f"Mean: {mean_ratio:.4f}",
            annotation_position="right"
        )
        
        # Add +1 std deviation
        fig.add_hline(
            y=mean_ratio + std_ratio,
            line_dash="dot",
            line_color="#ff4b4b",
            annotation_text=f"+1 SD: {mean_ratio + std_ratio:.4f}",
            annotation_position="right"
        )
        
        # Add -1 std deviation
        fig.add_hline(
            y=mean_ratio - std_ratio,
            line_dash="dot",
            line_color="#00ff00",
            annotation_text=f"-1 SD: {mean_ratio - std_ratio:.4f}",
            annotation_position="right"
        )
        
        # Add +2 std deviation
        fig.add_hline(
            y=mean_ratio + (2 * std_ratio),
            line_dash="dot",
            line_color="#ff0000",
            opacity=0.5,
            annotation_text=f"+2 SD: {mean_ratio + (2 * std_ratio):.4f}",
            annotation_position="right",
            annotation_font_size=10
        )
        
        # Add -2 std deviation
        fig.add_hline(
            y=mean_ratio - (2 * std_ratio),
            line_dash="dot",
            line_color="#00ff00",
            opacity=0.5,
            annotation_text=f"-2 SD: {mean_ratio - (2 * std_ratio):.4f}",
            annotation_position="right",
            annotation_font_size=10
        )
        
        # Dark theme layout
        fig.update_layout(
            title=f'Ratio: {name1} / {name2}',
            yaxis_title='Ratio Value',
            xaxis_title='Date',
            template='plotly_dark',
            height=600,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(38, 39, 48, 0.8)'
            )
        )
        
        return fig
    
    def plot_dual_chart(ratio_df, name1, name2):
        """Create chart showing both series on separate y-axes"""
        fig = go.Figure()
        
        # Add first series
        fig.add_trace(go.Scatter(
            x=ratio_df.index,
            y=ratio_df[name1],
            mode='lines',
            name=name1,
            line=dict(color='#00bfff', width=2),
            yaxis='y'
        ))
        
        # Add second series
        fig.add_trace(go.Scatter(
            x=ratio_df.index,
            y=ratio_df[name2],
            mode='lines',
            name=name2,
            line=dict(color='#ffa500', width=2),
            yaxis='y2'
        ))
        
        # Dark theme layout with dual y-axes
        fig.update_layout(
            title=f'{name1} vs {name2}',
            xaxis_title='Date',
            yaxis=dict(
                title=dict(text=name1, font=dict(color='#00bfff')),
                tickfont=dict(color='#00bfff')
            ),
            yaxis2=dict(
                title=dict(text=name2, font=dict(color='#ffa500')),
                tickfont=dict(color='#ffa500'),
                overlaying='y',
                side='right'
            ),
            template='plotly_dark',
            height=500,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(38, 39, 48, 0.8)'
            )
        )
        
        return fig
    
    # Load available data sources
    available_indices = load_available_indices(INDEX_DATA_DIR)
    monetary_columns = load_monetary_columns(MONETARY_DATA_FILE)
    available_global_indices = load_available_indices(GLOBAL_DATA_DIR)

    
    # Check what data is available
    has_indices = len(available_indices) > 0
    has_global = len(available_global_indices) > 0
    has_monetary = len(monetary_columns) > 0

    
    if not has_indices and not has_monetary:
        st.error("❌ No data sources found. Please check file paths.")
        st.info("💡 Use the 'File Diagnostics' section above to troubleshoot")
    else:
        # Show available data sources
        col1, col2 = st.columns(2)
        with col1:
            if has_indices:
                st.success(f"✅ Found {len(available_indices)} index files")
            else:
                st.warning("⚠️ No index files found")
        with col2:
            if has_monetary:
                st.success(f"✅ Found {len(monetary_columns)} monetary columns")
            else:
                st.warning("⚠️ No monetary data loaded")
        
        # Display available data in expanders
        if has_indices:
            with st.expander("📋 Available NSE Indices"):
                cols = st.columns(3)
                for idx, index_name in enumerate(available_indices):
                    with cols[idx % 3]:
                        st.write(f"• {index_name}")
        
        if has_monetary:
            with st.expander("💰 Available Monetary Data"):
                cols = st.columns(3)
                for idx, col_name in enumerate(monetary_columns):
                    with cols[idx % 3]:
                        st.write(f"• {col_name}")

        if has_global:
            with st.expander("🌍 Available Global Indices / ETFs"):
                cols = st.columns(3)
                for idx, index_name in enumerate(available_global_indices):
                    with cols[idx % 3]:
                        st.write(f"• {index_name}")

        
        # Only proceed if we have at least one data source
        if not has_indices and not has_monetary:
            st.stop()
        
        st.markdown("---")
        
        # Create combined options for selection
        all_options = []

        if has_indices:
            all_options.extend([f"NSE INDEX: {idx}" for idx in available_indices])

        if has_global:
            all_options.extend([f"GLOBAL: {idx}" for idx in available_global_indices])

        if has_monetary:
            all_options.extend([f"MONETARY: {col}" for col in monetary_columns])

        
        if len(all_options) < 2:
            st.warning("⚠️ Need at least 2 data series to calculate ratio")
            st.stop()
        
        # Data selection
        st.subheader("🔢 Select Data Series")
        
        col1, col2 = st.columns(2)
        
        with col1:
            series1 = st.selectbox(
                "Select Numerator (Series 1)",
                all_options,
                key='series1'
            )
        
        with col2:
            # Filter out the selected series1
            series2_options = [opt for opt in all_options if opt != series1]
            series2 = st.selectbox(
                "Select Denominator (Series 2)",
                series2_options,
                key='series2'
            )
        
        # Date range selection
        st.markdown("### 📅 Date Range")
        col1, col2 = st.columns(2)
        
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=pd.to_datetime("2020-01-01"),
                key='start_date'
            )
        
        with col2:
            end_date = st.date_input(
                "End Date",
                value=pd.to_datetime("today"),
                key='end_date'
            )
        
        # Calculate button
        if st.button("📊 Calculate Ratio", type="primary"):
            with st.spinner(f"Loading data and calculating ratio..."):
                # Parse series selections
                # Parse series selections
                type1, name1 = series1.split(": ", 1)
                type2, name2 = series2.split(": ", 1)

                # Load series 1
                if type1 == "NSE INDEX":
                    df1 = load_index_data(name1, INDEX_DATA_DIR)

                elif type1 == "GLOBAL":
                    df1 = load_global_index_data(name1, GLOBAL_DATA_DIR)

                else:  # MONETARY
                    df1 = load_monetary_data(MONETARY_DATA_FILE, name1)

                # Load series 2
                if type2 == "NSE INDEX":
                    df2 = load_index_data(name2, INDEX_DATA_DIR)

                elif type2 == "GLOBAL":
                    df2 = load_global_index_data(name2, GLOBAL_DATA_DIR)

                else:  # MONETARY
                    df2 = load_monetary_data(MONETARY_DATA_FILE, name2)

                
                if df1 is not None and df2 is not None:
                    # Filter by date range
                    st.write(f"**Filtering data to date range: {start_date} to {end_date}**")
                    df1_filtered = df1.loc[start_date:end_date]
                    df2_filtered = df2.loc[start_date:end_date]
                    
                    if df1_filtered.empty or df2_filtered.empty:
                        st.error("❌ No data available for the selected date range.")
                        if df1_filtered.empty:
                            st.write(f"**{name1}** has no data in this range")
                        if df2_filtered.empty:
                            st.write(f"**{name2}** has no data in this range")
                    else:
                        # Calculate ratio
                        ratio_df = calculate_ratio(df1_filtered, df2_filtered, name1, name2)
                        
                        if ratio_df is not None and not ratio_df.empty:
                            # Store in session state
                            st.session_state['ratio_df'] = ratio_df
                            st.session_state['name1'] = name1
                            st.session_state['name2'] = name2
                            st.session_state['type1'] = type1
                            st.session_state['type2'] = type2
                            
                            st.success(f"✅ Calculated ratio for {len(ratio_df)} data points")
                        else:
                            st.error("❌ Failed to calculate ratio.")
                else:
                    st.error("❌ Failed to load one or both data series.")
                    if df1 is None:
                        st.write(f"**Failed to load:** {name1}")
                    if df2 is None:
                        st.write(f"**Failed to load:** {name2}")
        
        # Display results if available
        if 'ratio_df' in st.session_state:
            ratio_df = st.session_state['ratio_df']
            name1 = st.session_state['name1']
            name2 = st.session_state['name2']
            type1 = st.session_state.get('type1', 'INDEX')
            type2 = st.session_state.get('type2', 'INDEX')
            
            st.markdown("---")
            
            # Summary statistics
            st.subheader("📈 Ratio Statistics")
            
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Current Ratio", f"{ratio_df['RATIO'].iloc[-1]:.4f}")
            
            with col2:
                st.metric("Mean Ratio", f"{ratio_df['RATIO'].mean():.4f}")
            
            with col3:
                st.metric("Std Deviation", f"{ratio_df['RATIO'].std():.4f}")
            
            with col4:
                st.metric("Max Ratio", f"{ratio_df['RATIO'].max():.4f}")
            
            with col5:
                st.metric("Min Ratio", f"{ratio_df['RATIO'].min():.4f}")
            
            # Z-score (current position relative to mean)
            current_ratio = ratio_df['RATIO'].iloc[-1]
            mean_ratio = ratio_df['RATIO'].mean()
            std_ratio = ratio_df['RATIO'].std()
            z_score = (current_ratio - mean_ratio) / std_ratio
            
            st.markdown("---")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric(
                    "Z-Score", 
                    f"{z_score:.2f}",
                    help="Number of standard deviations from the mean. >2 or <-2 indicates extreme values."
                )
            
            with col2:
                # Interpretation
                if z_score > 2:
                    interpretation = f"{name1} is significantly overvalued relative to {name2}"
                    color = "#ff4b4b"
                elif z_score < -2:
                    interpretation = f"{name1} is significantly undervalued relative to {name2}"
                    color = "#00ff00"
                elif z_score > 1:
                    interpretation = f"{name1} is moderately overvalued relative to {name2}"
                    color = "#ffa500"
                elif z_score < -1:
                    interpretation = f"{name1} is moderately undervalued relative to {name2}"
                    color = "#4da6ff"
                else:
                    interpretation = "Ratio is near historical average"
                    color = "#ffffff"
                
                st.markdown(f"<p style='color: {color}; font-weight: bold;'>{interpretation}</p>", unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Charts
            st.subheader("📊 Ratio Analysis Chart")
            
            # Create ratio chart
            fig_ratio = plot_ratio_chart_enhanced(ratio_df, name1, name2)
            st.plotly_chart(fig_ratio, use_container_width=True)
            
            # Statistical interpretation
            col1, col2 = st.columns(2)
            with col1:
                if z_score > 2:
                    interpretation = "⚠️ Ratio is significantly HIGH (>2 SD) - potential overvaluation"
                    color = "#ff4b4b"
                elif z_score > 1:
                    interpretation = "📈 Ratio is moderately HIGH (>1 SD)"
                    color = "#ffa500"
                elif z_score < -2:
                    interpretation = "⚠️ Ratio is significantly LOW (<-2 SD) - potential undervaluation"
                    color = "#00ff00"
                elif z_score < -1:
                    interpretation = "📉 Ratio is moderately LOW (<-1 SD)"
                    color = "#4da6ff"
                else:
                    interpretation = "✅ Ratio is within normal range (±1 SD)"
                    color = "#ffffff"
                
                st.markdown(f"<p style='color: {color}; font-weight: bold; font-size: 16px;'>{interpretation}</p>", unsafe_allow_html=True)
            
            with col2:
                st.info(f"**Statistical Bands:**\n- Blue Line: Current Ratio\n- Orange Dashed: Mean\n- Red/Green Dotted: ±1 & ±2 Standard Deviations")
            
            st.markdown("---")
            
            # Dual chart (optional)
            with st.expander("📊 View Individual Series Charts"):
                st.markdown("View both series separately with their actual values")
                fig_dual = plot_dual_chart(ratio_df, name1, name2)
                st.plotly_chart(fig_dual, use_container_width=True)
            
            st.markdown("---")
            
            # Data table
            st.subheader("📋 Ratio Data")
            
            # Prepare display dataframe
            display_df = ratio_df.copy()
            display_df = display_df.reset_index()
            
            # Format date column
            date_col = display_df.columns[0]
            display_df[date_col] = pd.to_datetime(display_df[date_col]).dt.strftime('%Y-%m-%d')
            
            # Show last 100 rows
            st.dataframe(
                display_df.tail(100).style.format({
                    name1: '{:.2f}',
                    name2: '{:.2f}',
                    'RATIO': '{:.4f}'
                }),
                use_container_width=True,
                height=400
            )
            
            # Download button
            csv = ratio_df.reset_index().to_csv(index=False)
            st.download_button(
                label="📥 Download Ratio Data as CSV",
                data=csv,
                file_name=f"ratio_{name1.replace(' ', '_')}_{name2.replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
# =============================================================================
# TAB 3: FNO TRADING ACTIVITY 
# =============================================================================

elif st.session_state.active_tab == "fno_trading":
    st.header("FNO Trading Activity Analysis")
    st.markdown("Analyze Futures & Options trading activity by participant type")
    
    # FNO data file path
    FNO_DATA_FILE = DATA_dir / "fii-dii_historical_data(nse).csv"
    
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def load_fno_data(file_path):
        """Load FNO trading data and ensure correct data types"""
        try:
            df = pd.read_csv(file_path)
            
            # 1. Fix Dates (DD-MM-YYYY)
            df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
            
            # 2. Fix Numeric Columns
            # We identify columns that should be numbers (usually everything except Date and Client Type)
            cols_to_fix = [
                'Future Index Long', 'Future Index Short', 
                'Future Stock Long', 'Future Stock Short',
                'Option Index Call Long', 'Option Index Put Long',
                'Option Index Call Short', 'Option Index Put Short',
                'Option Stock Call Long', 'Option Stock Put Long',
                'Option Stock Call Short', 'Option Stock Put Short',
                'Total Long Contracts', 'Total Short Contracts'
            ]
            
            for col in cols_to_fix:
                if col in df.columns:
                    # Remove commas if they exist, then convert to numeric
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce')
            
            # 3. Cleanup
            df = df.dropna(subset=['Date']) # Remove rows with invalid dates
            df = df.fillna(0)               # Replace any non-numeric entries with 0
            df = df.sort_values('Date')
            
            return df
            
        except Exception as e:
            st.error(f"Error processing FNO data: {e}")
            return None
    
    def calculate_net_oi(df, client_type):
        """Calculate net OI change for futures and options"""
        # Filter by client type
        df_client = df[df['Client Type'] == client_type].copy()
        
        if df_client.empty:
            return None
        
        # Calculate Futures Net OI (Long - Short)
        df_client['Futures_Net_OI'] = (
            df_client['Future Index Long'] - df_client['Future Index Short'] 
        )
        
        # Calculate Options Net OI (Call Long + Put Short - Call Short - Put Long)
        # This represents bullish positioning
        df_client['Options_Net_OI'] = (
            (df_client['Option Index Call Long'] + df_client['Option Index Put Short']) -
            (df_client['Option Index Call Short'] + df_client['Option Index Put Long'])
        )
        
        # Calculate Total Net OI
        df_client['Total_Net_OI'] = (
            df_client['Total Long Contracts'] - df_client['Total Short Contracts']
        )
        
        return df_client
    
    def plot_net_oi_chart(df_client, client_type):
        """Create chart for net OI changes"""
        fig = go.Figure()
        
        # Add Futures Net OI
        fig.add_trace(go.Scatter(
            x=df_client['Date'],
            y=df_client['Futures_Net_OI'],
            mode='lines',
            name='Futures Net OI',
            line=dict(color='#00bfff', width=2)
        ))
        
        # Add Options Net OI
        fig.add_trace(go.Scatter(
            x=df_client['Date'],
            y=df_client['Options_Net_OI'],
            mode='lines',
            name='Options Net OI',
            line=dict(color='#ffa500', width=2)
        ))
        
        # Add Total Net OI
#        fig.add_trace(go.Scatter(
#            x=df_client['Date'],
#            y=df_client['Total_Net_OI'],
#            mode='lines',
#            name='Total Net OI',
#            line=dict(color='#00ff00', width=2.5)
#        ))
        
        # Add zero line
        fig.add_hline(
            y=0,
            line_dash="dash",
            line_color="#ffffff",
            opacity=0.5,
            annotation_text="Zero Line",
            annotation_position="right"
        )
        
        # Dark theme layout
        fig.update_layout(
            title=f'{client_type} - Net Open Interest Changes',
            yaxis_title='Net OI (Contracts)',
            xaxis_title='Date',
            template='plotly_dark',
            height=600,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(38, 39, 48, 0.8)'
            )
        )
        
        return fig
    
    def plot_separate_charts(df_client, client_type):
        """Create separate charts for Futures and Options"""
        # Futures Chart
        fig_futures = go.Figure()
        
        fig_futures.add_trace(go.Bar(
            x=df_client['Date'],
            y=df_client['Futures_Net_OI'],
            name='Futures Net OI',
            marker=dict(
                color=df_client['Futures_Net_OI'],
                colorscale=[[0, '#ff4b4b'], [0.5, '#ffffff'], [1, '#00ff00']],
                showscale=False
            )
        ))
        
        fig_futures.add_hline(y=0, line_dash="dash", line_color="#ffffff", opacity=0.5)
        
        fig_futures.update_layout(
            title=f'{client_type} - Futures Net OI',
            yaxis_title='Net OI (Contracts)',
            xaxis_title='Date',
            template='plotly_dark',
            height=400,
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff')
        )
        
        # Options Chart
        fig_options = go.Figure()
        
        fig_options.add_trace(go.Bar(
            x=df_client['Date'],
            y=df_client['Options_Net_OI'],
            name='Options Net OI',
            marker=dict(
                color=df_client['Options_Net_OI'],
                colorscale=[[0, '#ff4b4b'], [0.5, '#ffffff'], [1, '#00ff00']],
                showscale=False
            )
        ))
        
        fig_options.add_hline(y=0, line_dash="dash", line_color="#ffffff", opacity=0.5)
        
        fig_options.update_layout(
            title=f'{client_type} - Options Net OI',
            yaxis_title='Net OI (Contracts)',
            xaxis_title='Date',
            template='plotly_dark',
            height=400,
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff')
        )
        
        return fig_futures, fig_options
    
    def plot_total_summary_charts(df_total):
        """Create summary charts for TOTAL market data"""
        # Chart 1: Total Long vs Short Contracts
        fig_long_short = go.Figure()
        
        fig_long_short.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total Long Contracts'],
            mode='lines',
            name='Total Long Contracts',
            line=dict(color='#00ff00', width=2),
            fill='tonexty'
        ))
        
        fig_long_short.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total Short Contracts'],
            mode='lines',
            name='Total Short Contracts',
            line=dict(color='#ff4b4b', width=2),
            fill='tozeroy'
        ))
        
        fig_long_short.update_layout(
            title='Total Market - Long vs Short Contracts',
            yaxis_title='Contracts',
            xaxis_title='Date',
            template='plotly_dark',
            height=500,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(38, 39, 48, 0.8)'
            )
        )
        
        # Chart 2: Futures Activity (Index + Stock)
        fig_futures = go.Figure()
        
        df_total['Total_Futures_Long'] = df_total['Future Index Long'] + df_total['Future Stock Long']
        df_total['Total_Futures_Short'] = df_total['Future Index Short'] + df_total['Future Stock Short']
        
        fig_futures.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total_Futures_Long'],
            mode='lines',
            name='Futures Long',
            line=dict(color='#00bfff', width=2)
        ))
        
        fig_futures.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total_Futures_Short'],
            mode='lines',
            name='Futures Short',
            line=dict(color='#ff6b6b', width=2)
        ))
        
        fig_futures.update_layout(
            title='Total Market - Futures Activity',
            yaxis_title='Contracts',
            xaxis_title='Date',
            template='plotly_dark',
            height=400,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True
        )
        
        # Chart 3: Options Activity
        fig_options = go.Figure()
        
        df_total['Total_Options_Long'] = (
            df_total['Option Index Call Long'] + df_total['Option Index Put Long'] +
            df_total['Option Stock Call Long'] + df_total['Option Stock Put Long']
        )
        df_total['Total_Options_Short'] = (
            df_total['Option Index Call Short'] + df_total['Option Index Put Short'] +
            df_total['Option Stock Call Short'] + df_total['Option Stock Put Short']
        )
        
        fig_options.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total_Options_Long'],
            mode='lines',
            name='Options Long',
            line=dict(color='#ffa500', width=2)
        ))
        
        fig_options.add_trace(go.Scatter(
            x=df_total['Date'],
            y=df_total['Total_Options_Short'],
            mode='lines',
            name='Options Short',
            line=dict(color='#ff1493', width=2)
        ))
        
        fig_options.update_layout(
            title='Total Market - Options Activity',
            yaxis_title='Contracts',
            xaxis_title='Date',
            template='plotly_dark',
            height=400,
            hovermode='x unified',
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            showlegend=True
        )
        
        return fig_long_short, fig_futures, fig_options
    
    # Load FNO data
    fno_df = load_fno_data(FNO_DATA_FILE)
    
    if fno_df is not None:
        st.success(f"✅ Loaded {len(fno_df)} records from FNO data")
        
        # Get available client types
        all_client_types = fno_df['Client Type'].unique().tolist()
        
        # Separate TOTAL from regular client types
        regular_client_types = [ct for ct in all_client_types if ct != 'TOTAL']
        has_total = 'TOTAL' in all_client_types
        
        # Create tabs - regular client types + TOTAL summary
        tab_labels = [f"📊 {ct}" for ct in regular_client_types]
        if has_total:
            tab_labels.append("📈 TOTAL (Market Summary)")
        
        fno_tabs = st.tabs(tab_labels)
        
        # =====================================================================
        # REGULAR CLIENT TYPES (Client, DII, FII, Pro)
        # =====================================================================
        for idx, client_type in enumerate(regular_client_types):
            with fno_tabs[idx]:
                st.subheader(f"{client_type} Trading Activity")
                
                # Date range selection
                col1, col2 = st.columns(2)
                
                # Get min and max dates from data
                min_date = fno_df['Date'].min().date()
                max_date = fno_df['Date'].max().date()
                
                with col1:
                    start_date_fno = st.date_input(
                        "Start Date",
                        value=max_date - pd.Timedelta(days=365),  # Default to 1 year ago
                        min_value=min_date,
                        max_value=max_date,
                        key=f'start_date_fno_{client_type}'
                    )
                
                with col2:
                    end_date_fno = st.date_input(
                        "End Date",
                        value=max_date,
                        min_value=min_date,
                        max_value=max_date,
                        key=f'end_date_fno_{client_type}'
                    )
                
                # Filter data by date range
                df_filtered = fno_df[
                    (fno_df['Date'].dt.date >= start_date_fno) & 
                    (fno_df['Date'].dt.date <= end_date_fno)
                ]
                
                if df_filtered.empty:
                    st.warning("No data available for the selected date range.")
                else:
                    # Calculate net OI
                    df_client = calculate_net_oi(df_filtered, client_type)
                    
                    if df_client is not None and not df_client.empty:
                        # Summary metrics
                        st.markdown("### 📈 Summary Statistics")
                        
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            latest_futures = df_client['Futures_Net_OI'].iloc[-1]
                            st.metric(
                                "Latest Futures Net OI", 
                                f"{latest_futures:,.0f}",
                                delta=f"{latest_futures - df_client['Futures_Net_OI'].iloc[-2]:,.0f}" if len(df_client) > 1 else None
                            )
                        
                        with col2:
                            latest_options = df_client['Options_Net_OI'].iloc[-1]
                            st.metric(
                                "Latest Options Net OI", 
                                f"{latest_options:,.0f}",
                                delta=f"{latest_options - df_client['Options_Net_OI'].iloc[-2]:,.0f}" if len(df_client) > 1 else None
                            )
                        
                        with col3:
                            latest_total = df_client['Total_Net_OI'].iloc[-1]
                            st.metric(
                                "Latest Total Net OI", 
                                f"{latest_total:,.0f}",
                                delta=f"{latest_total - df_client['Total_Net_OI'].iloc[-2]:,.0f}" if len(df_client) > 1 else None
                            )
                        
                        with col4:
                            avg_futures = df_client['Futures_Net_OI'].mean()
                            st.metric("Avg Futures Net OI", f"{avg_futures:,.0f}")
                        
                        st.markdown("---")
                        
                        # Main combined chart
                        st.markdown("### 📊 Net OI Trends")
                        fig_combined = plot_net_oi_chart(df_client, client_type)
                        st.plotly_chart(fig_combined, use_container_width=True)
                        
                        st.markdown("---")
                        
                        # Separate charts
                        st.markdown("### 📈 Detailed Analysis")
                        
                        fig_futures, fig_options = plot_separate_charts(df_client, client_type)
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.plotly_chart(fig_futures, use_container_width=True)
                        
                        with col2:
                            st.plotly_chart(fig_options, use_container_width=True)
                        
                        st.markdown("---")
                        
                        # Data breakdown
                        with st.expander("📋 View Detailed Breakdown"):
                            st.markdown("#### Component Breakdown")
                            
                            # Create breakdown table
                            breakdown_df = df_client[[
                                'Date',
                                'Future Index Long', 'Future Index Short',
                                'Future Stock Long', 'Future Stock Short',
                                'Option Index Call Long', 'Option Index Put Long',
                                'Option Index Call Short', 'Option Index Put Short',
                                'Option Stock Call Long', 'Option Stock Put Long',
                                'Option Stock Call Short', 'Option Stock Put Short',
                                'Futures_Net_OI', 'Options_Net_OI', 'Total_Net_OI'
                            ]].copy()
                            
                            breakdown_df['Date'] = breakdown_df['Date'].dt.strftime('%Y-%m-%d')
                            
                            st.dataframe(
                                breakdown_df.tail(50).style.format({
                                    col: '{:,.0f}' for col in breakdown_df.columns if col != 'Date'
                                }),
                                use_container_width=True,
                                height=400
                            )
                        
                        # Download button
                        csv = df_client.to_csv(index=False)
                        st.download_button(
                            label=f"📥 Download {client_type} Data as CSV",
                            data=csv,
                            file_name=f"fno_{client_type.lower()}_{datetime.now().strftime('%Y%m%d')}.csv",
                            mime="text/csv"
                        )
                        
                        # Interpretation guide
                        st.info("""
                        **Understanding Net OI:**
                        - **Positive Net OI**: More long positions than short (Bullish bias)
                        - **Negative Net OI**: More short positions than long (Bearish bias)
                        - **Futures Net OI**: Future Index + Future Stock (Long - Short)
                        - **Options Net OI**: Call Long + Put Short - Call Short - Put Long (Bullish positioning)
                        - **Total Net OI**: Total Long Contracts - Total Short Contracts
                        """)
                    else:
                        st.error(f"No data found for {client_type}")
        
        # =====================================================================
        # TOTAL MARKET SUMMARY (Special View)
        # =====================================================================
        if has_total:
            with fno_tabs[-1]:  # Last tab
                st.subheader("📈 Total Market Summary")
                st.info("⚠️ **Note**: TOTAL represents aggregated market data across all participant types (Client, DII, FII, Pro). This view shows overall market volumes rather than net positions.")
                
                # Date range selection for TOTAL
                col1, col2 = st.columns(2)
                
                min_date = fno_df['Date'].min().date()
                max_date = fno_df['Date'].max().date()
                
                with col1:
                    start_date_total = st.date_input(
                        "Start Date",
                        value=max_date - pd.Timedelta(days=365),
                        min_value=min_date,
                        max_value=max_date,
                        key='start_date_total'
                    )
                
                with col2:
                    end_date_total = st.date_input(
                        "End Date",
                        value=max_date,
                        min_value=min_date,
                        max_value=max_date,
                        key='end_date_total'
                    )
                
                # Filter TOTAL data
                df_total = fno_df[fno_df['Client Type'] == 'TOTAL'].copy()
                df_total = df_total[
                    (df_total['Date'].dt.date >= start_date_total) & 
                    (df_total['Date'].dt.date <= end_date_total)
                ]
                
                if df_total.empty:
                    st.warning("No TOTAL data available for the selected date range.")
                else:
                    # Summary Metrics
                    st.markdown("### 📊 Market Overview")
                    
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        latest_long = df_total['Total Long Contracts'].iloc[-1]
                        st.metric(
                            "Total Long Contracts",
                            f"{latest_long:,.0f}",
                            delta=f"{latest_long - df_total['Total Long Contracts'].iloc[-2]:,.0f}" if len(df_total) > 1 else None
                        )
                    
                    with col2:
                        latest_short = df_total['Total Short Contracts'].iloc[-1]
                        st.metric(
                            "Total Short Contracts",
                            f"{latest_short:,.0f}",
                            delta=f"{latest_short - df_total['Total Short Contracts'].iloc[-2]:,.0f}" if len(df_total) > 1 else None
                        )
                    
                    with col3:
                        avg_long = df_total['Total Long Contracts'].mean()
                        st.metric(
                            "Avg Long Contracts",
                            f"{avg_long:,.0f}"
                        )
                    
                    with col4:
                        avg_short = df_total['Total Short Contracts'].mean()
                        st.metric(
                            "Avg Short Contracts",
                            f"{avg_short:,.0f}"
                        )
                    
                    st.markdown("---")
                    
                    # Plot summary charts
                    st.markdown("### 📈 Market Activity Charts")
                    
                    fig_long_short, fig_futures, fig_options = plot_total_summary_charts(df_total)
                    
                    # Main chart - Long vs Short
                    st.plotly_chart(fig_long_short, use_container_width=True)
                    
                    st.markdown("---")
                    
                    # Futures and Options side by side
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.plotly_chart(fig_futures, use_container_width=True)
                    
                    with col2:
                        st.plotly_chart(fig_options, use_container_width=True)
                    
                    st.markdown("---")
                    
                    # Detailed breakdown by segment
                    st.markdown("### 📋 Segment Breakdown")
                    
                    # Create segment analysis
                    segment_cols = st.columns(2)
                    
                    with segment_cols[0]:
                        st.markdown("#### Futures Breakdown")
                        
                        futures_breakdown = pd.DataFrame({
                            'Segment': ['Index Long', 'Index Short', 'Stock Long', 'Stock Short'],
                            'Latest': [
                                df_total['Future Index Long'].iloc[-1],
                                df_total['Future Index Short'].iloc[-1],
                                df_total['Future Stock Long'].iloc[-1],
                                df_total['Future Stock Short'].iloc[-1]
                            ],
                            'Average': [
                                df_total['Future Index Long'].mean(),
                                df_total['Future Index Short'].mean(),
                                df_total['Future Stock Long'].mean(),
                                df_total['Future Stock Short'].mean()
                            ]
                        })
                        
                        st.dataframe(
                            futures_breakdown.style.format({
                                'Latest': '{:,.0f}',
                                'Average': '{:,.0f}'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                    
                    with segment_cols[1]:
                        st.markdown("#### Options Breakdown")
                        
                        options_breakdown = pd.DataFrame({
                            'Segment': [
                                'Index Call Long', 'Index Call Short',
                                'Index Put Long', 'Index Put Short',
                                'Stock Call Long', 'Stock Call Short',
                                'Stock Put Long', 'Stock Put Short'
                            ],
                            'Latest': [
                                df_total['Option Index Call Long'].iloc[-1],
                                df_total['Option Index Call Short'].iloc[-1],
                                df_total['Option Index Put Long'].iloc[-1],
                                df_total['Option Index Put Short'].iloc[-1],
                                df_total['Option Stock Call Long'].iloc[-1],
                                df_total['Option Stock Call Short'].iloc[-1],
                                df_total['Option Stock Put Long'].iloc[-1],
                                df_total['Option Stock Put Short'].iloc[-1]
                            ],
                            'Average': [
                                df_total['Option Index Call Long'].mean(),
                                df_total['Option Index Call Short'].mean(),
                                df_total['Option Index Put Long'].mean(),
                                df_total['Option Index Put Short'].mean(),
                                df_total['Option Stock Call Long'].mean(),
                                df_total['Option Stock Call Short'].mean(),
                                df_total['Option Stock Put Long'].mean(),
                                df_total['Option Stock Put Short'].mean()
                            ]
                        })
                        
                        st.dataframe(
                            options_breakdown.style.format({
                                'Latest': '{:,.0f}',
                                'Average': '{:,.0f}'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                    
                    st.markdown("---")
                    
                    # Full data table
                    with st.expander("📋 View Complete TOTAL Data"):
                        display_df = df_total.copy()
                        display_df['Date'] = display_df['Date'].dt.strftime('%Y-%m-%d')
                        
                        # Select relevant columns
                        display_cols = [
                            'Date',
                            'Future Index Long', 'Future Index Short',
                            'Future Stock Long', 'Future Stock Short',
                            'Option Index Call Long', 'Option Index Call Short',
                            'Option Index Put Long', 'Option Index Put Short',
                            'Option Stock Call Long', 'Option Stock Call Short',
                            'Option Stock Put Long', 'Option Stock Put Short',
                            'Total Long Contracts', 'Total Short Contracts'
                        ]
                        
                        st.dataframe(
                            display_df[display_cols].tail(100).style.format({
                                col: '{:,.0f}' for col in display_cols if col != 'Date'
                            }),
                            use_container_width=True,
                            height=500
                        )
                    
                    # Download button
                    csv_total = df_total.to_csv(index=False)
                    st.download_button(
                        label="📥 Download TOTAL Market Data as CSV",
                        data=csv_total,
                        file_name=f"fno_total_market_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
                    
                    # Explanation
                    st.info("""
                    **Understanding TOTAL Market Data:**
                    - **Total Long/Short Contracts**: Sum of all long/short positions across all participants
                    - **Futures**: Combined activity in index and stock futures
                    - **Options**: Combined activity in calls and puts across index and stock options
                    - **Note**: In balanced markets, total long and short contracts are often equal or very close
                    """)
    else:
        st.error("Failed to load FNO data. Please check the file path.")

# =============================================================================
# TAB 4: INDEX ANALYSIS — Point Movement Statistics
# =============================================================================

elif st.session_state.active_tab == "index_analysis":
    st.header("📊 Index Movement Analysis")
    st.markdown("Statistical analysis of index price movements over selected periods")

    INDEX_DATA_DIR_T4 = DATA_dir / "NSE_Indices_Data"
    GLOBAL_DATA_DIR_T4 = DATA_dir / "Global_Indices_Data"

    @st.cache_data(ttl=3600)
    def load_available_indices_t4(directory):
        try:
            path = Path(directory)
            if not path.exists():
                return []
            return sorted([f.stem for f in path.glob("*.csv")])
        except:
            return []

    @st.cache_data(ttl=3600)
    def load_index_ohlc(index_name, directory):
        """Load index data — auto-detects date and close columns"""
        try:
            file_path = Path(directory) / f"{index_name}.csv"
            if not file_path.exists():
                return None
            df = pd.read_csv(file_path)
            df.columns = df.columns.str.strip()

            # Detect date column
            date_col = None
            for col in df.columns:
                if col.strip().lower() in ['timestamp', 'date', 'datetime', 'time']:
                    date_col = col
                    break
            if date_col is None:
                for col in df.columns:
                    try:
                        pd.to_datetime(df[col].dropna().head(5), dayfirst=True)
                        date_col = col
                        break
                    except:
                        continue
            if date_col is None:
                return None

            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors='coerce')
            df = df.dropna(subset=[date_col]).sort_values(date_col)
            df.set_index(date_col, inplace=True)
            df.index.name = 'Date'

            # Detect close column
            close_col = None
            for col in df.columns:
                if col.strip().lower() in ['close', 'close_index_val', 'closing', 'last', 'price']:
                    close_col = col
                    break
            if close_col is None:
                for col in df.columns:
                    if 'close' in col.lower():
                        close_col = col
                        break
            if close_col is None:
                return None

            df[close_col] = pd.to_numeric(
                df[close_col].astype(str).str.replace(',', '').str.replace('$', '').str.strip(),
                errors='coerce'
            )
            df = df.dropna(subset=[close_col])
            df = df.rename(columns={close_col: 'Close'})
            return df[['Close']]
        except Exception as e:
            st.error(f"Error loading {index_name}: {e}")
            return None

    def compute_movement_stats(df, period, thresholds):
        """
        Count how many periods had absolute movement >= each threshold, grouped by year.
        period: 'D' (daily), 'W' (weekly), 'M' (monthly)
        """
        close = df['Close']

        if period == 'D':
            changes = close.diff().dropna().abs()
        elif period == 'W':
            weekly = close.resample('W-FRI').last().dropna()
            changes = weekly.diff().dropna().abs()
        elif period == 'M':
            monthly = close.resample('ME').last().dropna()
            changes = monthly.diff().dropna().abs()
        else:
            return None, None, None

        labels = changes.index
        years = labels.year

        # Year-wise breakdown
        result_rows = []
        for yr in sorted(years.unique()):
            mask = years == yr
            yr_changes = changes[mask]
            row = {'Year': int(yr), 'Total Periods': len(yr_changes)}
            for t in thresholds:
                row[f'≥ {t} pts'] = int((yr_changes >= t).sum())
            result_rows.append(row)

        result_df = pd.DataFrame(result_rows)

        # Overall summary row
        summary = {'Year': 'Overall', 'Total Periods': len(changes)}
        for t in thresholds:
            summary[f'≥ {t} pts'] = int((changes >= t).sum())
        summary_df = pd.DataFrame([summary])

        return result_df, summary_df, changes

    # ── UI ──────────────────────────────────────────────────────────────────

    nse_indices = load_available_indices_t4(INDEX_DATA_DIR_T4)
    global_indices = load_available_indices_t4(GLOBAL_DATA_DIR_T4)

    all_idx_options = []
    if nse_indices:
        all_idx_options += [f"NSE: {x}" for x in nse_indices]
    if global_indices:
        all_idx_options += [f"GLOBAL: {x}" for x in global_indices]

    if not all_idx_options:
        st.error("❌ No index data files found in NSE_Indices_Data or Global_Indices_Data.")
        st.stop()

    # Settings expander
    with st.expander("⚙️ Analysis Settings", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            selected_index = st.selectbox("Select Index", all_idx_options, key="idx_analysis_select")

        with col2:
            period_label = st.selectbox(
                "Grouping Period",
                ["Daily", "Weekly", "Monthly"],
                index=0,
                key="idx_period_select",
                help="Measure price change over each day / week / month"
            )
            period_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
            period_code = period_map[period_label]

        with col3:
            threshold_input = st.text_input(
                "Movement Thresholds (comma-separated)",
                value="50, 100, 250, 500",
                help="Absolute point thresholds to count",
                key="idx_thresh_input"
            )
            try:
                thresholds = sorted([int(x.strip()) for x in threshold_input.split(",") if x.strip()])
            except:
                thresholds = [50, 100, 250, 500]
                st.warning("Invalid thresholds — using defaults: 50, 100, 250, 500")

    # Date range
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        start_dt = st.date_input("Start Date", value=pd.to_datetime("2005-01-01"), key="idx_an_start")
    with col_d2:
        end_dt = st.date_input("End Date", value=pd.to_datetime("today"), key="idx_an_end")

    # Run
    if st.button("🔍 Run Analysis", type="primary", key="idx_run_btn"):
        idx_type, idx_name = selected_index.split(": ", 1)
        directory = INDEX_DATA_DIR_T4 if idx_type == "NSE" else GLOBAL_DATA_DIR_T4

        with st.spinner(f"Loading {idx_name}..."):
            df_idx = load_index_ohlc(idx_name, directory)

        if df_idx is None or df_idx.empty:
            st.error(f"❌ Could not load data for {idx_name}")
        else:
            df_idx = df_idx.loc[str(start_dt):str(end_dt)]
            if df_idx.empty:
                st.error("No data in selected date range.")
            else:
                st.success(
                    f"✅ Loaded {len(df_idx)} trading days for **{idx_name}** "
                    f"({df_idx.index.min().strftime('%Y-%m-%d')} → {df_idx.index.max().strftime('%Y-%m-%d')})"
                )

                result_df, summary_df, all_changes = compute_movement_stats(df_idx, period_code, thresholds)

                if result_df is None:
                    st.error("Error computing stats.")
                else:
                    st.session_state['idx_result'] = result_df
                    st.session_state['idx_summary'] = summary_df
                    st.session_state['idx_changes'] = all_changes
                    st.session_state['idx_thresholds'] = thresholds
                    st.session_state['idx_name'] = idx_name
                    st.session_state['idx_period_label'] = period_label

    # ── Display Results ─────────────────────────────────────────────────────
    if 'idx_result' in st.session_state:
        result_df = st.session_state['idx_result']
        summary_df = st.session_state['idx_summary']
        all_changes = st.session_state['idx_changes']
        thresholds = st.session_state['idx_thresholds']
        idx_name = st.session_state['idx_name']
        period_label = st.session_state['idx_period_label']

        st.markdown("---")

        # ── Summary metric cards ──
        st.subheader(f"📈 {idx_name} — {period_label} Movement Summary")

        metric_cols = st.columns(len(thresholds) + 1)
        total_periods = int(summary_df['Total Periods'].iloc[0])

        with metric_cols[0]:
            st.metric(f"Total {period_label} Periods", f"{total_periods:,}")

        for i, t in enumerate(thresholds):
            col_name = f'≥ {t} pts'
            count = int(summary_df[col_name].iloc[0])
            pct = (count / total_periods * 100) if total_periods else 0
            with metric_cols[i + 1]:
                st.metric(f"≥ {t} pts", f"{count:,}", delta=f"{pct:.1f}%")

        st.markdown("---")

        # ── Chart 1: Year-wise grouped bar chart ──
        st.subheader(f"📊 Year-wise Count of {period_label} Periods by Movement Threshold")

        colors = ['#4da6ff', '#ffa500', '#ff4b4b', '#00ff00', '#ff69b4', '#00bfff', '#ffff00']

        fig_bar = go.Figure()
        for i, t in enumerate(thresholds):
            col_name = f'≥ {t} pts'
            fig_bar.add_trace(go.Bar(
                x=result_df['Year'].astype(str),
                y=result_df[col_name],
                name=col_name,
                marker_color=colors[i % len(colors)]
            ))

        fig_bar.update_layout(
            barmode='group',
            title=f'{idx_name} — {period_label} periods with movement ≥ threshold (by year)',
            xaxis_title='Year',
            yaxis_title=f'Number of {period_label} Periods',
            template='plotly_dark',
            height=550,
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99,
                        bgcolor='rgba(38,39,48,0.8)')
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        st.markdown("---")

        # ── Chart 2: Percentage line chart ──
        st.subheader(f"📊 % of {period_label} Periods Exceeding Thresholds (by Year)")

        fig_pct = go.Figure()
        for i, t in enumerate(thresholds):
            col_name = f'≥ {t} pts'
            pct_values = (result_df[col_name] / result_df['Total Periods'] * 100).round(1)
            fig_pct.add_trace(go.Scatter(
                x=result_df['Year'].astype(str),
                y=pct_values,
                mode='lines+markers',
                name=col_name,
                line=dict(color=colors[i % len(colors)], width=2.5),
                marker=dict(size=7)
            ))

        fig_pct.update_layout(
            title=f'{idx_name} — % of {period_label} periods exceeding threshold (by year)',
            xaxis_title='Year',
            yaxis_title='% of Periods',
            template='plotly_dark',
            height=500,
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff'),
            hovermode='x unified',
            legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99,
                        bgcolor='rgba(38,39,48,0.8)')
        )
        st.plotly_chart(fig_pct, use_container_width=True)

        st.markdown("---")

        # ── Chart 3: Distribution histogram ──
        st.subheader(f"📊 Distribution of Absolute {period_label} Changes")

        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=all_changes.values,
            nbinsx=80,
            marker_color='#00bfff',
            opacity=0.75,
            name='Frequency'
        ))

        for i, t in enumerate(thresholds):
            fig_hist.add_vline(
                x=t, line_dash="dash",
                line_color=colors[i % len(colors)],
                annotation_text=f"{t} pts",
                annotation_position="top",
                annotation_font_color=colors[i % len(colors)]
            )

        fig_hist.update_layout(
            title=f'{idx_name} — Distribution of absolute {period_label.lower()} point changes',
            xaxis_title='Absolute Point Change',
            yaxis_title='Frequency',
            template='plotly_dark',
            height=450,
            plot_bgcolor='#0e1117',
            paper_bgcolor='#0e1117',
            font=dict(color='#ffffff')
        )
        st.plotly_chart(fig_hist, use_container_width=True)

        st.markdown("---")

        # ── Data table ──
        st.subheader("📋 Year-wise Data Table")

        display_table = pd.concat([result_df, summary_df], ignore_index=True)

        # Add percentage columns alongside counts
        for t in thresholds:
            col_name = f'≥ {t} pts'
            pct_col = f'% ≥ {t} pts'
            display_table[pct_col] = (
                display_table[col_name] / display_table['Total Periods'] * 100
            ).round(1)

        # Build format dict dynamically
        fmt_int = {col: '{:,.0f}' for col in display_table.columns
                   if col not in ['Year'] and '% ' not in col}
        fmt_pct = {col: '{:.1f}%' for col in display_table.columns if '% ' in col}
        combined_fmt = {**fmt_int, **fmt_pct}

        st.dataframe(
            display_table.style.format(combined_fmt),
            use_container_width=True,
            height=min(450, 50 + len(display_table) * 35)
        )

        # Download
        csv_out = display_table.to_csv(index=False)
        st.download_button(
            label="📥 Download Analysis as CSV",
            data=csv_out,
            file_name=f"index_movement_{idx_name}_{period_label}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key="idx_download_btn"
        )

        # Interpretation guide
        st.info(f"""
        **How to read this analysis:**
        - Each row shows a calendar year and how many {period_label.lower()} periods had absolute point moves ≥ each threshold.
        - **Higher counts at large thresholds** (e.g. ≥500 pts) indicate higher volatility years.
        - The **% columns** normalize for partial years (useful for the current year).
        - The **distribution chart** shows the full histogram of absolute moves — thresholds are marked as vertical lines.
        """)

# =============================================================================
# TAB 5: Macro Indicators 
# =============================================================================

elif st.session_state.active_tab == "macro_indicators":
    st.markdown("## 📡 High Frequency Macro Indicators")

    # ── Data source selector (3 checkmarks) ────────────────────────────────
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] div[data-testid="column"] label {
        font-size: 14px !important;
    }
    </style>
    """, unsafe_allow_html=True)

    if 'macro_data_source' not in st.session_state:
        st.session_state.macro_data_source = "Eco-Pulse"

    src_c1, src_c2, src_c3, _ = st.columns([1.2, 1.5, 1.8, 4])
    with src_c1:
        if st.button(
            ("✅ " if st.session_state.macro_data_source == "Eco-Pulse" else "⬜ ") + "Eco-Pulse",
            key="src_btn_eco", use_container_width=True
        ):
            st.session_state.macro_data_source = "Eco-Pulse"
            st.session_state.selected_macro_indicator = None
            st.rerun()
    with src_c2:
        if st.button(
            ("✅ " if st.session_state.macro_data_source == "RBI Macro Indicators" else "⬜ ") + "RBI Macro Indicators",
            key="src_btn_rbi", use_container_width=True
        ):
            st.session_state.macro_data_source = "RBI Macro Indicators"
            st.session_state.selected_macro_indicator = None
            st.rerun()
    with src_c3:
        if st.button(
            ("✅ " if st.session_state.macro_data_source == "Other Macro Indicators" else "⬜ ") + "Other Macro Indicators",
            key="src_btn_other", use_container_width=True
        ):
            st.session_state.macro_data_source = "Other Macro Indicators"
            st.session_state.selected_macro_indicator = None
            st.rerun()

    st.markdown("---")

    # ── Initialize selected indicator ──────────────────────────────────────
    if 'selected_macro_indicator' not in st.session_state:
        st.session_state.selected_macro_indicator = None

    data_source = st.session_state.macro_data_source

    # ══════════════════════════════════════════════════════════════════════════
    # SOURCE 1: ECO-PULSE (existing logic — YoY Growth % long format CSV)
    # ══════════════════════════════════════════════════════════════════════════
    if data_source == "Eco-Pulse":
        st.markdown("*YoY Growth % — Select an indicator to view historical chart*")

        DATA_PATH = Macrodata_dir / "eco_pulse_long.csv"

        @st.cache_data(ttl=3600)
        def load_macro_data(path):
            df = pd.read_csv(path)
            df.columns = df.columns.str.strip()
            df['Month'] = pd.to_datetime(df['Month'])
            return df

        try:
            macro_df = load_macro_data(DATA_PATH)
        except Exception as e:
            st.error(f"Could not load macro data: {e}")
            st.stop()

        all_months = sorted(macro_df['Month'].unique())
        latest_month = max(all_months)

        def get_month_offset(base, months_back):
            target = base - pd.DateOffset(months=months_back)
            available = [m for m in all_months if m <= target]
            return max(available) if available else None

        col_months = {
            "Current":      latest_month,
            "Prev Month":   get_month_offset(latest_month, 1),
            "2 Months Ago": get_month_offset(latest_month, 2),
            "Last Year":    get_month_offset(latest_month, 12),
            "2 Years Ago":  get_month_offset(latest_month, 24),
        }

        col_labels = {
            k: (v.strftime("%b '%y") if v is not None else "N/A")
            for k, v in col_months.items()
        }

        indicators = sorted(macro_df['Indicator'].unique())
        dir_color_map = {"green": "#00c853", "red": "#ff1744", "neutral": "#ffab00"}

        def get_val(indicator, month):
            if month is None:
                return None, None
            row = macro_df[(macro_df['Indicator'] == indicator) & (macro_df['Month'] == month)]
            if row.empty:
                return None, None
            try:
                return float(row.iloc[0]['Value']), str(row.iloc[0]['Direction']).lower()
            except (ValueError, TypeError):
                return None, None

        def fmt_cell(val, direction):
            if val is None:
                return "<td style='color:#555555; text-align:right; padding:7px 14px;'>—</td>"
            color = dir_color_map.get(direction, "#ffffff")
            if val == 0:
                return f"<td style='color:{color}; text-align:right; font-weight:600; padding:7px 14px;'>0.0%</td>"
            sign = "+" if val > 0 else ""
            return f"<td style='color:{color}; text-align:right; font-weight:600; padding:7px 14px;'>{sign}{val:.1f}%</td>"

        header_cells = ""
        for k in col_months:
            header_cells += (
                "<th style=\"text-align:right; color:#b0b0b0; font-weight:500; "
                "padding:10px 14px; white-space:nowrap; border-bottom:2px solid #ff4b4b;\">"
                f"{col_labels[k]}<br><span style=\"font-size:10px; color:#666;\">{k}</span></th>"
            )

        rows_html = ""
        for i, ind in enumerate(indicators):
            is_selected = st.session_state.selected_macro_indicator == ind
            row_bg = "#2a1a1a" if is_selected else ("#1a1a2e" if i % 2 == 0 else "#161625")
            row_border = "border-left: 3px solid #ff4b4b;" if is_selected else "border-left: 3px solid transparent;"
            name_color = "#ff6b6b" if is_selected else "#ffffff"
            font_weight = "600" if is_selected else "400"

            cells = ""
            for col_key, month in col_months.items():
                val, direction = get_val(ind, month)
                cells += fmt_cell(val, direction)

            rows_html += (
                f"<tr style=\"background:{row_bg}; {row_border}\">"
                f"<td style=\"color:{name_color}; padding:7px 14px; font-size:13px; "
                f"white-space:nowrap; font-weight:{font_weight};\">{ind}</td>"
                f"{cells}</tr>"
            )

        table_html = (
            "<div style=\"overflow-x:auto; border-radius:8px; border:1px solid #3d3d3d; margin-bottom:16px;\">"
            "<table style=\"width:100%; border-collapse:collapse; font-size:13px;\">"
            "<thead><tr style=\"background:#1e1e2e;\">"
            "<th style=\"text-align:left; color:#b0b0b0; font-weight:500; "
            "padding:10px 14px; border-bottom:2px solid #ff4b4b; min-width:220px;\">Indicator</th>"
            f"{header_cells}"
            "</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>"
        )

        st.markdown(table_html, unsafe_allow_html=True)

        sel_col, m1, m2, m3, m4 = st.columns([2.5, 1, 1, 1, 1])

        with sel_col:
            indicator_options = ["— Select an indicator —"] + indicators
            current_index = (
                indicators.index(st.session_state.selected_macro_indicator) + 1
                if st.session_state.selected_macro_indicator in indicators
                else 0
            )
            selected = st.selectbox(
                "📊 Select indicator to chart:",
                indicator_options,
                index=current_index,
                key="macro_selectbox",
                label_visibility="collapsed"
            )
            if selected != "— Select an indicator —":
                st.session_state.selected_macro_indicator = selected
            else:
                st.session_state.selected_macro_indicator = None

        if st.session_state.selected_macro_indicator:
            ind_df_stats = macro_df[
                macro_df['Indicator'] == st.session_state.selected_macro_indicator
            ].sort_values('Month')

            numeric_vals = pd.to_numeric(ind_df_stats['Value'], errors='coerce')
            valid_vals = numeric_vals.dropna()
            valid_vals = valid_vals[valid_vals != 0]

            cur_val = pd.to_numeric(ind_df_stats.iloc[-1]['Value'], errors='coerce')

            with m1:
                st.metric("Current", f"{cur_val:+.1f}%" if pd.notna(cur_val) else "N/A")
            with m2:
                st.metric("Average", f"{valid_vals.mean():+.1f}%" if not valid_vals.empty else "N/A")
            with m3:
                st.metric("Max", f"{valid_vals.max():+.1f}%" if not valid_vals.empty else "N/A")
            with m4:
                st.metric("Min", f"{valid_vals.min():+.1f}%" if not valid_vals.empty else "N/A")

        st.markdown("---")

        if st.session_state.selected_macro_indicator:
            selected_ind = st.session_state.selected_macro_indicator
            ind_df = macro_df[macro_df['Indicator'] == selected_ind].sort_values('Month')

            st.markdown(f"### 📈 {selected_ind} — Historical YoY Growth %")

            if ind_df.empty:
                st.warning("No data available for this indicator.")
            else:
                bar_colors = (
                    ind_df['Direction']
                    .str.lower()
                    .map(dir_color_map)
                    .fillna("#888888")
                    .tolist()
                )

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=ind_df['Month'],
                    y=ind_df['Value'],
                    marker_color=bar_colors,
                    marker_line_width=0,
                    name="YoY %",
                    hovertemplate="<b>%{x|%b %Y}</b><br>%{y:+.2f}%<extra></extra>"
                ))
                fig.add_trace(go.Scatter(
                    x=ind_df['Month'],
                    y=ind_df['Value'],
                    mode='lines+markers',
                    line=dict(color='#ffffff', width=1.5, dash='dot'),
                    marker=dict(size=4, color='#ffffff'),
                    name="Trend",
                    hoverinfo='skip'
                ))
                fig.add_hline(y=0, line_color="#444444", line_width=1.2)
                fig.update_layout(
                    plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font_color='#ffffff',
                    xaxis=dict(
                        showgrid=False, tickformat="%b %Y",
                        tickfont=dict(color='#b0b0b0', size=11),
                        title_font=dict(color='#ffffff'),
                        rangeslider=dict(visible=True, bgcolor='#1a1a2e', thickness=0.05),
                        rangeselector=dict(
                            buttons=[
                                dict(count=1, label="1Y", step="year", stepmode="backward"),
                                dict(count=3, label="3Y", step="year", stepmode="backward"),
                                dict(count=5, label="5Y", step="year", stepmode="backward"),
                                dict(step="all", label="All"),
                            ],
                            bgcolor='#262730', activecolor='#ff4b4b',
                            font=dict(color='#ffffff'),
                        ),
                    ),
                    yaxis=dict(
                        showgrid=True, gridcolor='#1e1e2e',
                        tickfont=dict(color='#b0b0b0', size=11),
                        title="YoY Growth %", title_font=dict(color='#b0b0b0'),
                        zeroline=False, ticksuffix="%",
                    ),
                    legend=dict(
                        font=dict(color='#ffffff'), bgcolor='rgba(0,0,0,0)',
                        orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                    ),
                    margin=dict(l=10, r=10, t=50, b=60),
                    height=460, bargap=0.15,
                )
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("📋 View Raw Data Table"):
                    display_df = ind_df[['Month', 'Value', 'Direction']].copy()
                    display_df['Month'] = display_df['Month'].dt.strftime('%b %Y')
                    display_df['Value'] = display_df['Value'].apply(
                        lambda x: f"{float(x):+.1f}%" if pd.notna(x) else "N/A"
                    )
                    display_df.columns = ['Month', 'YoY Growth %', 'Signal']
                    st.dataframe(
                        display_df.sort_values('Month', ascending=False).reset_index(drop=True),
                        use_container_width=True, hide_index=True
                    )
        else:
            st.markdown("""
            <div style='text-align:center; padding:60px 20px; color:#555555;'>
                <div style='font-size:48px;'>📊</div>
                <div style='font-size:16px; margin-top:12px;'>
                    Select an indicator from the dropdown above to view its historical chart
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SOURCE 2: RBI MACRO INDICATORS (50 Macroeconomic Indicators.xlsx — Monthly tab)
    # ══════════════════════════════════════════════════════════════════════════
    elif data_source == "RBI Macro Indicators":
        st.markdown("*Raw values — Select an indicator to view historical chart*")

        RBI_PATH = Macrodata_dir / "50 Macroeconomic Indicators.xlsx"

        @st.cache_data(ttl=3600)
        def load_rbi_data(path):
            df = pd.read_excel(path, sheet_name="Monthly", header=0)
            # First column is the period/date
            df.rename(columns={df.columns[0]: "Period"}, inplace=True)
            df['Period'] = pd.to_datetime(df['Period'], errors='coerce')
            df.dropna(subset=['Period'], inplace=True)
            df.sort_values('Period', inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        try:
            rbi_df = load_rbi_data(RBI_PATH)
        except Exception as e:
            st.error(f"Could not load RBI data: {e}")
            st.stop()

        # All indicator columns (everything except Period)
        rbi_indicators = [c for c in rbi_df.columns if c != "Period"]

        # ── Summary table: latest 3 months per indicator ──────────────────
        latest_periods = rbi_df['Period'].sort_values().unique()[-3:]
        sub = rbi_df[rbi_df['Period'].isin(latest_periods)].copy()
        sub['Period_label'] = sub['Period'].dt.strftime("%b '%y")

        period_labels = [p.strftime("%b '%y") for p in sorted(latest_periods)]

        rows_html = ""
        for i, ind in enumerate(rbi_indicators):
            is_selected = st.session_state.selected_macro_indicator == ind
            row_bg = "#2a1a1a" if is_selected else ("#1a1a2e" if i % 2 == 0 else "#161625")
            row_border = "border-left: 3px solid #ff4b4b;" if is_selected else "border-left: 3px solid transparent;"
            name_color = "#ff6b6b" if is_selected else "#ffffff"
            font_weight = "600" if is_selected else "400"

            cells = ""
            for period in sorted(latest_periods):
                row = sub[sub['Period'] == period]
                if row.empty or pd.isna(row.iloc[0].get(ind, None)):
                    cells += "<td style='color:#555555; text-align:right; padding:7px 14px;'>—</td>"
                else:
                    val = row.iloc[0][ind]
                    try:
                        fval = float(val)
                        cells += f"<td style='color:#e0e0e0; text-align:right; padding:7px 14px;'>{fval:,.2f}</td>"
                    except (ValueError, TypeError):
                        cells += f"<td style='color:#e0e0e0; text-align:right; padding:7px 14px;'>{val}</td>"

            rows_html += (
                f"<tr style=\"background:{row_bg}; {row_border}\">"
                f"<td style=\"color:{name_color}; padding:7px 14px; font-size:13px; "
                f"white-space:nowrap; font-weight:{font_weight};\">{ind}</td>"
                f"{cells}</tr>"
            )

        header_cells = "".join(
            f"<th style=\"text-align:right; color:#b0b0b0; font-weight:500; "
            f"padding:10px 14px; white-space:nowrap; border-bottom:2px solid #ff4b4b;\">{lbl}</th>"
            for lbl in period_labels
        )

        table_html = (
            "<div style=\"overflow-x:auto; border-radius:8px; border:1px solid #3d3d3d; margin-bottom:16px;\">"
            "<table style=\"width:100%; border-collapse:collapse; font-size:13px;\">"
            "<thead><tr style=\"background:#1e1e2e;\">"
            "<th style=\"text-align:left; color:#b0b0b0; font-weight:500; "
            "padding:10px 14px; border-bottom:2px solid #ff4b4b; min-width:280px;\">Indicator</th>"
            f"{header_cells}"
            "</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)

        # ── Selector ──────────────────────────────────────────────────────
        sel_col, m1, m2, m3, m4 = st.columns([2.5, 1, 1, 1, 1])

        with sel_col:
            indicator_options = ["— Select an indicator —"] + rbi_indicators
            current_index = (
                rbi_indicators.index(st.session_state.selected_macro_indicator) + 1
                if st.session_state.selected_macro_indicator in rbi_indicators
                else 0
            )
            selected = st.selectbox(
                "📊 Select indicator to chart:",
                indicator_options,
                index=current_index,
                key="rbi_macro_selectbox",
                label_visibility="collapsed"
            )
            if selected != "— Select an indicator —":
                st.session_state.selected_macro_indicator = selected
            else:
                st.session_state.selected_macro_indicator = None

        if st.session_state.selected_macro_indicator:
            ind_series = pd.to_numeric(rbi_df[st.session_state.selected_macro_indicator], errors='coerce')
            valid_vals = ind_series.dropna()

            with m1:
                st.metric("Latest", f"{valid_vals.iloc[-1]:,.2f}" if not valid_vals.empty else "N/A")
            with m2:
                st.metric("Average", f"{valid_vals.mean():,.2f}" if not valid_vals.empty else "N/A")
            with m3:
                st.metric("Max", f"{valid_vals.max():,.2f}" if not valid_vals.empty else "N/A")
            with m4:
                st.metric("Min", f"{valid_vals.min():,.2f}" if not valid_vals.empty else "N/A")

        st.markdown("---")

        if st.session_state.selected_macro_indicator:
            selected_ind = st.session_state.selected_macro_indicator
            ind_df = rbi_df[['Period', selected_ind]].copy()
            ind_df[selected_ind] = pd.to_numeric(ind_df[selected_ind], errors='coerce')
            ind_df.dropna(subset=[selected_ind], inplace=True)

            st.markdown(f"### 📈 {selected_ind} — Historical Values")

            if ind_df.empty:
                st.warning("No data available for this indicator.")
            else:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ind_df['Period'],
                    y=ind_df[selected_ind],
                    mode='lines+markers',
                    line=dict(color='#ff4b4b', width=2),
                    marker=dict(size=4, color='#ff4b4b'),
                    name=selected_ind,
                    hovertemplate="<b>%{x|%b %Y}</b><br>%{y:,.2f}<extra></extra>"
                ))
                fig.add_hline(y=0, line_color="#444444", line_width=1.2)
                fig.update_layout(
                    plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font_color='#ffffff',
                    xaxis=dict(
                        showgrid=False, tickformat="%b %Y",
                        tickfont=dict(color='#b0b0b0', size=11),
                        rangeslider=dict(visible=True, bgcolor='#1a1a2e', thickness=0.05),
                        rangeselector=dict(
                            buttons=[
                                dict(count=1, label="1Y", step="year", stepmode="backward"),
                                dict(count=3, label="3Y", step="year", stepmode="backward"),
                                dict(count=5, label="5Y", step="year", stepmode="backward"),
                                dict(step="all", label="All"),
                            ],
                            bgcolor='#262730', activecolor='#ff4b4b',
                            font=dict(color='#ffffff'),
                        ),
                    ),
                    yaxis=dict(
                        showgrid=True, gridcolor='#1e1e2e',
                        tickfont=dict(color='#b0b0b0', size=11),
                        title="Value", title_font=dict(color='#b0b0b0'),
                        zeroline=False,
                    ),
                    legend=dict(
                        font=dict(color='#ffffff'), bgcolor='rgba(0,0,0,0)',
                        orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                    ),
                    margin=dict(l=10, r=10, t=50, b=60),
                    height=460,
                )
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("📋 View Raw Data Table"):
                    display_df = ind_df.copy()
                    display_df['Period'] = display_df['Period'].dt.strftime('%b %Y')
                    display_df.columns = ['Month', 'Value']
                    st.dataframe(
                        display_df.sort_values('Month', ascending=False).reset_index(drop=True),
                        use_container_width=True, hide_index=True
                    )
        else:
            st.markdown("""
            <div style='text-align:center; padding:60px 20px; color:#555555;'>
                <div style='font-size:48px;'>📊</div>
                <div style='font-size:16px; margin-top:12px;'>
                    Select an indicator from the dropdown above to view its historical chart
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════════════════
    # SOURCE 3: OTHER MACRO INDICATORS (Other Macroeconomic Indicators.xlsx — Monthly tab)
    # ══════════════════════════════════════════════════════════════════════════
    elif data_source == "Other Macro Indicators":
        st.markdown("*Raw values — Select an indicator to view historical chart*")

        OTHER_PATH = Macrodata_dir / "Other Macroeconomic Indicators.xlsx"

        @st.cache_data(ttl=3600)
        def load_other_data(path):
            df = pd.read_excel(path, sheet_name="Monthly", header=0)
            df.rename(columns={df.columns[0]: "Period"}, inplace=True)
            df['Period'] = pd.to_datetime(df['Period'], errors='coerce')
            df.dropna(subset=['Period'], inplace=True)
            df.sort_values('Period', inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        try:
            other_df = load_other_data(OTHER_PATH)
        except Exception as e:
            st.error(f"Could not load Other Macro data: {e}")
            st.stop()

        other_indicators = [c for c in other_df.columns if c != "Period"]

        latest_periods = other_df['Period'].sort_values().unique()[-3:]
        sub = other_df[other_df['Period'].isin(latest_periods)].copy()
        period_labels = [p.strftime("%b '%y") for p in sorted(latest_periods)]

        rows_html = ""
        for i, ind in enumerate(other_indicators):
            is_selected = st.session_state.selected_macro_indicator == ind
            row_bg = "#2a1a1a" if is_selected else ("#1a1a2e" if i % 2 == 0 else "#161625")
            row_border = "border-left: 3px solid #ff4b4b;" if is_selected else "border-left: 3px solid transparent;"
            name_color = "#ff6b6b" if is_selected else "#ffffff"
            font_weight = "600" if is_selected else "400"

            cells = ""
            for period in sorted(latest_periods):
                row = sub[sub['Period'] == period]
                if row.empty or ind not in row.columns or pd.isna(row.iloc[0].get(ind, None)):
                    cells += "<td style='color:#555555; text-align:right; padding:7px 14px;'>—</td>"
                else:
                    val = row.iloc[0][ind]
                    try:
                        fval = float(val)
                        cells += f"<td style='color:#e0e0e0; text-align:right; padding:7px 14px;'>{fval:,.2f}</td>"
                    except (ValueError, TypeError):
                        cells += f"<td style='color:#e0e0e0; text-align:right; padding:7px 14px;'>{val}</td>"

            rows_html += (
                f"<tr style=\"background:{row_bg}; {row_border}\">"
                f"<td style=\"color:{name_color}; padding:7px 14px; font-size:13px; "
                f"white-space:nowrap; font-weight:{font_weight};\">{ind}</td>"
                f"{cells}</tr>"
            )

        header_cells = "".join(
            f"<th style=\"text-align:right; color:#b0b0b0; font-weight:500; "
            f"padding:10px 14px; white-space:nowrap; border-bottom:2px solid #ff4b4b;\">{lbl}</th>"
            for lbl in period_labels
        )

        table_html = (
            "<div style=\"overflow-x:auto; border-radius:8px; border:1px solid #3d3d3d; margin-bottom:16px;\">"
            "<table style=\"width:100%; border-collapse:collapse; font-size:13px;\">"
            "<thead><tr style=\"background:#1e1e2e;\">"
            "<th style=\"text-align:left; color:#b0b0b0; font-weight:500; "
            "padding:10px 14px; border-bottom:2px solid #ff4b4b; min-width:280px;\">Indicator</th>"
            f"{header_cells}"
            "</tr></thead>"
            f"<tbody>{rows_html}</tbody>"
            "</table></div>"
        )
        st.markdown(table_html, unsafe_allow_html=True)

        sel_col, m1, m2, m3, m4 = st.columns([2.5, 1, 1, 1, 1])

        with sel_col:
            indicator_options = ["— Select an indicator —"] + other_indicators
            current_index = (
                other_indicators.index(st.session_state.selected_macro_indicator) + 1
                if st.session_state.selected_macro_indicator in other_indicators
                else 0
            )
            selected = st.selectbox(
                "📊 Select indicator to chart:",
                indicator_options,
                index=current_index,
                key="other_macro_selectbox",
                label_visibility="collapsed"
            )
            if selected != "— Select an indicator —":
                st.session_state.selected_macro_indicator = selected
            else:
                st.session_state.selected_macro_indicator = None

        if st.session_state.selected_macro_indicator:
            ind_series = pd.to_numeric(other_df[st.session_state.selected_macro_indicator], errors='coerce')
            valid_vals = ind_series.dropna()

            with m1:
                st.metric("Latest", f"{valid_vals.iloc[-1]:,.2f}" if not valid_vals.empty else "N/A")
            with m2:
                st.metric("Average", f"{valid_vals.mean():,.2f}" if not valid_vals.empty else "N/A")
            with m3:
                st.metric("Max", f"{valid_vals.max():,.2f}" if not valid_vals.empty else "N/A")
            with m4:
                st.metric("Min", f"{valid_vals.min():,.2f}" if not valid_vals.empty else "N/A")

        st.markdown("---")

        if st.session_state.selected_macro_indicator:
            selected_ind = st.session_state.selected_macro_indicator
            ind_df = other_df[['Period', selected_ind]].copy()
            ind_df[selected_ind] = pd.to_numeric(ind_df[selected_ind], errors='coerce')
            ind_df.dropna(subset=[selected_ind], inplace=True)

            st.markdown(f"### 📈 {selected_ind} — Historical Values")

            if ind_df.empty:
                st.warning("No data available for this indicator.")
            else:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=ind_df['Period'],
                    y=ind_df[selected_ind],
                    mode='lines+markers',
                    line=dict(color='#ff4b4b', width=2),
                    marker=dict(size=4, color='#ff4b4b'),
                    name=selected_ind,
                    hovertemplate="<b>%{x|%b %Y}</b><br>%{y:,.2f}<extra></extra>"
                ))
                fig.add_hline(y=0, line_color="#444444", line_width=1.2)
                fig.update_layout(
                    plot_bgcolor='#0e1117', paper_bgcolor='#0e1117', font_color='#ffffff',
                    xaxis=dict(
                        showgrid=False, tickformat="%b %Y",
                        tickfont=dict(color='#b0b0b0', size=11),
                        rangeslider=dict(visible=True, bgcolor='#1a1a2e', thickness=0.05),
                        rangeselector=dict(
                            buttons=[
                                dict(count=1, label="1Y", step="year", stepmode="backward"),
                                dict(count=3, label="3Y", step="year", stepmode="backward"),
                                dict(count=5, label="5Y", step="year", stepmode="backward"),
                                dict(step="all", label="All"),
                            ],
                            bgcolor='#262730', activecolor='#ff4b4b',
                            font=dict(color='#ffffff'),
                        ),
                    ),
                    yaxis=dict(
                        showgrid=True, gridcolor='#1e1e2e',
                        tickfont=dict(color='#b0b0b0', size=11),
                        title="Value", title_font=dict(color='#b0b0b0'),
                        zeroline=False,
                    ),
                    legend=dict(
                        font=dict(color='#ffffff'), bgcolor='rgba(0,0,0,0)',
                        orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1,
                    ),
                    margin=dict(l=10, r=10, t=50, b=60),
                    height=460,
                )
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("📋 View Raw Data Table"):
                    display_df = ind_df.copy()
                    display_df['Period'] = display_df['Period'].dt.strftime('%b %Y')
                    display_df.columns = ['Month', 'Value']
                    st.dataframe(
                        display_df.sort_values('Month', ascending=False).reset_index(drop=True),
                        use_container_width=True, hide_index=True
                    )
        else:
            st.markdown("""
            <div style='text-align:center; padding:60px 20px; color:#555555;'>
                <div style='font-size:48px;'>📊</div>
                <div style='font-size:16px; margin-top:12px;'>
                    Select an indicator from the dropdown above to view its historical chart
                </div>
            </div>
            """, unsafe_allow_html=True)

            
# Sidebar information
with st.sidebar:
    st.markdown("---")
    st.markdown("### ℹ️ About")
    st.info("""
    **Stock Analysis**: Analyzes NSE stocks to find those trading below their 200-day moving average.
    
    **Index Ratio**: Calculates historical ratios between NSE indices to identify relative value opportunities.
    
    **FNO Trading**: Analyzes Futures & Options trading activity by participant type (Client, DII, FII, Pro) and overall market summary.
    
    **200 DMA**: A long-term trend indicator. Stocks below this level may be in a downtrend.
    
    **Z-Score**: Measures how many standard deviations the current ratio is from the mean. Values >2 or <-2 indicate extreme levels.
    
    **Net OI**: Difference between long and short positions, indicating market sentiment.
    
    """)
    
    st.markdown(f"*Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")


