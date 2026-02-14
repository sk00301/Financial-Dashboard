#DATA_DIR = Path("C:/Users/patel/OneDrive/Desktop/Code")
#!/usr/bin/env python3
"""
NSE Data Updater - Background Process
Fetches and updates:
1. NSE equity ticker symbols
2. FII-DII historical participant data
3. FII-only historical data
4. NSE Indices historical data
5. fetching fpi data from nsdl
6. Updating global indices
7. No. of stocks below 200 DMA
"""

import requests
import pandas as pd
import time
import logging
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from io import BytesIO
import schedule
from nselib import capital_market
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf


# ============================================================================
# CONFIGURATION
# ============================================================================

# File paths
DATA_DIR = Path("C:/Users/patel/OneDrive/Desktop/Code")
INDICES_DIR = Path("NSE_Indices_Data")
TICKERS_FILE = DATA_DIR / "nse_tickers.csv"
FII_DII_FILE = DATA_DIR / "fii-dii_historical_data(nse).csv"
FII_FILE = DATA_DIR / "fii_historical_data.csv"
LOG_FILE = DATA_DIR / "nse_updater.log"
FPI_DATA_DIR = DATA_DIR / "fpi_data"
FPI_DATA_FILE = FPI_DATA_DIR / "combined_fpi_equity_data.csv"
FPI_DOWNLOAD_DIR = FPI_DATA_DIR / "temp_downloads"
FPI_URL = "https://www.fpi.nsdl.co.in/web/Reports/Archive.aspx"
# Global Indices (Investing.com)
GLOBAL_INDICES_DIR = DATA_DIR / "Global_Indices_Data"
# 200 DMA Breadth
BELOW_DMA_FILE = DATA_DIR / "below_dma.csv"
DMA_PERIOD = 200


GLOBAL_INDEX_TICKERS = {
    "MSCI_ALL_WORLD_EQUITY index": "MIWD00000PUS",
    "MSCI_ASIA_APEX index": "ASIAAPEX",
    "MSCI_ASIA_EX_JAPAN index": "MIAX00000NUS",
    "MSCI_EAFE index": "MIEA00000PUS",
    "MSCI_EM index": "MIEF00000PUS",
    "MSCI_FM index": "MI7400000NUS",
    "MSCI_World index": "MIWO00000PUS",
}


# Selenium settings
SELENIUM_TIMEOUT = 60
PAGE_LOAD_WAIT = 3

FPI_ENABLED = True   # Set to False to skip FPI updates
FPI_HEADLESS = True  # Set to False to see browser (debugging)

# Update schedule
UPDATE_HOUR = 9
UPDATE_MINUTE = 0

# NSE Data Sources
NSE_EQUITY_CSV_URL = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_PARTICIPANT_URL_TEMPLATE = "https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date}.csv"

# Request settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 5


# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging():
    """Configure logging to both file and console"""
    DATA_DIR.mkdir(exist_ok=True)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler with UTF-8 encoding
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Try to set UTF-8 for Windows console
    if sys.platform == 'win32':
        try:
            import codecs
            if hasattr(sys.stdout, 'buffer'):
                sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
                sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
        except:
            # If UTF-8 setting fails, continue without it
            # Checkmarks will show as ? but won't crash
            pass
    
    return logger


def create_session():
    """Create a requests session with appropriate headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    return session


# ============================================================================
# TICKERS UPDATE FUNCTIONS
# ============================================================================

def fetch_nse_tickers_with_retry():
    """Fetch NSE equity tickers with retry logic"""
    logger = logging.getLogger()
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Fetching NSE tickers (attempt {attempt}/{MAX_RETRIES})...")
            
            session = create_session()
            response = session.get(NSE_EQUITY_CSV_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            
            temp_file = DATA_DIR / 'temp_equity.csv'
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            
            df = pd.read_csv(temp_file)
            temp_file.unlink()
            
            if 'SYMBOL' not in df.columns:
                logger.error(f"CSV format error. Available columns: {df.columns.tolist()}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return None
            
            symbols = df['SYMBOL'].dropna().unique()
            tickers = [{'symbol': symbol.strip()} for symbol in sorted(symbols)]
            
            logger.info(f"Successfully fetched {len(tickers)} ticker symbols")
            return tickers
            
        except requests.exceptions.Timeout:
            logger.warning(f"Request timeout on attempt {attempt}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Network error on attempt {attempt}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}: {e}", exc_info=True)
        
        if attempt < MAX_RETRIES:
            logger.info(f"Retrying in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    logger.error("All retry attempts exhausted")
    return None


def save_tickers_to_csv(tickers):
    """Save ticker data to CSV with metadata"""
    logger = logging.getLogger()
    
    if not tickers:
        logger.warning("No ticker data to save")
        return False
    
    try:
        df = pd.DataFrame(tickers)
        df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['total_count'] = len(tickers)
        df.to_csv(TICKERS_FILE, index=False)
        
        logger.info(f"Saved {len(tickers)} tickers to {TICKERS_FILE}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving tickers to CSV: {e}", exc_info=True)
        return False


def update_tickers():
    """Update NSE tickers"""
    logger = logging.getLogger()
    logger.info("Updating NSE tickers...")
    
    tickers = fetch_nse_tickers_with_retry()
    if tickers:
        return save_tickers_to_csv(tickers)
    return False


# ============================================================================
# FII-DII DATA UPDATE FUNCTIONS
# ============================================================================

def get_latest_date_from_csv(filepath):
    """
    Get the latest date from an existing CSV file
    
    Returns:
        datetime.date or None
    """
    logger = logging.getLogger()
    
    if not filepath.exists():
        logger.info(f"File {filepath.name} does not exist, will fetch from Feb 4, 2026")
        return datetime(2026, 2, 3).date()  # Start from Feb 3, so next day is Feb 4
    
    try:
        df = pd.read_csv(filepath, low_memory=False)
        
        # Handle different possible date column names
        date_column = None
        for col in ['Date', 'date', 'DATE']:
            if col in df.columns:
                date_column = col
                break
        
        if not date_column:
            logger.warning(f"No date column found in {filepath.name}, starting from Feb 4, 2026")
            return datetime(2026, 2, 3).date()
        
        # Try multiple date formats: DD-MM-YYYY, YYYY-MM-DD, mixed
        df[date_column] = pd.to_datetime(df[date_column], format='mixed', dayfirst=True, errors='coerce')
        
        # Drop any rows where date parsing failed
        df = df.dropna(subset=[date_column])
        
        if len(df) == 0:
            logger.warning(f"No valid dates in {filepath.name}, starting from Feb 4, 2026")
            return datetime(2026, 2, 3).date()
        
        latest_date = df[date_column].max().date()
        
        logger.info(f"Latest date in {filepath.name}: {latest_date}")
        return latest_date
        
    except Exception as e:
        logger.error(f"Error reading latest date from {filepath.name}: {e}")
        return datetime(2026, 2, 3).date()


def fetch_participant_data_for_date(date, session):
    """
    Fetch participant data for a single date
    
    Args:
        date: datetime.date object
        session: requests.Session object
        
    Returns:
        DataFrame or None
    """
    date_str = date.strftime('%d%m%Y')
    url = NSE_PARTICIPANT_URL_TEMPLATE.format(date=date_str)
    
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            # Read CSV, skip the 1st row (title row)
            df = pd.read_csv(BytesIO(response.content), skiprows=1)
            df.columns = [c.strip() for c in df.columns]
            
            # Add Date column
            df['Date'] = date
            
            return df
        else:
            return None
            
    except Exception as e:
        return None


def fetch_incremental_participant_data(start_date, end_date):
    """
    Fetch participant data from start_date to end_date
    
    Args:
        start_date: datetime.date - first date to fetch
        end_date: datetime.date - last date to fetch
        
    Returns:
        DataFrame or None
    """
    logger = logging.getLogger()
    
    all_dfs = []
    session = create_session()
    current_date = start_date
    
    logger.info(f"Fetching FII-DII data from {start_date} to {end_date}...")
    
    successful_fetches = 0
    skipped_dates = 0
    
    while current_date <= end_date:
        df = fetch_participant_data_for_date(current_date, session)
        
        if df is not None:
            all_dfs.append(df)
            successful_fetches += 1
            logger.info(f"✓ Fetched data for {current_date}")
        else:
            skipped_dates += 1
            # Don't log every skip (could be weekends/holidays)
        
        # Anti-blocking sleep
        time.sleep(0.5)
        current_date += timedelta(days=1)
    
    logger.info(f"Fetch complete: {successful_fetches} days successful, {skipped_dates} days skipped (holidays/weekends)")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        return final_df
    else:
        logger.warning("No new data fetched")
        return None


def update_fii_dii_data():
    """
    Update FII-DII historical data file with new data
    
    Returns:
        DataFrame with new data, or None if no update was performed
    """
    logger = logging.getLogger()
    logger.info("Updating FII-DII historical data...")
    
    try:
        # Get latest date from existing file
        latest_date = get_latest_date_from_csv(FII_DII_FILE)
        start_date = latest_date + timedelta(days=1)
        end_date = datetime.now().date()
        
        # Check if update is needed
        if start_date > end_date:
            logger.info("FII-DII data is already up to date")
            return pd.DataFrame()  # Return empty DataFrame instead of True
        
        # Fetch new data
        new_data = fetch_incremental_participant_data(start_date, end_date)
        
        if new_data is None:
            logger.warning("No new FII-DII data to add")
            return pd.DataFrame()  # Return empty DataFrame instead of False
        
        # Format the data
        new_data['Date'] = pd.to_datetime(new_data['Date'])
        
        # Set MultiIndex as per original format
        new_data_indexed = new_data.set_index(['Date', 'Client Type'])
        new_data_indexed = new_data_indexed.sort_index()
        
        # Load existing data or create new
        if FII_DII_FILE.exists():
            existing_data = pd.read_csv(FII_DII_FILE, low_memory=False)
            
            # Parse existing dates with mixed format support
            if 'Date' in existing_data.columns:
                existing_data['Date'] = pd.to_datetime(existing_data['Date'], format='mixed', dayfirst=True, errors='coerce')
            
            # Reset index on new data to append
            new_data_to_append = new_data_indexed.reset_index()
            
            # Append new data
            combined_data = pd.concat([existing_data, new_data_to_append], ignore_index=True)
            
            # Remove duplicates based on Date and Client Type
            combined_data = combined_data.drop_duplicates(subset=['Date', 'Client Type'], keep='last')
            
            # Sort by date
            combined_data = combined_data.sort_values('Date')
            
            # Preserve original date format (check first row of existing data)
            sample_df = pd.read_csv(FII_DII_FILE, nrows=5)
            if 'Date' in sample_df.columns and len(sample_df) > 0:
                sample_date = str(sample_df['Date'].iloc[0])
                # Check if format is DD-MM-YYYY
                if '-' in sample_date and not sample_date.startswith('20'):
                    combined_data['Date'] = combined_data['Date'].dt.strftime('%d-%m-%Y')
                else:
                    combined_data['Date'] = combined_data['Date'].dt.strftime('%Y-%m-%d')
            else:
                combined_data['Date'] = combined_data['Date'].dt.strftime('%Y-%m-%d')
            
            logger.info(f"Added {len(new_data_to_append)} new rows to FII-DII data")
        else:
            combined_data = new_data_indexed.reset_index()
            combined_data['Date'] = combined_data['Date'].dt.strftime('%Y-%m-%d')
            logger.info(f"Created new FII-DII file with {len(combined_data)} rows")
        
        # Save to CSV
        combined_data.to_csv(FII_DII_FILE, index=False)
        logger.info(f"Saved FII-DII data to {FII_DII_FILE}")
        
        # Return the new data for FII-only processing
        return new_data
        
    except Exception as e:
        logger.error(f"Error updating FII-DII data: {e}", exc_info=True)
        return None


def update_fii_only_data(new_fii_dii_data):
    """
    Update FII-only historical data file
    
    Args:
        new_fii_dii_data: DataFrame with new FII-DII data
        
    Returns:
        bool: True if successful
    """
    logger = logging.getLogger()
    logger.info("Updating FII-only historical data...")
    
    try:
        if new_fii_dii_data is None or len(new_fii_dii_data) == 0:
            logger.info("No new FII data to add")
            return True
        
        # Filter for FII only
        if isinstance(new_fii_dii_data.index, pd.MultiIndex):
            # Data is still indexed
            fii_data = new_fii_dii_data.xs('FII', level='Client Type').reset_index()
        else:
            # Data is not indexed
            fii_data = new_fii_dii_data[new_fii_dii_data['Client Type'] == 'FII'].copy()
        
        if len(fii_data) == 0:
            logger.warning("No FII records found in new data")
            return False
        
        # Calculate Net Index Future for new data
        if 'Future Index Long' in fii_data.columns and 'Future Index Short' in fii_data.columns:
            fii_data['Net Index Future'] = (
                pd.to_numeric(fii_data['Future Index Long'], errors='coerce') - 
                pd.to_numeric(fii_data['Future Index Short'], errors='coerce')
            )
        
        # Load existing FII data or create new
        if FII_FILE.exists():
            existing_fii = pd.read_csv(FII_FILE, low_memory=False)
            
            # Append new FII data
            combined_fii = pd.concat([existing_fii, fii_data], ignore_index=True)
            
            # Parse dates with mixed format support (handles both DD-MM-YYYY and YYYY-MM-DD)
            combined_fii['Date'] = pd.to_datetime(combined_fii['Date'], format='mixed', dayfirst=True, errors='coerce')
            
            # Recalculate Net Index Future for all data to ensure consistency
            if 'Future Index Long' in combined_fii.columns and 'Future Index Short' in combined_fii.columns:
                combined_fii['Net Index Future'] = (
                    pd.to_numeric(combined_fii['Future Index Long'], errors='coerce') - 
                    pd.to_numeric(combined_fii['Future Index Short'], errors='coerce')
                )
            
            # Remove duplicates based on Date
            combined_fii = combined_fii.drop_duplicates(subset=['Date'], keep='last')
            combined_fii = combined_fii.sort_values('Date')
            
            logger.info(f"Added {len(fii_data)} new rows to FII-only data")
        else:
            combined_fii = fii_data.copy()
            combined_fii['Date'] = pd.to_datetime(combined_fii['Date'], format='mixed', dayfirst=True, errors='coerce')
            logger.info(f"Created new FII-only file with {len(combined_fii)} rows")
        
        # Save to CSV - keep original date format if it exists in the old file
        # Check what format the existing file uses
        if FII_FILE.exists():
            sample_df = pd.read_csv(FII_FILE, nrows=5, low_memory=False)
            if 'Date' in sample_df.columns and len(sample_df) > 0:
                sample_date = str(sample_df['Date'].iloc[0])
                # Check if format is DD-MM-YYYY (has dashes and day first)
                if '-' in sample_date and not sample_date.startswith('20'):
                    # Keep DD-MM-YYYY format
                    combined_fii['Date'] = combined_fii['Date'].dt.strftime('%d-%m-%Y')
                else:
                    # Use YYYY-MM-DD format
                    combined_fii['Date'] = combined_fii['Date'].dt.strftime('%Y-%m-%d')
            else:
                combined_fii['Date'] = combined_fii['Date'].dt.strftime('%Y-%m-%d')
        else:
            combined_fii['Date'] = combined_fii['Date'].dt.strftime('%Y-%m-%d')
        
        combined_fii.to_csv(FII_FILE, index=False)
        logger.info(f"Saved FII-only data to {FII_FILE}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating FII-only data: {e}", exc_info=True)
        return False



# ============================================================================
# NSE INDICES DATA UPDATE FUNCTIONS
# ============================================================================

# Mapping of index file names to nselib index names
INDEX_NAME_MAPPING = {
    'NIFTY_50': 'NIFTY 50',
    'NIFTY_NEXT_50': 'NIFTY NEXT 50',
    'NIFTY_100': 'NIFTY 100',
    'NIFTY_200': 'NIFTY 200',
    'NIFTY_500': 'NIFTY 500',
    'NIFTY_MIDCAP_50': 'NIFTY MIDCAP 50',
    'NIFTY_MIDCAP_100': 'NIFTY MIDCAP 100',
    'NIFTY_MIDCAP_150': 'NIFTY MIDCAP 150',
    'NIFTY_SMALLCAP_50': 'NIFTY SMALLCAP 50',
    'NIFTY_SMALLCAP_100': 'NIFTY SMALLCAP 100',
    'NIFTY_SMALLCAP_250': 'NIFTY SMALLCAP 250',
    'NIFTY_MIDSMALLCAP_400': 'NIFTY MIDSMALLCAP 400',
    'NIFTY_LARGEMIDCAP_250': 'NIFTY LARGEMIDCAP 250',
    'NIFTY_TOTAL_MARKET': 'NIFTY TOTAL MARKET',
    'NIFTY_MICROCAP_250': 'NIFTY MICROCAP 250',
    'NIFTY_BANK': 'NIFTY BANK',
    'NIFTY_AUTO': 'NIFTY AUTO',
    'NIFTY_FINANCIAL_SERVICES': 'NIFTY FINANCIAL SERVICES',
    'NIFTY_FMCG': 'NIFTY FMCG',
    'NIFTY_IT': 'NIFTY IT',
    'NIFTY_MEDIA': 'NIFTY MEDIA',
    'NIFTY_METAL': 'NIFTY METAL',
    'NIFTY_PHARMA': 'NIFTY PHARMA',
    'NIFTY_PSU_BANK': 'NIFTY PSU BANK',
    'NIFTY_PRIVATE_BANK': 'NIFTY PRIVATE BANK',
    'NIFTY_REALTY': 'NIFTY REALTY',
    'NIFTY_HEALTHCARE': 'NIFTY HEALTHCARE',
    'NIFTY_CONSUMER_DURABLES': 'NIFTY CONSUMER DURABLES',
    'NIFTY_OIL___GAS': 'NIFTY OIL & GAS',
    'NIFTY_COMMODITIES': 'NIFTY COMMODITIES',
    'NIFTY_INDIA_CONSUMPTION': 'NIFTY INDIA CONSUMPTION',
    'NIFTY_CPSE': 'NIFTY CPSE',
    'NIFTY_ENERGY': 'NIFTY ENERGY',
    'NIFTY_INFRASTRUCTURE': 'NIFTY INFRASTRUCTURE',
    'NIFTY_MNC': 'NIFTY MNC',
    'NIFTY_PSE': 'NIFTY PSE',
    'NIFTY_SERVICES_SECTOR': 'NIFTY SERVICES SECTOR',
    'NIFTY_INDIA_DIGITAL': 'NIFTY INDIA DIGITAL',
    'NIFTY_INDIA_MANUFACTURING': 'NIFTY INDIA MANUFACTURING',
    'NIFTY_INDIA_DEFENCE': 'NIFTY INDIA DEFENCE',
    'NIFTY_TRANSPORTATION___LOGISTICS': 'NIFTY TRANSPORTATION & LOGISTICS',
    'NIFTY_HOUSING': 'NIFTY HOUSING',
    'NIFTY_MOBILITY': 'NIFTY MOBILITY',
    'NIFTY_EV___NEW_AGE_AUTOMOTIVE': 'NIFTY EV & NEW AGE AUTOMOTIVE',
    'NIFTY100_ESG': 'NIFTY100 ESG',
    'NIFTY_CORE_HOUSING': 'NIFTY CORE HOUSING'
}


def get_existing_index_files():
    """
    Scan the NSE_Indices_Data directory and return list of index files
    
    Returns:
        list: List of tuples (file_path, index_name)
    """
    logger = logging.getLogger()
    
    if not INDICES_DIR.exists():
        logger.warning(f"Indices directory {INDICES_DIR} does not exist")
        return []
    
    index_files = []
    
    for file_path in INDICES_DIR.glob("*.csv"):
        # Extract index name from filename (remove .csv)
        file_name = file_path.stem
        
        # Try to map to nselib index name
        if file_name in INDEX_NAME_MAPPING:
            nselib_name = INDEX_NAME_MAPPING[file_name]
            index_files.append((file_path, nselib_name))
        else:
            # Log unknown index files but skip them
            logger.debug(f"Unknown index file format: {file_name}.csv")
    
    logger.info(f"Found {len(index_files)} index files to update")
    return index_files


def get_latest_date_from_index_csv(filepath):
    """
    Get the latest date from an index CSV file
    
    Returns:
        datetime.date or None
    """
    logger = logging.getLogger()
    
    try:
        df = pd.read_csv(filepath, low_memory=False)
        
        # The timestamp column should be 'TIMESTAMP' based on the original code
        if 'TIMESTAMP' not in df.columns:
            logger.warning(f"No TIMESTAMP column in {filepath.name}")
            return None
        
        # Parse with mixed format support
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'], format='mixed', dayfirst=True, errors='coerce')
        df = df.dropna(subset=['TIMESTAMP'])
        
        if len(df) == 0:
            logger.warning(f"No valid dates in {filepath.name}")
            return None
        
        latest_date = df['TIMESTAMP'].max().date()
        return latest_date
        
    except Exception as e:
        logger.error(f"Error reading {filepath.name}: {e}")
        return None


def fetch_index_data_incremental(index_name, start_date, end_date):
    """
    Fetch index data from start_date to end_date using nselib
    
    Args:
        index_name: Name of the index as per nselib
        start_date: datetime.date - first date to fetch
        end_date: datetime.date - last date to fetch
        
    Returns:
        DataFrame or None
    """
    logger = logging.getLogger()
    
    all_data = []
    current_start = datetime.combine(start_date, datetime.min.time())
    final_end = datetime.combine(end_date, datetime.min.time())
    chunk_size = 60  # Days per request as per original code
    
    while current_start <= final_end:
        current_end = current_start + timedelta(days=chunk_size)
        if current_end > final_end:
            current_end = final_end
        
        str_start = current_start.strftime('%d-%m-%Y')
        str_end = current_end.strftime('%d-%m-%Y')
        
        try:
            df_chunk = capital_market.index_data(
                index=index_name,
                from_date=str_start,
                to_date=str_end
            )
            
            if df_chunk is not None and not df_chunk.empty:
                all_data.append(df_chunk)
                logger.debug(f"  ✓ {index_name} | {str_start} to {str_end} | +{len(df_chunk)} rows")
            
            time.sleep(0.6)  # Anti-blocking delay
            
        except Exception as e:
            logger.debug(f"  ✗ Error for {index_name} ({str_start} to {str_end}): {e}")
        
        current_start = current_end + timedelta(days=1)
    
    if not all_data:
        return None
    
    # Combine all chunks
    final_df = pd.concat(all_data, ignore_index=True)
    
    # Clean and format
    final_df['TIMESTAMP'] = pd.to_datetime(final_df['TIMESTAMP'], dayfirst=True, errors='coerce')
    final_df = final_df.dropna(subset=['TIMESTAMP'])
    final_df = final_df.drop_duplicates(subset=['TIMESTAMP'])
    final_df = final_df.sort_values('TIMESTAMP')
    
    return final_df


def update_single_index_file(file_path, index_name):
    """
    Update a single index CSV file with new data
    
    Args:
        file_path: Path to the CSV file
        index_name: Name of the index as per nselib
        
    Returns:
        bool: True if successful
    """
    logger = logging.getLogger()
    
    try:
        # Get latest date from existing file
        latest_date = get_latest_date_from_index_csv(file_path)
        
        if latest_date is None:
            logger.warning(f"Could not determine latest date for {file_path.name}, skipping")
            return False
        
        # Calculate date range
        start_date = latest_date + timedelta(days=1)
        end_date = datetime.now().date()
        
        # Check if update is needed
        if start_date > end_date:
            logger.debug(f"{file_path.name} is already up to date (latest: {latest_date})")
            return True
        
        logger.info(f"Updating {file_path.name} from {start_date} to {end_date}")
        
        # Fetch new data
        new_data = fetch_index_data_incremental(index_name, start_date, end_date)
        
        if new_data is None or len(new_data) == 0:
            logger.info(f"No new data available for {file_path.name}")
            return True  # Not an error, just no new data
        
        # Load existing data
        existing_data = pd.read_csv(file_path, low_memory=False)
        
        # Format new data timestamp to match existing format (YYYY-MM-DD)
        new_data['TIMESTAMP'] = new_data['TIMESTAMP'].dt.strftime('%Y-%m-%d')
        
        # Combine
        combined_data = pd.concat([existing_data, new_data], ignore_index=True)
        
        # Remove duplicates
        combined_data = combined_data.drop_duplicates(subset=['TIMESTAMP'], keep='last')
        
        # Sort by timestamp
        combined_data['TIMESTAMP'] = pd.to_datetime(combined_data['TIMESTAMP'])
        combined_data = combined_data.sort_values('TIMESTAMP')
        combined_data['TIMESTAMP'] = combined_data['TIMESTAMP'].dt.strftime('%Y-%m-%d')
        
        # Save
        combined_data.to_csv(file_path, index=False)
        
        logger.info(f"✓ Updated {file_path.name} (+{len(new_data)} new rows)")
        return True
        
    except Exception as e:
        logger.error(f"Error updating {file_path.name}: {e}", exc_info=True)
        return False


def update_all_indices():
    """
    Update all index files in NSE_Indices_Data directory
    
    Returns:
        tuple: (success_count, total_count)
    """
    logger = logging.getLogger()
    logger.info("Updating NSE Indices data...")
    
    # Get all index files
    index_files = get_existing_index_files()
    
    if not index_files:
        logger.warning("No index files found to update")
        return 0, 0
    
    success_count = 0
    
    for file_path, index_name in index_files:
        if update_single_index_file(file_path, index_name):
            success_count += 1
    
    logger.info(f"Indices update complete: {success_count}/{len(index_files)} files updated successfully")
    return success_count, len(index_files)


# ============================================================================
# 5. FPI DATA UPDATE FUNCTIONS (add before update_all_data function)
# ============================================================================

def setup_selenium_driver(headless=True):
    """
    Setup Selenium WebDriver with download directory
    
    Args:
        headless: Run in headless mode (default True)
    
    Returns:
        WebDriver instance or None if setup fails
    """
    logger = logging.getLogger()
    
    try:
        # Ensure download directory exists
        FPI_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")  # Run in background
        
        # Stability and compatibility options
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        
        # Ignore SSL errors (sometimes helps with corporate networks)
        chrome_options.add_argument("--ignore-certificate-errors")
        chrome_options.add_argument("--ignore-ssl-errors")
        
        # Download settings
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(FPI_DOWNLOAD_DIR.absolute()),
                "download.prompt_for_download": False,
                "directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )
        
        # Exclude automation flags
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        
        # Additional anti-detection
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("Selenium WebDriver initialized successfully")
        return driver
        
    except Exception as e:
        logger.error(f"Failed to initialize Selenium WebDriver: {e}")
        return None


def wait_and_get_downloaded_file(report_date, before_files, timeout=SELENIUM_TIMEOUT):
    """
    Wait for file download and return the cleaned DataFrame
    
    Args:
        report_date: Date string for file naming
        before_files: Set of files before download
        timeout: Maximum wait time in seconds
    
    Returns:
        pandas.DataFrame or None
    """
    logger = logging.getLogger()
    end_time = time.time() + timeout
    
    while time.time() < end_time:
        after_files = set(os.listdir(FPI_DOWNLOAD_DIR))
        new_files = after_files - before_files
        
        for f in new_files:
            if f.endswith((".csv", ".xls", ".xlsx")):
                old_path = FPI_DOWNLOAD_DIR / f
                ext = os.path.splitext(f)[1]
                new_path = FPI_DOWNLOAD_DIR / f"nsdl_fpi_{report_date}{ext}"
                
                # Rename file
                os.rename(old_path, new_path)
                
                # Read and clean file
                try:
                    # These .xls files are actually HTML files
                    if new_path.suffix in [".xls", ".xlsx"]:
                        logger.info(f"  Reading HTML-formatted .xls file...")
                        tables = pd.read_html(str(new_path))
                        df_raw = tables[0]
                        
                        # Set proper column names
                        num_cols = len(df_raw.columns)
                        headers = [
                            'Reporting Date', 
                            'Debt/Equity', 
                            'Investment Route', 
                            'Gross Purchases(Rs Crore)', 
                            'Gross Sales(Rs Crore)', 
                            'Net Investment (Rs Crore)', 
                            'Net Investment US($) million', 
                            'Conversion (1 USD TO INR)',
                            'nan'
                        ]
                        
                        # Handle extra columns
                        if num_cols > len(headers):
                            headers += ['nan'] * (num_cols - len(headers))
                        
                        df_raw.columns = headers[:num_cols]
                        
                        # Clean the data
                        df_clean = df_raw.iloc[0:].reset_index(drop=True)
                        
                        # Fill merged date cells
                        df_clean['Reporting Date'] = df_clean['Reporting Date'].ffill()
                        
                        # Clean the 'Reporting Date' column (remove newlines, tabs, extra spaces)
                        df_clean['Reporting Date'] = (
                            df_clean['Reporting Date']
                            .astype(str)
                            .str.replace(r'\s+', ' ', regex=True)
                            .str.strip()
                        )
                        
                        # Convert to datetime, coerce errors to NaN
                        valid_dates = pd.to_datetime(df_clean['Reporting Date'], errors='coerce')
                        
                        # Keep only rows with valid dates (removes "Total for Month", "Notes", etc.)
                        df_clean = df_clean[valid_dates.notna()].copy()
                        
                        logger.info(f"  Cleaned data: {len(df_clean)} rows with valid dates")
                        df = df_clean
                        
                    else:  # .csv files
                        df = pd.read_csv(new_path)
                    
                    # Clean up downloaded file
                    os.remove(new_path)
                    return df
                    
                except Exception as e:
                    logger.error(f"Error reading downloaded file: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # Try to clean up file even on error
                    try:
                        os.remove(new_path)
                    except:
                        pass
                    return None
        
        time.sleep(1)
    
    logger.error(f"Download timeout after {timeout} seconds")
    return None


def fetch_fpi_data_for_date(driver, date_obj):
    """
    Fetch FPI data for a specific date using Selenium
    
    Args:
        driver: Selenium WebDriver instance
        date_obj: datetime object for the date to fetch
    
    Returns:
        pandas.DataFrame or None
    """
    logger = logging.getLogger()
    
    date_ui = date_obj.strftime("%d-%b-%Y")
    date_file = date_obj.strftime("%Y-%m-%d")
    
    try:
        logger.info(f"  Fetching data for: {date_ui}")
        
        # Get list of files before download
        before_files = set(os.listdir(FPI_DOWNLOAD_DIR))
        
        # Find and manipulate date input
        date_input = driver.find_element(By.ID, "txtDate")
        driver.execute_script("arguments[0].removeAttribute('disabled')", date_input)
        date_input.clear()
        date_input.send_keys(date_ui)
        
        # Set hidden field value
        driver.execute_script(
            "document.getElementById('hdnDate').value = arguments[0]",
            date_ui
        )
        
        # Submit date
        driver.find_element(By.ID, "btnSubmit1").click()
        time.sleep(PAGE_LOAD_WAIT)
        
        # Click Excel download button
        driver.find_element(By.ID, "btnExcel").click()
        
        # Wait for download and get DataFrame
        df = wait_and_get_downloaded_file(date_file, before_files)
        
        if df is not None:
            logger.info(f"  ✓ Successfully fetched {len(df)} records")
        
        return df
        
    except Exception as e:
        logger.error(f"  ✗ Failed to fetch data for {date_ui}: {e}")
        return None


def get_last_reporting_date():
    """
    Get the last reporting date from the existing FPI data file
    
    Returns:
        datetime object or None
    """
    logger = logging.getLogger()
    
    try:
        if FPI_DATA_FILE.exists():
            df = pd.read_csv(FPI_DATA_FILE)
            if len(df) > 0 and 'Reporting Date' in df.columns:
                # Parse the date (try multiple formats)
                last_date_str = df['Reporting Date'].iloc[-1]
                
                # Try different date formats including 2-digit year
                for fmt in ['%d-%b-%Y', '%d-%b-%y', '%Y-%m-%d', '%d/%m/%Y', '%d/%m/%y']:
                    try:
                        last_date = datetime.strptime(last_date_str, fmt)
                        logger.info(f"Last reporting date in file: {last_date.strftime('%d-%b-%Y')}")
                        return last_date
                    except:
                        continue
                
                logger.warning(f"Could not parse last date: {last_date_str}")
        else:
            logger.info("FPI data file does not exist - will fetch current month")
            
    except Exception as e:
        logger.error(f"Error reading last reporting date: {e}")
    
    return None


def update_fpi_equity_data():
    """
    Update FPI equity data file with latest data
    Only fetches data for the current month (today's date)
    
    Returns:
        bool: True if successful, False otherwise
    """
    logger = logging.getLogger()
    driver = None
    
    try:
        # Ensure directories exist
        FPI_DATA_DIR.mkdir(parents=True, exist_ok=True)
        FPI_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        
        # Get last reporting date
        last_date = get_last_reporting_date()
        
        # Use current date for fetching
        current_date = datetime.today()
        
        # Check if we already have data for current month
        if last_date:
            if last_date.year == current_date.year and last_date.month == current_date.month:
                logger.info(f"Data already up to date for {current_date.strftime('%B %Y')}")
                return True
        
        # Initialize Selenium with retries
        logger.info("Initializing Selenium WebDriver...")
        max_init_retries = 3
        for attempt in range(max_init_retries):
            driver = setup_selenium_driver(headless=FPI_HEADLESS)
            if driver is not None:
                break
            if attempt < max_init_retries - 1:
                logger.warning(f"WebDriver init attempt {attempt + 1} failed, retrying...")
                time.sleep(5)
        
        if driver is None:
            logger.error("Failed to initialize WebDriver after multiple attempts")
            return False
        
        # Navigate to FPI website with retries
        logger.info(f"Navigating to {FPI_URL}")
        max_nav_retries = 3
        nav_success = False
        
        for attempt in range(max_nav_retries):
            try:
                driver.get(FPI_URL)
                time.sleep(PAGE_LOAD_WAIT)
                nav_success = True
                logger.info("Successfully loaded FPI website")
                break
            except Exception as e:
                logger.warning(f"Navigation attempt {attempt + 1} failed: {e}")
                if attempt < max_nav_retries - 1:
                    logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error("Failed to navigate to FPI website after multiple attempts")
                    logger.error("This might be a network/firewall issue. Please check:")
                    logger.error("  1. Internet connection")
                    logger.error("  2. Firewall settings")
                    logger.error("  3. VPN if required")
                    logger.error("  4. Try accessing the site manually in browser")
                    return False
        
        if not nav_success:
            return False
        
        # Fetch data for current date
        logger.info(f"Fetching data for current month: {current_date.strftime('%B %Y')}")
        new_df = fetch_fpi_data_for_date(driver, current_date)
        
        if new_df is None or len(new_df) == 0:
            logger.error("No data fetched for current month")
            return False
        
        logger.info(f"Raw data fetched: {len(new_df)} records")
        
        # Show unique values in key columns for debugging
        if 'Debt/Equity' in new_df.columns:
            debt_equity_values = new_df['Debt/Equity'].unique()
            logger.info(f"Debt/Equity values found: {debt_equity_values}")
        
        if 'Investment Route' in new_df.columns:
            route_values = new_df['Investment Route'].unique()
            logger.info(f"Investment Route values found: {route_values}")
        
        # Filter for Equity only
        logger.info("Filtering for Equity entries...")
        new_df = new_df[new_df["Debt/Equity"] == "Equity"].copy()
        logger.info(f"After Equity filter: {len(new_df)} records")
        
        # Filter for Stock Exchange only
        logger.info("Filtering for Stock Exchange entries...")
        new_df = new_df[new_df["Investment Route"].str.contains("Stock Exchange", na=False, case=False)].copy()
        logger.info(f"After Stock Exchange filter: {len(new_df)} records")
        
        if len(new_df) == 0:
            logger.warning("No records remaining after filtering")
            return False
        
        # Combine with existing data
        if FPI_DATA_FILE.exists():
            logger.info("Loading existing FPI data...")
            existing_df = pd.read_csv(FPI_DATA_FILE)
            logger.info(f"Existing records: {len(existing_df)}")
            
            # Combine DataFrames
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # Remove duplicates
            logger.info("Removing duplicates...")
            combined_df.drop_duplicates(
                subset=["Reporting Date", "Debt/Equity", "Investment Route"],
                inplace=True
            )
            
            # Sort by Reporting Date
            combined_df.sort_values("Reporting Date", inplace=True)
            
            logger.info(f"Total records after merge: {len(combined_df)}")
            logger.info(f"New records added: {len(combined_df) - len(existing_df)}")
            
        else:
            logger.info("Creating new FPI data file...")
            combined_df = new_df
            combined_df.sort_values("Reporting Date", inplace=True)
        
        # Save updated file
        logger.info(f"Saving to {FPI_DATA_FILE}")
        combined_df.to_csv(FPI_DATA_FILE, index=False)
        
        logger.info(f"✓ FPI equity data updated successfully ({len(combined_df)} total records)")
        return True
        
    except Exception as e:
        logger.error(f"Error updating FPI equity data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # Clean up
        if driver:
            try:
                driver.quit()
                logger.info("WebDriver closed")
            except:
                pass
        
        # Clean up temp directory
        try:
            if FPI_DOWNLOAD_DIR.exists():
                for file in FPI_DOWNLOAD_DIR.iterdir():
                    try:
                        file.unlink()
                    except:
                        pass
        except:
            pass

# ============================================================================
# 6. GLOBAL INDICES UPDATE (FINAL HARDENED VERSION)
# ============================================================================

def update_global_indices():
    """
    Update global indices data from Investing.com using investgo
    Handles:
    - date as column
    - date as index
    - date_time column
    - inconsistent casing
    - incremental update
    """

    logger = logging.getLogger()
    logger.info("Updating Global Indices (Investing.com)...")

    try:
        from investgo import get_pair_id, get_historical_prices
    except ImportError:
        logger.error("investgo package not installed.")
        return False

    GLOBAL_INDICES_DIR.mkdir(parents=True, exist_ok=True)

    success_count = 0
    total = len(GLOBAL_INDEX_TICKERS)

    for file_name, ticker in GLOBAL_INDEX_TICKERS.items():
        try:
            file_path = GLOBAL_INDICES_DIR / f"{file_name}.csv"

            # =============================
            # Determine start date
            # =============================
            if file_path.exists():
                existing_df = pd.read_csv(file_path)
                existing_df.columns = existing_df.columns.str.lower()

                if "date" in existing_df.columns:
                    existing_df["date"] = pd.to_datetime(existing_df["date"], dayfirst=True)

                    last_date = existing_df["date"].max()
                    start_date = (last_date + timedelta(days=1)).strftime("%d%m%Y")
                else:
                    start_date = "01012000"
            else:
                existing_df = pd.DataFrame()
                start_date = "01012000"

            end_date = datetime.now().strftime("%d%m%Y")

            # =============================
            # Fetch data
            # =============================
            ids = get_pair_id([ticker])
            if not ids:
                logger.warning(f"ID not found for {ticker}")
                continue

            inv_id = ids[0]
            df = get_historical_prices(inv_id, start_date, end_date)

            if df is None or df.empty:
                logger.info(f"{file_name}: Already up to date")
                success_count += 1
                continue

            # =============================
            # Normalize structure
            # =============================

            # Case 1: date is index
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                df.rename(columns={"index": "date"}, inplace=True)

            # Normalize column names
            df.columns = df.columns.str.lower()

            # If 'date_time' exists
            if "date_time" in df.columns:
                df.rename(columns={"date_time": "date"}, inplace=True)

            # If still no 'date', try first column assumption
            if "date" not in df.columns:
                first_col = df.columns[0]
                try:
                    df[first_col] = pd.to_datetime(df[first_col])
                    df.rename(columns={first_col: "date"}, inplace=True)
                except:
                    logger.error(f"{file_name}: Could not identify date column")
                    continue

            # Convert date
            df["date"] = pd.to_datetime(df["date"], dayfirst=True)

            # =============================
            # Merge with existing
            # =============================
            if not existing_df.empty:
                df = pd.concat([existing_df, df])
                df = df.drop_duplicates(subset="date")

            df = df.sort_values("date")

            df.to_csv(file_path, index=False)

            logger.info(f"✓ Updated {file_name}")
            success_count += 1

        except Exception as e:
            logger.error(f"Failed for {file_name}: {e}")

    logger.info(f"Global Indices Updated: {success_count}/{total}")
    return success_count == total

# ============================================================================
# 7. NSE 200 DMA BREADTH UPDATE
# ============================================================================

def update_nse_200dma_breadth():
    logger = logging.getLogger()
    logger.info("Calculating NSE 200 DMA Breadth (Stable Mode)...")

    try:
        tickers_df = pd.read_csv("nse_tickers.csv")

        if "symbol" not in tickers_df.columns:
            logger.error("symbol column not found in nse_tickers.csv")
            return False

        symbols = tickers_df["symbol"].dropna().unique()
        total = len(symbols)

        logger.info(f"Total symbols: {total}")

        below_count = 0

        for i, symbol in enumerate(symbols):
            ticker = f"{symbol}.NS"

            try:
                stock = yf.Ticker(ticker)
                data = stock.history(period="1y")

                if len(data) < 200:
                    continue

                close = data["Close"]
                dma200 = close.rolling(200).mean().iloc[-1]
                latest_close = close.iloc[-1]

                if pd.notna(dma200) and latest_close < dma200:
                    below_count += 1

                if i % 200 == 0:
                    logger.info(f"Processed {i}/{total}")

            except Exception:
                continue

        result = pd.DataFrame({
            "last_updated": [datetime.now().strftime("%d-%m-%Y %H:%M")],
            "total_below_200dma": [below_count]
        })

        result.to_csv("below_dma.csv", index=False)

        logger.info(f"✓ NSE 200 DMA Breadth Updated: {below_count}/{total} stocks below 200DMA")
        return True

    except Exception as e:
        logger.error(f"Error calculating 200DMA breadth: {e}")
        return False

# ============================================================================
# MAIN UPDATE FUNCTION
# ============================================================================

def update_all_data():
    """
    Main update function - updates all data files
    
    Returns:
        bool: True if all updates successful
    """
    logger = logging.getLogger()
    logger.info("="*60)
    logger.info("STARTING COMPLETE DATA UPDATE")
    logger.info("="*60)
    
    success_count = 0
    total_tasks = 7  # Changed from 4 to 7
    
    # Task 1: Update NSE Tickers
    logger.info("\n[1/7] NSE Tickers Update")
    logger.info("-" * 40)
    if update_tickers():
        success_count += 1
        logger.info("✓ NSE Tickers updated successfully")
    else:
        logger.error("✗ NSE Tickers update failed")
    
    # Task 2: Update FII-DII Data
    logger.info("\n[2/7] FII-DII Historical Data Update")
    logger.info("-" * 40)
    new_fii_dii_data = update_fii_dii_data()
    if new_fii_dii_data is not None:
        success_count += 1
        if len(new_fii_dii_data) > 0:
            logger.info("✓ FII-DII data updated successfully")
        else:
            logger.info("✓ FII-DII data already up to date")
    else:
        logger.error("✗ FII-DII data update failed")
        new_fii_dii_data = pd.DataFrame()  # Use empty DataFrame for next step
    
    # Task 3: Update FII-only Data
    logger.info("\n[3/7] FII-Only Historical Data Update")
    logger.info("-" * 40)
    if update_fii_only_data(new_fii_dii_data):
        success_count += 1
        logger.info("✓ FII-only data updated successfully")
    else:
        logger.error("✗ FII-only data update failed")
    
    # Task 4: Update NSE Indices
    logger.info("\n[4/7] NSE Indices Data Update")
    logger.info("-" * 40)
    indices_success, indices_total = update_all_indices()
    if indices_success > 0:
        success_count += 1
        logger.info(f"✓ NSE Indices updated ({indices_success}/{indices_total} files)")
    else:
        logger.error("✗ NSE Indices update failed")
    
    # Task 5: Update FPI Equity Data (NEW)
    logger.info("\n[5/7] FPI Equity Data Update")
    logger.info("-" * 40)
    if FPI_ENABLED:
        if update_fpi_equity_data():
            success_count += 1
            logger.info("✓ FPI Equity data updated successfully")
        else:
            logger.error("✗ FPI Equity data update failed")
    else:
        logger.info("⊘ FPI Equity data update disabled (FPI_ENABLED=False)")
        success_count += 1  # Don't count as failure if disabled
        
        # Task 6: Update Global Indices
    logger.info("\n[6/7] Global Indices Data Update")
    logger.info("-" * 40)

    if update_global_indices():
        success_count += 1
        logger.info("✓ Global Indices updated successfully")
    else:
        logger.error("✗ Global Indices update failed")

        # Task 7: NSE 200 DMA Breadth
    logger.info("\n[7/7] NSE 200 DMA Breadth Update")
    logger.info("-" * 40)

    if update_nse_200dma_breadth():
        success_count += 1
        logger.info("✓ NSE 200 DMA breadth updated successfully")
    else:
        logger.error("✗ NSE 200 DMA breadth update failed")

    
    # Summary
    logger.info("")
    logger.info("="*60)
    logger.info(f"UPDATE SUMMARY: {success_count}/{total_tasks} tasks completed successfully")
    logger.info("="*60)
    
    return success_count == total_tasks


# ============================================================================
# SCHEDULING & EXECUTION MODES
# ============================================================================

def run_once():
    """Run update once and exit"""
    logger = logging.getLogger()
    logger.info("Running in ONCE mode")
    update_all_data()


def run_daily():
    """Run update daily at scheduled time"""
    logger = logging.getLogger()
    logger.info("Running in DAILY mode")
    logger.info(f"Scheduled time: {UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d}")
    
    # Run immediately on startup
    logger.info("Running initial update...")
    update_all_data()
    
    # Schedule daily updates
    schedule_time = f"{UPDATE_HOUR:02d}:{UPDATE_MINUTE:02d}"
    schedule.every().day.at(schedule_time).do(update_all_data)
    
    logger.info(f"Background process started. Next update at {schedule_time}")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Process stopped by user")


def run_interval(hours=24):
    """Run update at regular intervals"""
    logger = logging.getLogger()
    logger.info(f"Running in INTERVAL mode (every {hours} hours)")
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            logger.info(f"\nIteration #{iteration}")
            
            update_all_data()
            
            next_time = datetime.now() + timedelta(hours=hours)
            logger.info(f"Next update at: {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Sleeping for {hours} hours...")
            
            time.sleep(hours * 3600)
            
    except KeyboardInterrupt:
        logger.info("Process stopped by user")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main entry point"""
    logger = setup_logging()
    
    logger.info("")
    logger.info("NSE DATA UPDATER")
    logger.info("="*60)
    logger.info(f"Data directory: {DATA_DIR.absolute()}")
    logger.info(f"Indices directory: {INDICES_DIR.absolute()}")
    logger.info(f"Files managed:")
    logger.info(f"  1. {TICKERS_FILE.name}")
    logger.info(f"  2. {FII_DII_FILE.name}")
    logger.info(f"  3. {FII_FILE.name}")
    logger.info(f"  4. All CSV files in {INDICES_DIR.name}/")
    logger.info(f"  5. {FPI_DATA_FILE.relative_to(DATA_DIR)}")  # NEW LINE
    logger.info("")
    
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "once"
    
    if mode == "once":
        run_once()
    elif mode == "daily":
        run_daily()
    elif mode == "interval":
        hours = int(sys.argv[2]) if len(sys.argv) > 2 else 24
        run_interval(hours)
    else:
        logger.error(f"Unknown mode: {mode}")
        print("\nUsage:")
        print("  python nse_data_updater.py once           - Run once and exit")
        print("  python nse_data_updater.py daily          - Run daily at 9:00 AM")
        print("  python nse_data_updater.py interval [hrs] - Run every N hours (default: 24)")
        sys.exit(1)



if __name__ == "__main__":
    main()