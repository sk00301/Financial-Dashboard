import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configuration
DATA_DIR = Path("C:/Users/patel/OneDrive/Desktop/Code")/ "Data"
TICKERS_FILE = DATA_DIR / "nse_tickers.csv"
BELOW_DMA_FILE = DATA_DIR / "below_ddma.csv"
DMA_PERIOD = 200
START_DATE = "2000-01-01"

# ============================================================================
# 7. NSE 200 DMA BREADTH UPDATE (WITH HISTORICAL DATA)
# ============================================================================

def update_nse_200dma_breadth():
    """
    Calculate NSE 200 DMA breadth for:
    1. Current day (real-time update)
    2. Historical data from Jan 1, 2024 to today (backfill if needed)
    """
    logger = logging.getLogger()
    logger.info("Calculating NSE 200 DMA Breadth with Historical Data...")

    try:
        # Load tickers
        tickers_df = pd.read_csv(DATA_DIR / "nse_tickers.csv")
        
        if "symbol" not in tickers_df.columns:
            logger.error("symbol column not found in nse_tickers.csv")
            return False

        symbols = tickers_df["symbol"].dropna().unique()
        total = len(symbols)
        logger.info(f"Total symbols: {total}")

        # Load existing data if available
        if BELOW_DMA_FILE.exists():
            existing_df = pd.read_csv(BELOW_DMA_FILE)
            
            # Handle old format (last_updated column) vs new format (date column)
            if 'date' in existing_df.columns:
                existing_df['date'] = pd.to_datetime(existing_df['date'])
                logger.info(f"Loaded existing data with {len(existing_df)} records")
            elif 'last_updated' in existing_df.columns:
                # Old format - we'll start fresh but log the info
                logger.info(f"Found old format file with 'last_updated' column")
                logger.info(f"Starting fresh with new format (date-based historical data)")
                existing_df = pd.DataFrame(columns=['date', 'total_below_200dma', 'total_stocks', 'pct_below_200dma'])
            else:
                logger.warning("Existing file has unexpected format, starting fresh")
                existing_df = pd.DataFrame(columns=['date', 'total_below_200dma', 'total_stocks', 'pct_below_200dma'])
        else:
            existing_df = pd.DataFrame(columns=['date', 'total_below_200dma', 'total_stocks', 'pct_below_200dma'])
            logger.info("No existing data found, will create new file")

        # Determine date range to fetch
        start_date = pd.to_datetime(START_DATE)
        end_date = pd.to_datetime(datetime.now().date())
        
        # Get all trading dates we need to calculate
        if not existing_df.empty:
            last_date = existing_df['date'].max()
            # Only calculate for dates after the last existing date
            start_date = max(start_date, last_date + timedelta(days=1))
            logger.info(f"Updating from {start_date.date()} to {end_date.date()}")
        else:
            logger.info(f"Calculating historical data from {start_date.date()} to {end_date.date()}")

        # If we're already up to date, just return
        if start_date > end_date:
            logger.info("Data is already up to date")
            return True

        # Download historical data for all symbols at once
        # We need extra data for 200 DMA calculation (200 days before start)
        fetch_start = start_date - timedelta(days=250)  # Extra buffer for DMA calculation
        
        logger.info(f"Downloading historical data from {fetch_start.date()}...")
        
        # Dictionary to store all stock data
        stock_data = {}
        
        for i, symbol in enumerate(symbols):
            ticker = f"{symbol}.NS"
            
            try:
                stock = yf.Ticker(ticker)
                data = stock.history(start=fetch_start, end=end_date + timedelta(days=1))
                
                if len(data) >= DMA_PERIOD:
                    # Remove timezone from index to avoid comparison issues
                    close_series = data['Close']
                    if hasattr(close_series.index, 'tz') and close_series.index.tz is not None:
                        close_series.index = close_series.index.tz_localize(None)
                    stock_data[symbol] = close_series
                
                if (i + 1) % 50 == 0:
                    logger.info(f"Downloaded data for {i + 1}/{total} symbols")
                    
            except Exception as e:
                logger.warning(f"Error downloading {ticker}: {e}")
                continue

        logger.info(f"Successfully downloaded data for {len(stock_data)} symbols")

        # Calculate 200 DMA for each date
        results = []
        
        # Get all unique trading dates in the range (all timestamps are now tz-naive)
        all_dates = set()
        for data in stock_data.values():
            all_dates.update(data.index)
        
        trading_dates = sorted([d for d in all_dates if start_date <= d <= end_date])
        
        logger.info(f"Calculating breadth for {len(trading_dates)} trading days...")
        
        for date_idx, calc_date in enumerate(trading_dates):
            below_count = 0
            valid_stocks = 0
            
            for symbol, close_series in stock_data.items():
                try:
                    # Get data up to this date
                    data_until_date = close_series[close_series.index <= calc_date]
                    
                    if len(data_until_date) < DMA_PERIOD:
                        continue
                    
                    # Calculate 200 DMA
                    dma_200 = data_until_date.rolling(DMA_PERIOD).mean().iloc[-1]
                    latest_close = data_until_date.iloc[-1]
                    
                    if pd.notna(dma_200) and pd.notna(latest_close):
                        valid_stocks += 1
                        if latest_close < dma_200:
                            below_count += 1
                            
                except Exception:
                    continue
            
            if valid_stocks > 0:
                pct_below = (below_count / valid_stocks) * 100
                
                results.append({
                    'date': calc_date.strftime('%Y-%m-%d'),
                    'total_below_200dma': below_count,
                    'total_stocks': valid_stocks,
                    'pct_below_200dma': round(pct_below, 2)
                })
            
            if (date_idx + 1) % 10 == 0 or date_idx == len(trading_dates) - 1:
                logger.info(f"Processed {date_idx + 1}/{len(trading_dates)} dates")

        # Create new results DataFrame
        new_results_df = pd.DataFrame(results)
        
        if not new_results_df.empty:
            # Combine with existing data
            combined_df = pd.concat([existing_df, new_results_df], ignore_index=True)
            
            # Remove duplicates (keep the latest calculation)
            combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
            
            # Sort by date
            combined_df = combined_df.sort_values('date')
            
            # Save to CSV
            combined_df.to_csv(BELOW_DMA_FILE, index=False)
            
            logger.info(f"✓ NSE 200 DMA Breadth Updated")
            logger.info(f"  Total records: {len(combined_df)}")
            logger.info(f"  Latest date: {combined_df['date'].iloc[-1]}")
            logger.info(f"  Latest breadth: {combined_df['total_below_200dma'].iloc[-1]}/{combined_df['total_stocks'].iloc[-1]} stocks below 200DMA ({combined_df['pct_below_200dma'].iloc[-1]}%)")
            
            return True
        else:
            logger.warning("No new data calculated")
            return False

    except Exception as e:
        logger.error(f"Error calculating 200DMA breadth: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


# ============================================================================
# QUICK UPDATE (FOR DAILY RUNS - ONLY TODAY'S DATA)
# ============================================================================
'''
def quick_update_today_only():
    """
    Quick update function that only calculates today's breadth.
    Use this for daily automated updates after the initial historical backfill.
    """
    logger = logging.getLogger()
    logger.info("Quick update: Calculating today's 200 DMA Breadth...")

    try:
        tickers_df = pd.read_csv(DATA_DIR / "nse_tickers.csv")
        
        if "symbol" not in tickers_df.columns:
            logger.error("symbol column not found in nse_tickers.csv")
            return False

        symbols = tickers_df["symbol"].dropna().unique()
        total = len(symbols)

        logger.info(f"Total symbols: {total}")

        below_count = 0
        valid_stocks = 0

        for i, symbol in enumerate(symbols):
            ticker = f"{symbol}.NS"

            try:
                stock = yf.Ticker(ticker)
                data = stock.history(period="1y")

                if len(data) < DMA_PERIOD:
                    continue

                close = data["Close"]
                dma200 = close.rolling(DMA_PERIOD).mean().iloc[-1]
                latest_close = close.iloc[-1]

                if pd.notna(dma200) and pd.notna(latest_close):
                    valid_stocks += 1
                    if latest_close < dma200:
                        below_count += 1

                if (i + 1) % 50 == 0:
                    logger.info(f"Processed {i + 1}/{total}")

            except Exception:
                continue

        today = datetime.now().strftime('%Y-%m-%d')
        pct_below = (below_count / valid_stocks * 100) if valid_stocks > 0 else 0

        result = pd.DataFrame({
            "date": [today],
            "total_below_200dma": [below_count],
            "total_stocks": [valid_stocks],
            "pct_below_200dma": [round(pct_below, 2)]
        })

        # Load existing data and append
        if BELOW_DMA_FILE.exists():
            existing_df = pd.read_csv(BELOW_DMA_FILE)
            
            # Handle old format vs new format
            if 'date' in existing_df.columns:
                # Remove today's data if it exists (update it)
                existing_df = existing_df[existing_df['date'] != today]
                combined_df = pd.concat([existing_df, result], ignore_index=True)
            elif 'last_updated' in existing_df.columns:
                # Old format - start fresh with new format
                logger.info("Converting from old format to new date-based format")
                combined_df = result
            else:
                # Unknown format - start fresh
                combined_df = result
        else:
            combined_df = result

        combined_df.to_csv(BELOW_DMA_FILE, index=False)

        logger.info(f"✓ Today's NSE 200 DMA Breadth Updated: {below_count}/{valid_stocks} stocks below 200DMA ({pct_below:.2f}%)")
        return True

    except Exception as e:
        logger.error(f"Error calculating today's 200DMA breadth: {e}")
        return False
'''

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run the update
    # For first time or when you need to backfill historical data:
    update_nse_200dma_breadth()
    
    # For daily automated runs (faster):
    # quick_update_today_only()