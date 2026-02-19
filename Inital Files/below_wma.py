import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configuration
DATA_DIR = Path("C:/Users/patel/OneDrive/Desktop/Code") / "Data"
TICKERS_FILE = DATA_DIR / "nse_tickers.csv"
BELOW_WMA_FILE = DATA_DIR / "below_wma.csv"
WMA_PERIOD = 100  # 200 days ≈ 40 weeks (200/5 trading days per week)
START_DATE = "2016-01-01"

# ============================================================================
# 7. NSE 40 WMA BREADTH UPDATE (WITH HISTORICAL DATA)
# ============================================================================

def update_nse_100wma_breadth():
    """
    Calculate NSE 40 WMA breadth for:
    1. Current week (real-time update)
    2. Historical data from Jan 1, 2024 to today (backfill if needed)
    
    Note: 40-week moving average is approximately equivalent to 200-day moving average
    (40 weeks * 5 trading days = 200 days)
    """
    logger = logging.getLogger()
    logger.info("Calculating NSE 40 WMA Breadth with Historical Data...")

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
        if BELOW_WMA_FILE.exists():
            existing_df = pd.read_csv(BELOW_WMA_FILE)
            
            # Handle old format (last_updated column) vs new format (date column)
            if 'date' in existing_df.columns:
                existing_df['date'] = pd.to_datetime(existing_df['date'])
                logger.info(f"Loaded existing data with {len(existing_df)} records")
            elif 'last_updated' in existing_df.columns:
                # Old format - we'll start fresh but log the info
                logger.info(f"Found old format file with 'last_updated' column")
                logger.info(f"Starting fresh with new format (date-based historical data)")
                existing_df = pd.DataFrame(columns=['date', 'total_below_100wma', 'total_stocks', 'pct_below_100wma'])
            else:
                logger.warning("Existing file has unexpected format, starting fresh")
                existing_df = pd.DataFrame(columns=['date', 'total_below_100wma', 'total_stocks', 'pct_below_100wma'])
        else:
            existing_df = pd.DataFrame(columns=['date', 'total_below_100wma', 'total_stocks', 'pct_below_100wma'])
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
        # We need extra data for 40 WMA calculation (40 weeks = ~280 days before start)
        fetch_start = start_date - timedelta(days=350)  # Extra buffer for WMA calculation
        
        logger.info(f"Downloading historical data from {fetch_start.date()}...")
        
        # Dictionary to store all stock data
        stock_data = {}
        
        for i, symbol in enumerate(symbols):
            ticker = f"{symbol}.NS"
            
            try:
                stock = yf.Ticker(ticker)
                data = stock.history(start=fetch_start, end=end_date + timedelta(days=1))
                
                if len(data) >= WMA_PERIOD * 5:  # Need at least 40 weeks of daily data
                    # Remove timezone from index to avoid comparison issues
                    close_series = data['Close']
                    if hasattr(close_series.index, 'tz') and close_series.index.tz is not None:
                        close_series.index = close_series.index.tz_localize(None)
                    
                    # Resample to weekly data (using Friday as the week end, or last available day)
                    weekly_close = close_series.resample('W-FRI').last()
                    stock_data[symbol] = weekly_close
                
                if (i + 1) % 50 == 0:
                    logger.info(f"Downloaded data for {i + 1}/{total} symbols")
                    
            except Exception as e:
                logger.warning(f"Error downloading {ticker}: {e}")
                continue

        logger.info(f"Successfully downloaded data for {len(stock_data)} symbols")

        # Calculate 40 WMA for each week
        results = []
        
        # Get all unique trading weeks in the range
        all_weeks = set()
        for data in stock_data.values():
            all_weeks.update(data.index)
        
        trading_weeks = sorted([w for w in all_weeks if start_date <= w <= end_date])
        
        logger.info(f"Calculating breadth for {len(trading_weeks)} trading weeks...")
        
        for week_idx, calc_week in enumerate(trading_weeks):
            below_count = 0
            valid_stocks = 0
            
            for symbol, weekly_close in stock_data.items():
                try:
                    # Get data up to this week
                    data_until_week = weekly_close[weekly_close.index <= calc_week]
                    
                    if len(data_until_week) < WMA_PERIOD:
                        continue
                    
                    # Calculate 40 WMA
                    wma_40 = data_until_week.rolling(WMA_PERIOD).mean().iloc[-1]
                    latest_close = data_until_week.iloc[-1]
                    
                    if pd.notna(wma_40) and pd.notna(latest_close):
                        valid_stocks += 1
                        if latest_close < wma_40:
                            below_count += 1
                            
                except Exception:
                    continue
            
            if valid_stocks > 0:
                pct_below = (below_count / valid_stocks) * 100
                
                results.append({
                    'date': calc_week.strftime('%Y-%m-%d'),
                    'total_below_100wma': below_count,
                    'total_stocks': valid_stocks,
                    'pct_below_100wma': round(pct_below, 2)
                })
            
            if (week_idx + 1) % 10 == 0 or week_idx == len(trading_weeks) - 1:
                logger.info(f"Processed {week_idx + 1}/{len(trading_weeks)} weeks")

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
            combined_df.to_csv(BELOW_WMA_FILE, index=False)
            
            logger.info(f"✓ NSE 40 WMA Breadth Updated")
            logger.info(f"  Total records: {len(combined_df)}")
            logger.info(f"  Latest date: {combined_df['date'].iloc[-1]}")
            logger.info(f"  Latest breadth: {combined_df['total_below_100wma'].iloc[-1]}/{combined_df['total_stocks'].iloc[-1]} stocks below 100WMA ({combined_df['pct_below_100wma'].iloc[-1]}%)")
            
            return True
        else:
            logger.warning("No new data calculated")
            return False

    except Exception as e:
        logger.error(f"Error calculating 100WMA breadth: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


# ============================================================================
# QUICK UPDATE (FOR WEEKLY RUNS - ONLY CURRENT WEEK'S DATA)
# ============================================================================

def quick_update_current_week():
    """
    Quick update function that only calculates current week's breadth.
    Use this for weekly automated updates after the initial historical backfill.
    """
    logger = logging.getLogger()
    logger.info("Quick update: Calculating current week's 40 WMA Breadth...")

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
                # Get 2 years of data to ensure we have enough for 40-week MA
                data = stock.history(period="2y")

                if len(data) < WMA_PERIOD * 5:  # Need at least 40 weeks of data
                    continue

                # Resample to weekly data
                close = data["Close"]
                weekly_close = close.resample('W-FRI').last()
                
                if len(weekly_close) < WMA_PERIOD:
                    continue
                
                wma40 = weekly_close.rolling(WMA_PERIOD).mean().iloc[-1]
                latest_close = weekly_close.iloc[-1]

                if pd.notna(wma40) and pd.notna(latest_close):
                    valid_stocks += 1
                    if latest_close < wma40:
                        below_count += 1

                if (i + 1) % 50 == 0:
                    logger.info(f"Processed {i + 1}/{total}")

            except Exception:
                continue

        # Get the current week's end date (Friday)
        today = datetime.now()
        days_until_friday = (4 - today.weekday()) % 7  # 4 = Friday
        if days_until_friday == 0 and today.weekday() != 4:
            # If today is not Friday and days_until_friday is 0, it means we need to go back
            days_until_friday = -3 if today.weekday() < 4 else 4
        
        current_week_end = today + timedelta(days=days_until_friday)
        week_date = current_week_end.strftime('%Y-%m-%d')
        
        pct_below = (below_count / valid_stocks * 100) if valid_stocks > 0 else 0

        result = pd.DataFrame({
            "date": [week_date],
            "total_below_100wma": [below_count],
            "total_stocks": [valid_stocks],
            "pct_below_100wma": [round(pct_below, 2)]
        })

        # Load existing data and append
        if BELOW_WMA_FILE.exists():
            existing_df = pd.read_csv(BELOW_WMA_FILE)
            
            # Handle old format vs new format
            if 'date' in existing_df.columns:
                # Remove current week's data if it exists (update it)
                existing_df = existing_df[existing_df['date'] != week_date]
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

        combined_df.to_csv(BELOW_WMA_FILE, index=False)

        logger.info(f"✓ Current Week's NSE 40 WMA Breadth Updated: {below_count}/{valid_stocks} stocks below 100WMA ({pct_below:.2f}%)")
        return True

    except Exception as e:
        logger.error(f"Error calculating current week's 100WMA breadth: {e}")
        return False


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Run the update
    # For first time or when you need to backfill historical data:
    update_nse_100wma_breadth()
    
    # For weekly automated runs (faster):
    # quick_update_current_week()