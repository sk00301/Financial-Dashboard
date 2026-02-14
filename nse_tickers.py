import requests
import pandas as pd
import time
from datetime import datetime
import os

def fetch_nse_tickers():
    """
    Fetch all equity ticker symbols from NSE India using CSV download
    """
    try:
        print(f"Fetching NSE equity list at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
        
        # NSE provides equity list as CSV
        csv_url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        
        session = requests.Session()
        session.headers.update(headers)
        
        # Download CSV
        response = session.get(csv_url, timeout=15)
        response.raise_for_status()
        
        # Save temporarily and read
        temp_file = 'temp_equity.csv'
        with open(temp_file, 'wb') as f:
            f.write(response.content)
        
        # Read CSV and extract symbols
        df = pd.read_csv(temp_file)
        
        # Clean up temp file
        os.remove(temp_file)
        
        if 'SYMBOL' in df.columns:
            # Get unique symbols
            symbols = df['SYMBOL'].unique()
            tickers = [{'symbol': symbol} for symbol in sorted(symbols)]
            print(f"✓ Successfully fetched {len(tickers)} ticker symbols")
            return tickers
        else:
            print(f"✗ Unexpected CSV format. Columns: {df.columns.tolist()}")
            return None
    
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error: {e}")
        return None
    except Exception as e:
        print(f"✗ Error fetching NSE data: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_to_csv(tickers, filename='nse_tickers.csv'):
    """
    Save ticker symbols to CSV file with timestamp
    """
    if tickers:
        df = pd.DataFrame(tickers)
        
        # Add metadata
        df['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        df.to_csv(filename, index=False)
        print(f"✓ Saved to {filename}\n")
        return True
    else:
        print("✗ No data to save\n")
        return False

def fetch_once():
    """Fetch NSE tickers immediately"""
    tickers = fetch_nse_tickers()
    save_to_csv(tickers)

def fetch_daily_at_time(hour=9, minute=30):
    """
    Fetch NSE tickers once per day at specified time
    
    Args:
        hour: Hour to fetch (0-23, default 9 for 9 AM)
        minute: Minute to fetch (0-59, default 30)
    """
    print(f"Daily NSE ticker fetcher started")
    print(f"Will fetch daily at {hour:02d}:{minute:02d}")
    print("Press Ctrl+C to stop\n")
    
    last_fetch_date = None
    
    try:
        while True:
            now = datetime.now()
            current_date = now.date()
            
            # Check if it's time to fetch and we haven't fetched today
            if (now.hour == hour and now.minute == minute and 
                last_fetch_date != current_date):
                
                print(f"\n{'='*50}")
                print(f"Daily fetch triggered: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*50}\n")
                
                # Fetch and save data
                tickers = fetch_nse_tickers()
                if save_to_csv(tickers):
                    last_fetch_date = current_date
                
                # Sleep for 70 seconds to avoid duplicate fetch
                time.sleep(70)
            
            # Check every 30 seconds
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\nDaily fetcher stopped by user")
    except Exception as e:
        print(f"\nError in daily fetch loop: {e}")

def fetch_daily_interval(hours=24):
    """
    Fetch NSE tickers at regular intervals
    
    Args:
        hours: Hours between fetches (default 24)
    """
    print(f"Interval-based NSE ticker fetcher started")
    print(f"Will fetch every {hours} hours")
    print("Press Ctrl+C to stop\n")
    
    iteration = 0
    
    try:
        while True:
            iteration += 1
            
            print(f"\n{'='*50}")
            print(f"Fetch #{iteration}: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*50}\n")
            
            # Fetch and save data
            tickers = fetch_nse_tickers()
            save_to_csv(tickers)
            
            # Wait for next iteration
            print(f"Next fetch in {hours} hours...")
            print(f"Next fetch at: {datetime.fromtimestamp(time.time() + hours*3600).strftime('%Y-%m-%d %H:%M:%S')}\n")
            time.sleep(hours * 3600)
            
    except KeyboardInterrupt:
        print("\n\nInterval fetcher stopped by user")
    except Exception as e:
        print(f"\nError in interval fetch loop: {e}")

# Main execution
if __name__ == "__main__":
    import sys
    
    print("NSE Ticker Fetcher")
    print("="*50)
    print()
    
    # Choose mode based on argument or default
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
    else:
        mode = "once"  # Default mode
    
    if mode == "once":
        # Fetch immediately and exit
        fetch_once()
        
    elif mode == "daily":
        # Fetch once now, then daily at 9:30 AM
        print("Running initial fetch...\n")
        fetch_once()
        print("\nSwitching to daily mode...\n")
        fetch_daily_at_time(hour=9, minute=30)
        
    elif mode == "interval":
        # Fetch every 24 hours
        fetch_daily_interval(hours=24)
        
    else:
        print("Usage:")
        print("  python script.py once      - Fetch once and exit")
        print("  python script.py daily     - Fetch daily at 9:30 AM")
        print("  python script.py interval  - Fetch every 24 hours")