import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configuration
DATA_DIR = Path("data")  # Adjust to your path
DMA_PERIOD = 200

def diagnose_missing_stocks():
    """
    Identify which stocks are missing from the breadth calculation and why.
    """
    logger = logging.getLogger()
    logger.info("Diagnosing missing stocks...")

    try:
        # Load all tickers
        tickers_df = pd.read_csv(DATA_DIR / "nse_tickers.csv")
        
        if "symbol" not in tickers_df.columns:
            logger.error("symbol column not found in nse_tickers.csv")
            return

        symbols = tickers_df["symbol"].dropna().unique()
        total = len(symbols)
        
        logger.info(f"Total symbols in file: {total}")

        # Categories for missing stocks
        missing_stocks = {
            'delisted': [],
            'insufficient_data': [],
            'download_failed': [],
            'valid': []
        }

        # Test each stock
        for i, symbol in enumerate(symbols):
            ticker = f"{symbol}.NS"
            
            try:
                stock = yf.Ticker(ticker)
                data = stock.history(period="1y")
                
                if data.empty:
                    missing_stocks['delisted'].append(symbol)
                elif len(data) < DMA_PERIOD:
                    missing_stocks['insufficient_data'].append({
                        'symbol': symbol,
                        'days_available': len(data)
                    })
                else:
                    missing_stocks['valid'].append(symbol)
                
                if (i + 1) % 100 == 0:
                    logger.info(f"Checked {i + 1}/{total} stocks")
                    
            except Exception as e:
                missing_stocks['download_failed'].append({
                    'symbol': symbol,
                    'error': str(e)
                })

        # Print summary
        print("\n" + "="*70)
        print("STOCK ANALYSIS SUMMARY")
        print("="*70)
        print(f"Total stocks in nse_tickers.csv: {total}")
        print(f"Valid stocks (with 200+ days data): {len(missing_stocks['valid'])}")
        print(f"\nMISSING STOCKS BREAKDOWN:")
        print(f"  - Delisted/No data: {len(missing_stocks['delisted'])}")
        print(f"  - Insufficient data (<200 days): {len(missing_stocks['insufficient_data'])}")
        print(f"  - Download failed: {len(missing_stocks['download_failed'])}")
        print(f"\nTotal missing: {total - len(missing_stocks['valid'])}")
        print("="*70)

        # Save detailed reports
        reports_dir = DATA_DIR / "reports"
        reports_dir.mkdir(exist_ok=True)

        # Delisted stocks
        if missing_stocks['delisted']:
            delisted_df = pd.DataFrame({'symbol': missing_stocks['delisted']})
            delisted_df.to_csv(reports_dir / "delisted_stocks.csv", index=False)
            print(f"\n✓ Saved {len(missing_stocks['delisted'])} delisted stocks to reports/delisted_stocks.csv")
            print(f"  First 10: {', '.join(missing_stocks['delisted'][:10])}")

        # Insufficient data
        if missing_stocks['insufficient_data']:
            insufficient_df = pd.DataFrame(missing_stocks['insufficient_data'])
            insufficient_df = insufficient_df.sort_values('days_available', ascending=False)
            insufficient_df.to_csv(reports_dir / "insufficient_data_stocks.csv", index=False)
            print(f"\n✓ Saved {len(missing_stocks['insufficient_data'])} stocks with insufficient data to reports/insufficient_data_stocks.csv")
            print(f"  First 10: {', '.join([x['symbol'] for x in missing_stocks['insufficient_data'][:10]])}")

        # Download failed
        if missing_stocks['download_failed']:
            failed_df = pd.DataFrame(missing_stocks['download_failed'])
            failed_df.to_csv(reports_dir / "download_failed_stocks.csv", index=False)
            print(f"\n✓ Saved {len(missing_stocks['download_failed'])} failed downloads to reports/download_failed_stocks.csv")

        # Valid stocks
        valid_df = pd.DataFrame({'symbol': missing_stocks['valid']})
        valid_df.to_csv(reports_dir / "valid_stocks.csv", index=False)
        print(f"\n✓ Saved {len(missing_stocks['valid'])} valid stocks to reports/valid_stocks.csv")

        print("\n" + "="*70)
        print("DIAGNOSIS COMPLETE")
        print("="*70)

    except Exception as e:
        logger.error(f"Error during diagnosis: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    diagnose_missing_stocks()