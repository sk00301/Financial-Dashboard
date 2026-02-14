import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global Configuration
START_DATE = datetime(2000, 1, 1)
END_DATE = datetime.now()
CHUNK_DAYS = 364  # Max days per request to avoid timeouts
OUTPUT_DIR = "nifty_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
TEMP_FILE_PREFIX = "temp_"
MASTER_FILE = "Nifty_All_Indices_History.csv"

async def scrape_nifty_indices():
    """Main function to scrape all Nifty index historical data."""
    async with async_playwright() as p:
        # Launch headless browser
        browser = await p.chromium.launch(headless=True, args=['--disable-gpu', '--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # 1. Initialization Phase
            logger.info("Navigating to Nifty Indices Historical Data page...")
            await page.goto("https://www.niftyindices.com/reports/historical-data", wait_until="domcontentloaded")

            # Wait for page to stabilize
            await page.wait_for_selector("select#ddlHistoricaltypee", timeout=10000)

            # 2. Discovery Loop
            logger.info("Starting Discovery Loop to map out indexes...")
            index_queue = await discover_indexes(page)
            logger.info(f"Discovered {len(index_queue)} indexes to scrape.")

            # 3. Extraction Loop
            logger.info("Starting Extraction Loop...")
            for index_name in index_queue:
                logger.info(f"Scraping data for index: {index_name}")
                try:
                    await extract_index_data(page, index_name, START_DATE, END_DATE)
                except Exception as e:
                    logger.error(f"Failed to scrape index {index_name}: {e}")
                    continue  # Skip to next index

            # 4. Storage & Cleanup
            logger.info("Merging temporary files into master CSV...")
            merge_temp_files()
            logger.info(f"Scraping completed. Master file saved as {MASTER_FILE}")

        except Exception as e:
            logger.error(f"An error occurred during scraping: {e}")
        finally:
            await browser.close()

async def discover_indexes(page):
    """Discover all available indexes by interacting with dropdowns."""
    index_queue = []

    # Select "Equity" in first dropdown
    await page.select_option("select#ddlHistoricaltypee", "Equity")
    await page.wait_for_selector("select#ddlHistoricaltypeeSubindex", state="visible", timeout=10000)

    # Get all sub-categories
    sub_category_options = await page.query_selector_all("select#ddlHistoricaltypeeSubindex option")
    sub_categories = []
    for option in sub_category_options:
        value = await option.get_attribute("value")
        text = await option.text_content()
        if value and value != "":  # Skip empty or placeholder options
            sub_categories.append(value)
            logger.debug(f"Found sub-category: {text} (value: {value})")

    # Iterate through sub-categories
    for sub_cat in sub_categories:
        logger.info(f"Processing sub-category: {sub_cat}")
        await page.select_option("select#ddlHistoricaltypeeSubindex", sub_cat)
        await page.wait_for_selector("select#ddlHistoricaltypeindex", state="visible", timeout=10000)

        # Get all index names
        index_options = await page.query_selector_all("select#ddlHistoricaltypeindex option")
        for option in index_options:
            value = await option.get_attribute("value")
            text = await option.text_content()
            if value and value != "":  # Skip empty or placeholder options
                index_queue.append(text.strip())
                logger.debug(f"  Added index: {text.strip()}")

    return index_queue

async def extract_index_data(page, index_name, start_date, end_date):
    """Extract historical data for a specific index."""
    # Select the index
    await page.select_option("select#ddlHistoricaltypeindex", index_name)
    await page.wait_for_timeout(1000)  # Small wait for UI to settle

    current_pointer = start_date
    index_data = []

    while current_pointer < end_date:
        chunk_end = current_pointer + timedelta(days=CHUNK_DAYS)
        if chunk_end > end_date:
            chunk_end = end_date

        # Format dates as DD-MM-YYYY for injection
        from_date_str = current_pointer.strftime("%d-%m-%Y")
        to_date_str = chunk_end.strftime("%d-%m-%Y")

        logger.info(f"  Fetching data from {from_date_str} to {to_date_str}")

        # Inject dates directly via JavaScript (bypass calendar widget)
        await page.evaluate(f"""
            document.getElementById('txtFromDate').value = '{from_date_str}';
            document.getElementById('txtToDate').value = '{to_date_str}';
        """)

        # Click Submit button
        await page.click("input#btnHistorical")
        await page.wait_for_selector("#historicalData", timeout=20000)

        # Check for "No Records Found"
        no_records = await page.query_selector("text=No Records Found")
        if no_records:
            logger.info("    No records found for this date range. Skipping.")
            current_pointer = chunk_end + timedelta(days=1)
            continue

        # Parse the table
        table_rows = await page.query_selector_all("#historicalData tbody tr")
        if len(table_rows) == 0:
            logger.warning("    Table found but no rows. Skipping.")
            current_pointer = chunk_end + timedelta(days=1)
            continue

        for row in table_rows:
            cells = await row.query_selector_all("td")
            if len(cells) >= 5:  # Ensure we have at least Date, Open, High, Low, Close
                row_data = {
                    "Index_Name": index_name,
                    "Date": (await cells[0].text_content()).strip(),
                    "Open": (await cells[1].text_content()).strip(),
                    "High": (await cells[2].text_content()).strip(),
                    "Low": (await cells[3].text_content()).strip(),
                    "Close": (await cells[4].text_content()).strip(),
                    # Add more columns if needed (Volume, etc.)
                }
                index_data.append(row_data)

        # Increment pointer
        current_pointer = chunk_end + timedelta(days=1)

    # Save data for this index to a temporary file
    if index_data:
        df = pd.DataFrame(index_data)
        temp_file = os.path.join(OUTPUT_DIR, f"{TEMP_FILE_PREFIX}{index_name.replace(' ', '_')}.csv")
        df.to_csv(temp_file, index=False, encoding='utf-8')
        logger.info(f"  Saved {len(index_data)} records for {index_name} to {temp_file}")

def merge_temp_files():
    """Merge all temporary CSV files into a single master file."""
    all_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith(TEMP_FILE_PREFIX) and f.endswith('.csv')]
    if not all_files:
        logger.warning("No temporary files found to merge.")
        return

    dfs = []
    for file in all_files:
        file_path = os.path.join(OUTPUT_DIR, file)
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            dfs.append(df)
            logger.info(f"Loaded {len(df)} rows from {file}")
        except Exception as e:
            logger.error(f"Failed to load {file}: {e}")

    if dfs:
        master_df = pd.concat(dfs, ignore_index=True)
        master_df.to_csv(MASTER_FILE, index=False, encoding='utf-8')
        logger.info(f"Merged {len(dfs)} files into {MASTER_FILE} with {len(master_df)} total rows.")

        # Optional: Clean up temporary files
        for file in all_files:
            os.remove(os.path.join(OUTPUT_DIR, file))
            logger.debug(f"Deleted temporary file: {file}")

if __name__ == "__main__":
    asyncio.run(scrape_nifty_indices())