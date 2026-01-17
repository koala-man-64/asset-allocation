
import os
import sys
import asyncio
import warnings
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import playwright_lib as pl
from scripts.common import core as mdc
from scripts.market_data import config as cfg
from scripts.common.pipeline import ListManager

# Suppress warnings
warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
# Usage of silver_client for ListManager to maintain state with existing lists
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER) 
list_manager = ListManager(silver_client, "market-data")

def _validate_environment():
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")

async def download_and_save_raw(ticker, page):
    if list_manager.is_whitelisted(ticker):
        mdc.write_line(f'{ticker} is in whitelist, skipping validation')
    elif list_manager.is_blacklisted(ticker):
        mdc.write_line(f'{ticker} is in blacklist, skipping')
        return

    # Check existence on Yahoo (Validation)
    if not list_manager.is_whitelisted(ticker):
        try:
            quote_url = f'https://finance.yahoo.com/quote/{ticker}/'
            await page.goto(quote_url)
            page_title = await page.title()
            if "Symbol Lookup" in page_title or "Lookup" in page_title:
                mdc.write_line(f"Ticker {ticker} not found on Yahoo. Blacklisting.")
                list_manager.add_to_blacklist(ticker)
                return
        except Exception as e:
            mdc.write_error(f"Error checking ticker {ticker}: {e}")
            return

    # Download
    try:
        # Default to 10 years or max
        period1 = int((datetime.today() - relativedelta(years=10)).timestamp())
        url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker.replace(".", "-")}?period1={period1}&period2={cfg.YAHOO_MAX_PERIOD}&interval=1d&events=history'
        
        download_path = await pl.download_yahoo_price_data_async(page, url)
        
        if os.path.exists(download_path):
            with open(download_path, 'rb') as f:
                raw_bytes = f.read()
            
            # Save to Bronze with Timestamp to allow history/debugging? 
            # Or just overwrite "latest"? 
            # Medallion often keeps history. 
            # For now, let's Stick to {ticker}.csv as "Latest Raw" to match current logic, 
            # but ideally we'd partition by date. 
            # The current requirement is "Bronze -> Silver". 
            # I will save as `market-data/{ticker}.csv` matching existing logic 
            # to minimize disruption, but arguably this should be `market-data/{date}/{ticker}.csv`.
            # implementation_plan says: "Bronze (raw, symbol-partitioned, latest-only snapshot)"
            # So overwriting is correct for "snapshot".
            
            mdc.store_raw_bytes(raw_bytes, f"market-data/{ticker}.csv", client=bronze_client)
            mdc.write_line(f"Saved raw {ticker}.csv to Bronze.")
            
            # Cleanup local
            os.remove(download_path)
            
            # Whitelist on success
            list_manager.add_to_whitelist(ticker)
        else:
            mdc.write_line(f"Download failed for {ticker}. Adding to blacklist.")
            list_manager.add_to_blacklist(ticker)
            
    except Exception as e:
        mdc.write_error(f"Error downloading {ticker}: {e}")
        # Add basic error handling/blacklist logic if needed

async def main_async():
    mdc.log_environment_diagnostics()
    _validate_environment()
    
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    await pl.authenticate_yahoo_async(page, context)
    
    mdc.write_line("Fetching symbol universe...")
    df_symbols = mdc.get_symbols()
    
    # Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Restricting to {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]
    
    # Load Lists
    list_manager.load()
    
    symbols = [row['Symbol'] for _, row in df_symbols.iterrows() if '.' not in row['Symbol']]
    
    mdc.write_line(f"Starting Bronze Ingestion for {len(symbols)} symbols...")
    
    semaphore = asyncio.Semaphore(3)
    
    async def process(symbol):
        async with semaphore:
            page = await context.new_page()
            try:
                await download_and_save_raw(symbol, page)
            finally:
                await page.close()

    tasks = [process(sym) for sym in symbols]
    await asyncio.gather(*tasks)
    
    await browser.close()
    await playwright.stop()
    mdc.write_line("Bronze Ingestion Complete.")

if __name__ == "__main__":
    job_name = 'market-data-job-bronze'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
