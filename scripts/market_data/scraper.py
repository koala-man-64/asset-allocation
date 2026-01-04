
import os
import sys
import asyncio
from playwright.async_api import async_playwright
import warnings

# Add project root to sys.path to ensure absolute imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import playwright_lib as pl
from scripts.common import core as mdc
from scripts.market_data import core as malib
from scripts.common import config as cfg

def _validate_environment():
    if not cfg.AZURE_CONTAINER_MARKET:
        raise ValueError("Environment variable 'AZURE_CONTAINER_MARKET' is strictly required for Market Data Scraper.")

# Suppress warnings
warnings.filterwarnings('ignore')

async def main_async():
    mdc.log_environment_diagnostics()
    _validate_environment()
    # 1. Setup Browser
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    
    # 2. Authenticate
    await pl.authenticate_yahoo_async(page, context)

    # Params
    lookback_bars = 475
    drop_prior = True
    get_latest = False
    
    # 3. Get Universe of Symbols
    mdc.write_line("Fetching symbol universe...")
    # This involves Azure cache check or Nasdaq API call
    df_symbols = mdc.get_symbols()
    
    # Apply Debug Filter
    from scripts.common import config as cfg
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Restricting execution to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]
    
    # 4. Refresh Data
    mdc.write_line(f"Starting data refresh for {len(df_symbols)} symbols...")
    # This orchestrates the concurrent download and Azure upload
    df_history = await malib.refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context)
    
    if df_history is not None and not df_history.empty:
        mdc.write_line(f"Data refresh/load complete. Loaded {len(df_history)} rows.")
    else:
        mdc.write_line("Data refresh complete (no new data loaded or returned).")
    
    await browser.close()
    await playwright.stop()
    
if __name__ == "__main__":
    job_name = 'market-data-job'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
