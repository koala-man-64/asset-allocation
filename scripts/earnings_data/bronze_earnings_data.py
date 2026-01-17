
import sys
import os
import asyncio
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Adjust path to find 'scripts' when running directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import core as mdc
from scripts.earnings_data import config as cfg
from scripts.common import playwright_lib as pl
from scripts.common.pipeline import ListManager

warnings.filterwarnings('ignore')

# Initialize Client
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "earnings-data")

def _validate_environment() -> None:
    required = [
        "AZURE_CONTAINER_EARNINGS", # Wait, environment uses this specific var? 
        "DOWNLOADS_PATH", 
        "PLAYWRIGHT_USER_DATA_DIR",
        "YAHOO_USERNAME",
        "YAHOO_PASSWORD",
        "AZURE_CONTAINER_BRONZE"
    ]
    # Check logic...
    if not cfg.AZURE_CONTAINER_BRONZE:
        raise ValueError("Environment variable 'AZURE_CONTAINER_BRONZE' is strictly required.")

async def fetch_and_save_raw(symbol: str, context, semaphore):
    async with semaphore:
        if list_manager.is_blacklisted(symbol):
            return

        page = await context.new_page()
        try:
            # Check whitelist - if present, skip some validation? 
            # For earnings, we rely on pl.get_yahoo_earnings_data return value.
            
            # Retrieve earnings data
            # Note: get_yahoo_earnings_data navigates and scrapes.
            df_new = await pl.get_yahoo_earnings_data(page, symbol, timeout=30000)
            
            if df_new is not None and not df_new.empty:
                # Cleanup columns (minimal for Bronze, but removing Unnamed is good)
                df_new = df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in col], errors='ignore')
                
                # Save Raw to Bronze (Snapshot)
                # Using JSON for earnings data as it's often structured or sparse, 
                # but Market was CSV. 
                # Original code: df_new.to_json(orient='records')
                try:
                     raw_json = df_new.to_json(orient='records').encode('utf-8')
                     mdc.store_raw_bytes(raw_json, f"earnings-data/{symbol}.json", client=bronze_client)
                     mdc.write_line(f"Saved raw earnings for {symbol} to Bronze.")
                     
                     list_manager.add_to_whitelist(symbol)
                except Exception as e:
                     mdc.write_error(f"Failed to save raw bronze for {symbol}: {e}")

            else:
                mdc.write_line(f"No earnings data found for {symbol}")
                # Possibly blacklist if strictly not found vs just empty?
                # Original code checks "Symbol not found" in ValueError.
                
        except ValueError as ve:
            if "Symbol not found" in str(ve):
                mdc.write_line(f"Blacklisting {symbol} (detected invalid/no-data).")
                list_manager.add_to_blacklist(symbol)
            else:
                mdc.write_line(f"ValueError for {symbol}: {ve}")
        except Exception as e:
            mdc.write_line(f"Error retrieving earnings data for {symbol}: {str(e)}")
        finally:                
            await page.close()

async def main_async():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    mdc.log_environment_diagnostics()
    _validate_environment()
    
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(headless=None, use_async=True)
    
    await pl.authenticate_yahoo_async(page, context)
    
    list_manager.load()
    
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    
    symbols = [
        row['Symbol'] 
        for _, row in df_symbols.iterrows() 
        if '.' not in str(row['Symbol']) and not list_manager.is_blacklisted(row['Symbol'])
    ]
    
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]

    mdc.write_line(f"Starting Bronze Earnings Ingestion for {len(symbols)} symbols...")
    
    semaphore = asyncio.Semaphore(3)
    tasks = [fetch_and_save_raw(sym, context, semaphore) for sym in symbols]
    await asyncio.gather(*tasks, return_exceptions=True)
    
    await browser.close()
    await playwright.stop()
    mdc.write_line("Bronze Ingestion Complete.")

if __name__ == "__main__":
    job_name = 'bronze-earnings-job-bronze'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
