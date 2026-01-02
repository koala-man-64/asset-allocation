
import sys
import os
import asyncio
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

# Local imports
# Adjust path to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from scripts.common import core as mdc
from scripts.common import config as cfg
from scripts.common import playwright_lib as pl
from scripts.common import delta_core

warnings.filterwarnings('ignore')

# Initialize Client
_earn_client = None

def get_client():
    """Lazy loader for the Azure Storage Client."""
    global _earn_client
    if _earn_client is None:
        _earn_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_EARNINGS)
    return _earn_client

async def main_async(df_symbols: pd.DataFrame):
     # Initialize playwright objects
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    
    # Paths (Configured)
    cookies_path = cfg.USER_DATA_DIR / "pw_cookies_yahoo.json" 
    
    if cookies_path.exists():
         await pl.pw_load_cookies_async(context, str(cookies_path))
    
    await page.reload()
    
    # Filter out symbols containing dots
    symbols = [
            row['Symbol'] 
            for _, row in df_symbols.iterrows() 
            if '.' not in str(row['Symbol'])
    ]
    
    # Debug override
    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        
    # Setup dates to scan for earnings
    today = pd.to_datetime(datetime.now().date())
    one_year_ago = today - pd.DateOffset(years=1)
    
    semaphore = asyncio.Semaphore(3) # Limit concurrency
    
    async def fetch(symbol):
        async with semaphore:
            # Cloud path: bronze/earnings/{symbol}
            # Note: We use snake_case folder structure as per Architecture Refactor
            cloud_path = f"bronze/earnings/{symbol}"
            
            # Check Freshness via Delta Metadata
            last_ts = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_EARNINGS, cloud_path)
            
            # Load existing to check logic
            df_symbol_earnings = None
            if last_ts:
                # Optimized: We could skip load if very fresh, but we need to check 'missing earnings' logic
                # For now, let's load it.
                df_symbol_earnings = delta_core.load_delta(cfg.AZURE_CONTAINER_EARNINGS, cloud_path)

            should_fetch = True
            
            if df_symbol_earnings is not None and not df_symbol_earnings.empty:
                if 'Date' in df_symbol_earnings.columns:
                    df_symbol_earnings['Date'] = pd.to_datetime(df_symbol_earnings['Date'], errors='coerce')
                    
                    # Logic: If date is passed and no reported EPS/Surprise, and date is recent enough.
                    dates_without_earnings = df_symbol_earnings[
                        (df_symbol_earnings['Date'] < today) &          
                        (pd.isna(df_symbol_earnings.get('Reported EPS', np.nan))) & 
                        (pd.isna(df_symbol_earnings.get('Surprise', np.nan))) &     
                        (df_symbol_earnings['Date'] >= one_year_ago)    
                    ]
                    
                    # Freshness check (simplistic: if file updated < 24h ago and no holes, skip)
                    now_ts = datetime.now(timezone.utc).timestamp()
                    if len(dates_without_earnings) == 0 and (now_ts - last_ts < 24 * 3600):
                        mdc.write_line(f"Skipping {symbol}: upcoming earnings already recorded / recently checked")
                        should_fetch = False

            if not should_fetch:
                return
        
            page = await context.new_page()
            try:
                # Retrieve earnings data
                df_new = await pl.get_yahoo_earnings_data(page, symbol, timeout=30000)
                
                if df_new is not None and not df_new.empty:
                    # Cleanup columns
                    df_new = df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in col], errors='ignore')
                    
                    # Save to Cloud (Delta)
                    delta_core.store_delta(df_new, cfg.AZURE_CONTAINER_EARNINGS, cloud_path)
                    mdc.write_line(f"Saved earnings for {symbol} to {cfg.AZURE_CONTAINER_EARNINGS}/{cloud_path}")
                else:
                    mdc.write_line(f"No earnings data found for {symbol}")
            except Exception as e:
                mdc.write_line(f"Error retrieving earnings data for {symbol}: {str(e)}")
            finally:                
                await page.close()
            
    # kick off all fetches
    mdc.write_line(f"Starting fetch for {len(symbols)} symbols...")
    tasks  = [fetch(sym) for sym in symbols]
    await asyncio.gather(*tasks, return_exceptions=True)
           
    if browser:
        await browser.close()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if not cfg.AZURE_CONTAINER_EARNINGS:
        raise ValueError("Environment variable 'AZURE_CONTAINER_EARNINGS' is strictly required for Earnings Data Scraper.")

    # Load symbols
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    
    # Filter blacklisted
    blacklist_file = "earnings_data_blacklist.csv"
    blacklist_list = mdc.load_ticker_list(blacklist_file, client=get_client())
    if blacklist_list:
         mdc.write_line(f"Filtering out {len(blacklist_list)} blacklisted symbols.")
         df_symbols = df_symbols[~df_symbols['Symbol'].isin(blacklist_list)]

    try:
        asyncio.run(main_async(df_symbols))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Main loop error: {e}")
