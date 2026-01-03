
import sys
import os
import asyncio
import warnings
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

# Add project root to sys.path
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

async def fetch_earnings_for_symbol(symbol: str, context, semaphore, whitelist_set: set, blacklist_callback, whitelist_callback):
    """
    Fetches earnings data for a single symbol.
    """
    async with semaphore:
        # Check whitelist - if present, we might skip some checks, but for earnings we mostly just fetch.
        # But we can log it.
        if symbol in whitelist_set:
            mdc.write_line(f"{symbol} is in whitelist.")
            
        # Cloud path: bronze/earnings/{symbol}
        cloud_path = f"bronze/earnings/{symbol}"
        
        # Check Freshness via Delta Metadata
        last_ts = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_EARNINGS, cloud_path)
        
        today = pd.to_datetime(datetime.now().date())
        one_year_ago = today - pd.DateOffset(years=1)
        
        # Load existing to check logic
        df_symbol_earnings = None
        if last_ts:
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
                
                # Whitelist on success
                if whitelist_callback:
                    whitelist_callback(symbol)
            else:
                mdc.write_line(f"No earnings data found for {symbol}")
                
        except ValueError as ve:
            if "Symbol not found" in str(ve):
                mdc.write_line(f"Blacklisting {symbol} (detected invalid/no-data).")
                if blacklist_callback:
                    blacklist_callback(symbol)
            else:
                mdc.write_line(f"ValueError for {symbol}: {ve}")
        except Exception as e:
            mdc.write_line(f"Error retrieving earnings data for {symbol}: {str(e)}")
        finally:                
            await page.close()

async def run_earnings_refresh(df_symbols: pd.DataFrame):
    """
    Main orchestration function for earnings refresh.
    """
    # Initialize playwright objects
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    
    try:
        # Paths (Configured)
        cookies_path = cfg.USER_DATA_DIR / "pw_cookies_yahoo.json" 
        
        if cookies_path.exists():
             await pl.pw_load_cookies_async(context, str(cookies_path))
        
        await page.reload() 
        
        client = get_client()

        # Load Blacklist
        blacklist_path = "earnings_data_blacklist.csv"
        blacklist_list = mdc.load_ticker_list(blacklist_path, client=client)
        full_blacklist = set(blacklist_list)
        
        def blacklist_ticker(ticker):
            mdc.update_csv_set(blacklist_path, ticker, client=client)

        # Load Whitelist
        whitelist_path = "earnings_data_whitelist.csv"
        whitelist_list = mdc.load_ticker_list(whitelist_path, client=client)
        whitelist_set = set(whitelist_list)

        def whitelist_ticker(ticker):
            mdc.update_csv_set(whitelist_path, ticker, client=client)

        # Filter symbols
        symbols = [
                row['Symbol'] 
                for _, row in df_symbols.iterrows() 
                if '.' not in str(row['Symbol']) and row['Symbol'] not in full_blacklist
        ]
        
        if full_blacklist:
            mdc.write_line(f"Filtered {len(full_blacklist)} blacklisted symbols.")
        
        # Debug override
        if cfg.DEBUG_SYMBOLS:
            mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
            symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
            
        semaphore = asyncio.Semaphore(3) # Limit concurrency
        
        mdc.write_line(f"Starting fetch for {len(symbols)} symbols...")
        tasks = [fetch_earnings_for_symbol(sym, context, semaphore, whitelist_set, blacklist_ticker, whitelist_ticker) for sym in symbols]
        await asyncio.gather(*tasks, return_exceptions=True)
        
    finally:
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
