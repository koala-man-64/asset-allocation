
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

# Suppress warnings
warnings.filterwarnings('ignore')

async def main_async():
    # 1. Setup Browser
    print("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    
    # 2. Authenticate
    print("Loading cookies and logging in...")
    
    cookies_path = "pw_cookies.json"
    cookies_data = mdc.get_json_content(cookies_path)
    if cookies_data:
        await context.add_cookies(cookies_data)
        
    await page.reload()
    await pl.pw_login_to_yahoo_async(page, context)
    
    new_cookies = await context.cookies()
    mdc.save_json_content(new_cookies, cookies_path)

    # Params
    lookback_bars = 475
    drop_prior = True
    get_latest = False
    
    # 3. Get Universe of Symbols
    print("Fetching symbol universe...")
    # This involves Azure cache check or Nasdaq API call
    df_symbols = malib.get_symbols()
    
    # Apply Debug Filter
    from scripts.common import config as cfg
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        print(f"⚠️ DEBUG MODE: Restricting execution to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]
    
    # 4. Refresh Data
    print(f"Starting data refresh for {len(df_symbols)} symbols...")
    # This orchestrates the concurrent download and Azure upload
    df_history = await malib.refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context)
    
    if df_history is not None and not df_history.empty:
        print(f"Data refresh/load complete. Loaded {len(df_history)} rows.")
    else:
        print("Data refresh complete (no new data loaded or returned).")
    
    await browser.close()
    await playwright.close()
    
if __name__ == "__main__":
    asyncio.run(main_async())
