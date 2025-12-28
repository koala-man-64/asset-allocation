import os
import sys
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime
import warnings

# Add project root to sys.path to ensure absolute imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import playwright_lib as pl
from scripts.market_data import core as malib

# Suppress warnings
warnings.filterwarnings('ignore')

async def main_async():
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    await pl.pw_load_cookies_async(context, str(pl.COMMON_DIR / 'pw_cookies.json'))
    await page.reload()
    await pl.pw_login_to_yahoo_async(page, context)
    await pl.pw_save_cookies_async(context, str(pl.COMMON_DIR / 'pw_cookies.json'))

    # Params
    lookback_bars = 475
    drop_prior = True
    get_latest = False
    
    # Get Symbols
    df_symbols = malib.get_symbols()
    
    # Refresh Data
    df_history = await malib.refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context)
    
    if df_history is not None and not df_history.empty:
        print(f"Data refresh/load complete. Loaded {len(df_history)} rows.")
    else:
        print("Data refresh complete (no new data loaded or returned).")
    
    await browser.close()
    await playwright.close()
    
if __name__ == "__main__":
    asyncio.run(main_async())
