import asyncio
import sys
import os
import pandas as pd
from scripts.market_data import core as malib
from scripts.common import playwright_lib as pl

# Mock symbols
async def run_test():
    print("Starting Test Run...")
    
    # 1. Setup Browser
    playwright, browser, context, page = await pl.get_playwright_browser(headless=True, use_async=True)
    
    # 2. Mock Symbols
    df_symbols = pd.DataFrame([
        {'Symbol': 'SPY'},
        {'Symbol': 'AAPL'},
        {'Symbol': 'MSFT'}
    ])
    
    # 3. Refresh Data
    print(f"Testing with {len(df_symbols)} symbols...")
    await malib.refresh_stock_data_async(df_symbols, 10, True, False, browser, page, context)
    
    print("Test Run Complete.")
    await browser.close()
    await playwright.close()

if __name__ == "__main__":
    asyncio.run(run_test())
