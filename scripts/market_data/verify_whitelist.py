import asyncio
import os
import sys
import pandas as pd
from datetime import datetime
from playwright.async_api import async_playwright

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.market_data import core
from scripts.common import playwright_lib as pl

async def verify():
    print("Starting whitelist verification...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Test with SPY which is in whitelist
        ticker = "SPY"
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
        ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
        period1 = int(datetime(2023, 1, 1).timestamp())
        
        print(f"Testing ticker: {ticker}")
        await core.download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1)
        
        await browser.close()
    print("Verification complete.")

if __name__ == "__main__":
    asyncio.run(verify())
