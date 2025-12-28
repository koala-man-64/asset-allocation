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
    # Remove QQQ from whitelist if it exists to test adding it
    white_path = str(pl.COMMON_DIR / 'whitelist.csv')
    try:
        df = pd.read_csv(white_path)
        if 'QQQ' in df['Symbol'].values:
            df = df[df['Symbol'] != 'QQQ']
            df.to_csv(white_path, index=False)
            print("Removed QQQ from whitelist for testing.")
    except Exception as e:
        print(f"Pre-test clean failed (maybe file empty): {e}")

    print("Starting auto-whitelist verification...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Test with QQQ - valid ticker
        ticker = "QQQ"
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
        ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
        period1 = int(datetime(2023, 1, 1).timestamp())
        
        print(f"Fetching ticker: {ticker}")
        await core.download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1)
        
        await browser.close()
    
    # Check if QQQ is now in whitelist
    try:
        df = pd.read_csv(white_path)
        if 'QQQ' in df['Symbol'].values:
            print("SUCCESS: QQQ was automatically added to whitelist.")
        else:
            print("FAILURE: QQQ was NOT added to whitelist.")
    except Exception as e:
        print(f"Verification check failed: {e}")

if __name__ == "__main__":
    asyncio.run(verify())
