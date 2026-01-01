
import os
import sys
import asyncio
import warnings
import datetime
import pandas as pd
from datetime import timedelta, timezone
from pathlib import Path
import shutil

# Add project root to sys.path to ensure absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Local imports
from scripts.common import core as mdc
from scripts.common import playwright_lib as pl
from scripts.common import config as cfg

warnings.filterwarnings('ignore')

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
# Define report types locally to control paths maps to Cloud
REPORT_CONFIG = [
    {
        "name": "Quarterly Balance Sheet",
        "folder": "Balance Sheet",
        "file_suffix": "quarterly_balance-sheet",
        "url_template": 'https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}',
        "period": "quarterly"
    },
    {
        "name": "Quarterly Valuations",
        "folder": "Valuation",
        "file_suffix": "quarterly_valuation_measures",
        "url_template": 'https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}',
        "period": "quarterly"
    },
    {
        "name": "Quarterly Cash Flow",
        "folder": "Cash Flow",
        "file_suffix": "quarterly_cash-flow",
        "url_template": 'https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}',
        "period": "quarterly"
    },
    {
        "name": "Quarterly Income Statement",
        "folder": "Income Statement",
        "file_suffix": "quarterly_financials",
        "url_template": 'https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}',
        "period": "quarterly"
    }
]

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

def transpose_dataframe(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transposes the Yahoo Finance dataframe, sets proper index, and casts types.
    Matched logic from playwright_lib.transpose_yahoo_dataframe but in-memory.
    """
    # Assumption: df matches raw CSV structure from Yahoo
    
    # Set 'name' as index if exists (it's usually the first column)
    if 'name' in df.columns:
        df.set_index("name", inplace=True)
    elif df.columns[0] != 'Date': # If explicit 'name' col missing, assume first col is labels
         df.set_index(df.columns[0], inplace=True)

    # Drop all-NaN rows
    df.dropna(how='all', inplace=True)

    # Handle 'ttm' column
    if 'ttm' in df.columns:
        df = df.rename(columns={"ttm": datetime.date.today().strftime("%m/%d/%Y")})
    
    # Remove commas and cast to float
    # We use regex replace and apply to whole DF
    df = df.replace(',', '', regex=True)
    
    # Try converting to numeric, errors='ignore' (some might remain object if truly text)
    # But Yahoo finance data is mostly numbers.
    # Safe float conversion:
    df = df.apply(pd.to_numeric, errors='coerce').fillna(0)

    # Transpose
    df_transposed = df.T

    # Convert index to datetime
    df_transposed.index = pd.to_datetime(df_transposed.index, errors='coerce')
    
    # Filter out NaT index if any (invalid dates)
    df_transposed = df_transposed[df_transposed.index.notnull()]

    df_transposed['Symbol'] = ticker
    df_transposed.index.name = 'Date'
    
    return df_transposed

async def process_report_cloud(playwright_params, report, blacklist_callback=None):
    """
    Orchestrates: Navigation -> Download (Temp) -> Read -> Transpose -> Upload (Cloud) -> Cleanup.
    """
    playwright, browser, context, page = playwright_params
    ticker = report['ticker']
    
    # Retry logic matching pl library
    max_retries = 3
    retry_counter = 0
    
    # Determine cloud path
    cloud_path = f"Yahoo/{report['folder']}/{ticker}_{report['file_suffix']}.parquet"
    
    # Temp download dir
    temp_dir = Path.home() / "Downloads" / f"temp_{ticker}_{report['period']}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    success = False
    
    try:
        while retry_counter < max_retries:
            retry_counter += 1
            try:
                # 1. Navigation
                await pl.load_url_async(page, report["url"])
                
                # 1b. Check for invalid ticker (Redirection to Symbol Lookup)
                title = await page.title()
                if "Symbol Lookup" in title or "Lookup" in title:
                     mdc.write_line(f"Ticker {ticker} not found (Redirected to {title}). Blacklisting.")
                     if blacklist_callback: blacklist_callback(ticker)
                     break

                # 2. Check tab existence
                selector = f'button#tab-{report["period"]}[role="tab"]'
                exists = await pl.element_exists_async(page, selector)
                
                if not exists:
                    mdc.write_line(f"Skipping {report['name']} for {ticker} (Period tab not found - likely temporary or data missing)")
                    # DO NOT BLACKLIST for missing tab - could be network timeout or layout shift
                    break
                
                # 3. Click Tab
                selectors_tab = [
                    {"property_type": "button", "property_name": "id", "property_value": f"tab-{report['period']}"},
                    {"property_type": "button", "property_name": "title", "property_value": f"{report['period'].capitalize()}"}
                ]
                await pl.pw_click_by_selectors_async(page, selectors_tab)
                
                # 4. Find Download Link
                selectors_dl = [
                    {"property_type": "button", "property_name": "data-testid", "property_value": "download-link"},
                    {"property_type": "button", "property_name": "data-rapid_p", "property_value": "21"}
                ]
                
                # Check for button before trying click
                if await pl.element_exists_async(page, 'button[data-testid="download-link"]'):
                     # 5. Download
                     download_path = await pl.pw_download_after_click_by_selectors_async(page, selectors_dl, str(temp_dir))
                     
                     # Check for file existence with retries
                     file_exists = False
                     for i in range(3):
                         if download_path and os.path.exists(download_path):
                             file_exists = True
                             break
                         if i < 2:
                             await asyncio.sleep(3)

                     if file_exists:
                         mdc.write_line(f"Downloaded {report['name']} for {ticker}")
                         
                         # 6. Read & Transpose
                         try:
                             df = pd.read_csv(download_path)
                             df_clean = transpose_dataframe(df, ticker)
                             
                             # 7. Upload to Azure
                             mdc.store_parquet(df_clean, cloud_path)
                             mdc.write_line(f"Uploaded {cloud_path}")
                             success = True
                             break 
                             
                         except Exception as e:
                             mdc.write_line(f"Error processing CSV for {ticker}: {e}")
                             break
                     else:
                        mdc.write_line(f"Download returned no file for {ticker}")
                else:
                    mdc.write_line(f"No download link for {ticker}")
                    # DO NOT BLACKLIST - might be temp failure
                    break
                    
            except Exception as e:
                mdc.write_line(f"Error taking snapshot for {ticker}: {e}")
                # Refresh page and wait before retry
                try:
                    await page.reload()
                except Exception:
                    pass
                await asyncio.sleep(2)

    finally:
        # Cleanup temp
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
                
    return success


async def run_async_playwright(reports_to_refresh):
    """
    Main async orchestration.
    """
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    
    try:
        # Auth
        await pl.authenticate_yahoo_async(page, context)
        
        # Semaphore for parallel tabs
        semaphore = asyncio.Semaphore(4) 
        
        # Blacklist helper
        black_path = "blacklist_financial.csv"
        def blacklist_ticker(ticker):
            mdc.update_csv_set(black_path, ticker)

        async def fetch_task(report):
            async with semaphore:
                # New page per task for isolation
                task_page = await context.new_page()
                try:
                    params = (playwright, browser, context, task_page)
                    await process_report_cloud(params, report, blacklist_callback=blacklist_ticker)
                except Exception as e:
                    mdc.write_line(f"Task error {report['ticker']}: {e}")
                finally:
                    await task_page.close()

        tasks = [fetch_task(rep) for rep in reports_to_refresh]
        
        if tasks:
            mdc.write_line(f"Starting parallel processing of {len(tasks)} reports...")
            await asyncio.gather(*tasks)
        else:
            mdc.write_line("No reports to refresh.")
            
    finally:
        await context.close()
        await browser.close()
        await playwright.stop()


async def main():
    mdc.write_line(f"Processing Business Data Scraper {mdc.get_current_timestamp_str()}...")

    # Load Universe
    df_symbols = mdc.get_symbols()
    
    # Filter Blacklist
    blacklist_path = "blacklist_financial.csv"
    blacklist_list = mdc.load_ticker_list(blacklist_path)
    # Also usual blacklist
    blacklist_general = mdc.load_ticker_list("blacklist.csv")
    
    full_blacklist = set(blacklist_list + blacklist_general)
    
    if full_blacklist:
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(full_blacklist)]

    # Apply Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Restricting execution to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]        

    reports_to_process = []
    
    mdc.write_line("Generating report list and checking freshness...")
    
    # Check freshness
    freshness_threshold_days = 28
    now_utc = datetime.datetime.now(timezone.utc)
    
    for symbol in df_symbols['Symbol'].tolist():
        # Iterate our supported report types
        for cfg_report in REPORT_CONFIG:
            
            # Construct report object
            report = cfg_report.copy()
            report['ticker'] = symbol
            report['url'] = report['url_template'].format(ticker=symbol)
            
            # Check Cloud
            cloud_path = f"Yahoo/{report['folder']}/{symbol}_{report['file_suffix']}.parquet"
            
            should_refresh = True
            if mdc.storage_client:
                last_mod = mdc.storage_client.get_last_modified(cloud_path)
                if last_mod:
                    # Ensure UTC
                    if last_mod.tzinfo is None:
                        last_mod = last_mod.replace(tzinfo=timezone.utc)
                    
                    age = now_utc - last_mod
                    if age.days < freshness_threshold_days:
                        should_refresh = False
            
            if should_refresh:
                reports_to_process.append(report)
    
    mdc.write_line(f"Found {len(reports_to_process)} reports to process/refresh.")

    if reports_to_process:
        await run_async_playwright(reports_to_process)


if __name__ == "__main__":
    asyncio.run(main())
