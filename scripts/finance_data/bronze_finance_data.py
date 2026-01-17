
import os
import sys
import asyncio
import warnings
import datetime
from pathlib import Path
import shutil
from datetime import timezone

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import core as mdc
from scripts.common import playwright_lib as pl
from scripts.finance_data import config as cfg
from scripts.common.pipeline import DataPaths, ListManager

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)
list_manager = ListManager(silver_client, "finance-data")

REPORT_CONFIG = [
    {"name": "Quarterly Balance Sheet", "folder": "Balance Sheet", "file_suffix": "quarterly_balance-sheet", "url_template": 'https://finance.yahoo.com/quote/{ticker}/balance-sheet?p={ticker}', "period": "quarterly"},
    {"name": "Quarterly Valuations", "folder": "Valuation", "file_suffix": "quarterly_valuation_measures", "url_template": 'https://finance.yahoo.com/quote/{ticker}/key-statistics?p={ticker}', "period": "quarterly"},
    {"name": "Quarterly Cash Flow", "folder": "Cash Flow", "file_suffix": "quarterly_cash-flow", "url_template": 'https://finance.yahoo.com/quote/{ticker}/cash-flow?p={ticker}', "period": "quarterly"},
    {"name": "Quarterly Income Statement", "folder": "Income Statement", "file_suffix": "quarterly_financials", "url_template": 'https://finance.yahoo.com/quote/{ticker}/financials?p={ticker}', "period": "quarterly"}
]

def _validate_environment() -> None:
    required = ["AZURE_CONTAINER_BRONZE", "DOWNLOADS_PATH", "PLAYWRIGHT_USER_DATA_DIR", "YAHOO_USERNAME", "YAHOO_PASSWORD"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing env vars: " + ", ".join(missing))

async def download_report(playwright_params, ticker, report, client):
    playwright, browser, context, page = playwright_params
    
    # 1. Check Freshness of Bronze Blob
    raw_blob_path = f"finance-data/{report['folder']}/{ticker}_{report['file_suffix']}.csv"
    
    # Check if exists and fresh 
    # Use client.get_blob_client(raw_blob_path).get_blob_properties()
    try:
        blob_client = client.get_blob_client(blob=raw_blob_path)
        if blob_client.exists():
            props = blob_client.get_blob_properties()
            last_modified = props.last_modified
            age = datetime.datetime.now(timezone.utc) - last_modified
            if age.days < 28:
                # Fresh enough for quarterly data
                return
    except Exception:
        pass # Force download if check fails
    
    # 2. Download
    try:
        await pl.load_url_async(page, report["url"])
        
        # Validation
        if list_manager.is_whitelisted(ticker):
            pass
        else:
            title = await page.title()
            if "Symbol Lookup" in title or "Lookup" in title:
                mdc.write_line(f"Ticker {ticker} not found. Blacklisting.")
                list_manager.add_to_blacklist(ticker)
                return

        # Check tab existence
        selector = f'button#tab-{report["period"]}[role="tab"]'
        if not await pl.element_exists_async(page, selector):
            mdc.write_line(f"Tab not found for {ticker}")
            return
            
        # Click Tab
        selectors_tab = [
            {"property_type": "button", "property_name": "id", "property_value": f"tab-{report['period']}"},
            {"property_type": "button", "property_name": "title", "property_value": f"{report['period'].capitalize()}"}
        ]
        await pl.pw_click_by_selectors_async(page, selectors_tab)
        
        # Find Download
        selectors_dl = [
            {"property_type": "button", "property_name": "data-testid", "property_value": "download-link"},
            {"property_type": "button", "property_name": "data-rapid_p", "property_value": "21"}
        ]
        
        if await pl.element_exists_async(page, 'button[data-testid="download-link"]'):
            temp_dir = Path.home() / "Downloads" / f"temp_{ticker}_{report['period']}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            
            try:
                download_path = await pl.pw_download_after_click_by_selectors_async(page, selectors_dl, str(temp_dir))
                
                await asyncio.sleep(2) # Wait for file write
                
                if download_path and os.path.exists(download_path):
                     with open(download_path, 'rb') as f:
                         raw_bytes = f.read()
                     mdc.store_raw_bytes(raw_bytes, raw_blob_path, client=client)
                     mdc.write_line(f"Saved raw {ticker} {report['name']} to Bronze.")
                     
                     list_manager.add_to_whitelist(ticker)
                else:
                    mdc.write_line(f"Download returned no file for {ticker}")
            finally:
                if os.path.exists(temp_dir):
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    
        else:
            mdc.write_line(f"No download link for {ticker}")
            
    except Exception as e:
        mdc.write_error(f"Error processing {ticker} {report['name']}: {e}")

async def main_async():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    mdc.log_environment_diagnostics()
    _validate_environment()
    
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    await pl.authenticate_yahoo_async(page, context)
    
    list_manager.load()
    
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    
    # Filter
    symbols = [
        row['Symbol'] for _, row in df_symbols.iterrows() 
        if '.' not in str(row['Symbol']) and not list_manager.is_blacklisted(row['Symbol'])
    ]
    
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        
    mdc.write_line(f"Starting Bronze Finance Ingestion for {len(symbols)} symbols...")

    semaphore = asyncio.Semaphore(1)
    
    async def process_symbol(symbol):
        async with semaphore:
             # Iterate reports
             for cfg_report in REPORT_CONFIG:
                 report = cfg_report.copy()
                 report['ticker'] = symbol
                 report['url'] = report['url_template'].format(ticker=symbol)
                 
                 task_page = await context.new_page()
                 try:
                     params = (playwright, browser, context, task_page)
                     await download_report(params, symbol, report, bronze_client)
                 finally:
                     await task_page.close()

    tasks = [process_symbol(sym) for sym in symbols]
    await asyncio.gather(*tasks)
    
    await browser.close()
    await playwright.stop()
    mdc.write_line("Bronze Finance Ingestion Complete.")

if __name__ == "__main__":
    job_name = 'finance-data-job-bronze'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
