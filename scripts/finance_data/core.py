
import os
import sys
import asyncio
import warnings
import datetime
import pandas as pd
from datetime import timedelta, timezone
from pathlib import Path
import shutil
import logging

# Add project root to sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Local imports
from scripts.common import core as mdc
from scripts.common import playwright_lib as pl
from scripts.common import config as cfg
from scripts.common import delta_core

warnings.filterwarnings('ignore')

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
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
# Client Management
# ------------------------------------------------------------------------------
fin_client = None

def _require_fin_client():
    global fin_client
    if fin_client is None:
        fin_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_FINANCE)
    if fin_client is None:
        raise RuntimeError("Finance storage client failed to initialize.")
    return fin_client

# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

def transpose_dataframe(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transposes the Yahoo Finance dataframe, sets proper index, and casts types.
    """
    if 'name' in df.columns:
        df.set_index("name", inplace=True)
    elif df.columns[0] != 'Date':
         df.set_index(df.columns[0], inplace=True)

    df.dropna(how='all', inplace=True)

    if 'ttm' in df.columns:
        df = df.rename(columns={"ttm": datetime.date.today().strftime("%m/%d/%Y")})
    
    df = df.replace(',', '', regex=True)
    df = df.apply(pd.to_numeric, errors='coerce').fillna(0)

    df_transposed = df.T
    df_transposed.index = pd.to_datetime(df_transposed.index, errors='coerce')
    df_transposed = df_transposed[df_transposed.index.notnull()]

    df_transposed['Symbol'] = ticker
    df_transposed.index.name = 'Date'
    
    return df_transposed

async def save_debug_artifacts(page, ticker, context_name, client):
    """
    Captures screenshot andHTML content for debugging.
    """
    try:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        debug_dir = cfg.BASE_DIR / "debug_dumps"
        debug_dir.mkdir(parents=True, exist_ok=True)
        
        base_name = f"{ticker}_{context_name}_{timestamp}"
        
        # Screenshot
        screenshot_path = debug_dir / f"{base_name}.png"
        await page.screenshot(path=str(screenshot_path))
        mdc.write_line(f"Saved screenshot: {screenshot_path}")
        mdc.store_file(str(screenshot_path), f"debug_dumps/{base_name}.png", client=client)

        # HTML
        html_path = debug_dir / f"{base_name}.html"
        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)
        mdc.write_line(f"Saved HTML dump: {html_path}")
        mdc.store_file(str(html_path), f"debug_dumps/{base_name}.html", client=client)
        
    except Exception as e:
        mdc.write_error(f"Failed to save debug artifacts: {e}")

async def process_report_cloud(playwright_params, report, client, blacklist_callback=None, whitelist_set=None, whitelist_callback=None):
    """
    Orchestrates: Navigation -> Download (Temp) -> Read -> Transpose -> Upload (Cloud) -> Cleanup.
    """
    playwright, browser, context, page = playwright_params
    ticker = report['ticker']
    max_retries = 3
    retry_counter = 0
    
    cloud_path = f"bronze/{report['folder'].lower().replace(' ', '_')}/{ticker}_{report['file_suffix']}"
    temp_dir = Path.home() / "Downloads" / f"temp_{ticker}_{report['period']}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    success = False
    
    try:
        while retry_counter < max_retries:
            retry_counter += 1
            try:
                # 1. Navigation
                await pl.load_url_async(page, report["url"])
                
                # Check for invalid ticker
                if whitelist_set and ticker in whitelist_set:
                     mdc.write_line(f"{ticker} is in whitelist, skipping validation")
                else:
                    title = await page.title()
                    if "Symbol Lookup" in title or "Lookup" in title:
                         mdc.write_line(f"Ticker {ticker} not found (Redirected to {title}). Blacklisting.")
                         if blacklist_callback: blacklist_callback(ticker)
                         break

                # 2. Check tab existence
                selector = f'button#tab-{report["period"]}[role="tab"]'
                exists = await pl.element_exists_async(page, selector)
                
                if not exists:
                    mdc.write_line(f"Skipping {report['name']} for {ticker} (Period tab not found)")
                    await save_debug_artifacts(page, ticker, f"missing_tab_{report['period']}", client=client)
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
                
                if await pl.element_exists_async(page, 'button[data-testid="download-link"]'):
                     # 5. Download
                     download_path = await pl.pw_download_after_click_by_selectors_async(page, selectors_dl, str(temp_dir))
                     
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
                             
                             # 7. Upload to Azure (Delta)
                             delta_core.store_delta(df_clean, cfg.AZURE_CONTAINER_FINANCE, cloud_path)
                             mdc.write_line(f"Uploaded {cloud_path} (Delta)")
                             
                             if whitelist_callback: whitelist_callback(ticker)
                             
                             success = True
                             break 
                         except Exception as e:
                             mdc.write_error(f"Error processing CSV for {ticker}: {e}")
                             break
                     else:
                        mdc.write_line(f"Download returned no file for {ticker}")
                else:
                    mdc.write_line(f"No download link for {ticker} - {report['name']}")
                    await save_debug_artifacts(page, ticker, "missing_download_link", client=client)
                    break
                    
            except Exception as e:
                mdc.write_error(f"Error taking snapshot for {ticker}: {e}")
                try:
                    await page.reload()
                except Exception:
                    pass
                await asyncio.sleep(2)

    finally:
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass
                
    return success

async def _run_async_playwright(reports_to_refresh):
    """
    Internal execution of playwright tasks.
    """
    mdc.write_line("Initializing Playwright...")
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    
    try:
        await pl.authenticate_yahoo_async(page, context)
        
        semaphore = asyncio.Semaphore(1) 
        client = _require_fin_client()

        black_path = "finance_data_blacklist.csv"
        def blacklist_ticker(ticker):
            mdc.update_csv_set(black_path, ticker, client=client)

        white_path = "finance_data_whitelist.csv"
        whitelist_list = mdc.load_ticker_list(white_path, client=client)
        whitelist_set = set(whitelist_list)
        
        def whitelist_ticker(ticker):
            mdc.update_csv_set(white_path, ticker, client=client)

        async def fetch_task(report):
            async with semaphore:
                task_page = await context.new_page()
                try:
                    params = (playwright, browser, context, task_page)
                    await process_report_cloud(
                        params, 
                        report,
                        client,
                        blacklist_callback=blacklist_ticker,
                        whitelist_set=whitelist_set,
                        whitelist_callback=whitelist_ticker
                    )
                except Exception as e:
                    mdc.write_error(f"Task error {report['ticker']}: {e}")
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

# ------------------------------------------------------------------------------
# Main Logic (Callable from Scraper)
# ------------------------------------------------------------------------------

async def refresh_finance_data_async(df_symbols: pd.DataFrame):
    """
    Main entry point for finance data refresh logic.
    Identifies reports to refresh and executes orchestrator.
    """
    client = _require_fin_client()
    mdc.write_line("Generating report list and checking freshness...")
    
    # Load Blacklist
    blacklist_path = "finance_data_blacklist.csv"
    blacklist_list = mdc.load_ticker_list(blacklist_path, client=client)
    full_blacklist = set(blacklist_list)
    
    if full_blacklist:
        mdc.write_line(f"Filtering out {len(full_blacklist)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(full_blacklist)]

    reports_to_process = []
    
    # Check freshness
    freshness_threshold_days = 28
    now_utc = datetime.datetime.now(timezone.utc)
    
    for symbol in df_symbols['Symbol'].tolist():
        # Iterate supported report types
        for cfg_report in REPORT_CONFIG:
            
            report = cfg_report.copy()
            report['ticker'] = symbol
            report['url'] = report['url_template'].format(ticker=symbol)
            
            cloud_path = f"{report['folder'].lower()}/{symbol}_{report['file_suffix']}"
            
            should_refresh = True
            
            # Use delta_core for freshness
            last_ts = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_FINANCE, cloud_path)
            
            if last_ts:
                dt_last = datetime.datetime.fromtimestamp(last_ts, timezone.utc)
                age = now_utc - dt_last
                if age.days < freshness_threshold_days:
                    should_refresh = False
            
            if should_refresh:
                reports_to_process.append(report)
    
    mdc.write_line(f"Found {len(reports_to_process)} reports to process/refresh.")

    if reports_to_process:
        await _run_async_playwright(reports_to_process)
