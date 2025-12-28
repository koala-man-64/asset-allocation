
import os
import sys
import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor # Kept if needed, but likely removing usage

import pandas as pd

# Add project root to sys.path to ensure absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Local imports
from scripts.common import core as mdc
from scripts.common import playwright_lib as pl

warnings.filterwarnings('ignore')

def combine_department_csvs(directories, output_folder, files_to_ignore):
    """
    Combines CSV files from specified directories into consolidated files.
    """
    os.makedirs(output_folder, exist_ok=True)

    for directory in directories:
        # print(f"Processing directory: {directory}")
        combined_data = []

        if not os.path.isdir(directory):
            print(f"Directory not found: {directory}, skipping.")
            continue

        for file_name in os.listdir(directory):
            if file_name.endswith(".csv") and file_name not in files_to_ignore:
                # print(f"Processing file: {file_name}")
                file_path = os.path.join(directory, file_name)

                try:
                    df = pd.read_csv(file_path)
                    # print(f"Successfully read file: {file_name}")
                    combined_data.append(df)
                except Exception as e:
                    print(f"Error reading file {file_name}: {e}")
            else:
                pass

        if combined_data:
            print(f"Combining data from {len(combined_data)} files in {directory}")
            combined_df = pd.concat(combined_data, ignore_index=True)

            folder_name = os.path.basename(os.path.normpath(directory))
            output_file_name = f"{folder_name}_Combined.csv"
            output_file_path = os.path.join(output_folder, output_file_name)

            combined_df.to_csv(output_file_path, index=False)
            print(f"Combined CSV saved to: {output_file_path}")
        else:
            print(f"No CSV files found or combined in directory: {directory}")


async def run_async_playwright(reports_to_refresh):
    """
    Runs the playwright scraper asynchronously, matching market_data pattern.
    """
    # Step 1: Initialize Async Browser
    # We use the list-like unpacking as get_playwright_browser returns a tuple, 
    # but in async mode it returns the specific objects if awaited? 
    # Checking lib: returns (playwright, browser, context, page)
    playwright, browser, context, page = await pl.get_playwright_browser(use_async=True)
    
    
    try:
        # Use generic path string, let mdc handle location (Azure vs Local)
        cookies_path = "pw_cookies.json"
        
        # Load cookies
        # Need to manually inject cookies into context since pl.pw_load_cookies* expects a file path 
        # but we want to load from Azure content.
        # Check if pl.pw_load_cookies_async supports dict or content? 
        # It takes a path. We might need to adjust PL lib or just download to temp.
        
        # Option A: Download from Azure to temp local path, then pass to PL.
        # This keeps PL lib clean of Azure logic (good separation).
        # But we want "utilize playwright the same way as market data" which implies using mdc.
        
        # Using mdc to get content:
        cookies_data = mdc.get_json_content(cookies_path)
        
        # If we have data, we can add it to context.
        if cookies_data:
            await context.add_cookies(cookies_data)
        
        # We need to reload the page to apply cookies effectively if we were already at a url, 
        # but here we are fresh. Login check handles navigation.
        await pl.pw_login_to_yahoo_async(page, context)
        
        # Save cookies back to Azure
        new_cookies = await context.cookies()
        mdc.save_json_content(new_cookies, cookies_path)
        
        # Parallel Processing with Semaphore
        # Mimic market_data/core.py: create separate pages for tasks to ensure isolation
        semaphore = asyncio.Semaphore(4) # Adjust based on system resources
        
        async def fetch_report(report):
            async with semaphore:
                report_page = await context.new_page()
                try:
                    # get_all_reports_list_async expects a LIST of reports
                    # We pass a single-item list to process just this report on this page
                    # It also expects params tuple: (playwright, browser, context, page)
                    params = (playwright, browser, context, report_page)
                    await pl.get_all_reports_list_async(params, [report])
                except Exception as e:
                    mdc.write_line(f"Error processing {report['ticker']}: {e}")
                finally:
                    await report_page.close()

        # Create tasks
        tasks = [fetch_report(report) for report in reports_to_refresh]
        
        if tasks:
            mdc.write_line(f"Starting parallel download of {len(tasks)} reports...")
            await asyncio.gather(*tasks)
        else:
            mdc.write_line("No reports to refresh.")
            
    finally:
        # Clean up
        await context.close()
        await browser.close()
        await playwright.stop()

    # Post-processing (CSV Combining)
    base_dir = r"G:\My Drive\Python\Common\Yahoo"
    directories = [
        os.path.join(base_dir, "Valuation"),
        os.path.join(base_dir, "Income Statement"),
        os.path.join(base_dir, "Balance Sheet"),
        os.path.join(base_dir, "Cash Flow")
    ]

    files_to_ignore = {"blacklist.csv", "whitelist.csv"}
    output_folder = r"G:\My Drive\Python\Common"

    # Run blocking IO in thread pool if needed, but for simple file ops direct call is okay 
    # or asyncio.to_thread for better async hygiene
    await asyncio.to_thread(combine_department_csvs, directories, output_folder, files_to_ignore)


async def main():
    mdc.write_line(f"Processing {mdc.get_current_timestamp_str()}...")

    # MIGRATION: Use Core
    df_symbols = mdc.get_symbols()
    
    # MIGRATION: Use Core for robust list loading
    blacklist_path = r"G:\My Drive\Python\Common\blacklist_financial.csv"
    
    # Use mdc.load_ticker_list which returns a list. 
    blacklist_list = mdc.load_ticker_list(blacklist_path)
    df_blacklist = pd.DataFrame(blacklist_list, columns=['Ticker'])
    
    # Filter blacklisted symbols
    if not df_blacklist.empty:
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(df_blacklist['Ticker'])]

    reports_to_refresh = []
    
    # Generate report list synchronously (fast local IO)
    # We can just check the first few or all. 
    # Originally threaded, but file checks are fast.
    mdc.write_line("Generating report list...")
    
    # We can use pl.get_all_reports_to_refresh(symbol)
    # Since checking 500+ files might take a moment, let's keep it clean but simple.
    # We'll just loop. If it's too slow, we can asyncify it, 
    # but pl.get_all_reports_to_refresh is sync.
    
    for symbol in df_symbols['Symbol'].tolist():
         # This touches the disk (os.path.exists)
         # We can't really async this effectively without wrappers.
         # For 500 files it's negligible.
         found_reports = pl.get_all_reports_to_refresh(symbol)
         if found_reports:
             reports_to_refresh.extend(found_reports)

    mdc.write_line(f"Found {len(reports_to_refresh)} reports to refresh.")

    await run_async_playwright(reports_to_refresh)


if __name__ == "__main__":
    asyncio.run(main())
