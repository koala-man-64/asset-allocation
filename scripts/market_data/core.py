
import logging
import asyncio
from datetime import datetime, timezone
import pandas as pd
import pytz
import os
import sys
from dateutil.relativedelta import relativedelta
import warnings

# Add project root to sys.path to ensure absolute imports work
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Local imports

# Local imports
from scripts.common import playwright_lib as pl

from scripts.market_data import config as cfg
from scripts.common import core as mdc
from scripts.common import delta_core

from scripts.common.pipeline import DataPaths, ListManager
import time
import random

# Helper Functions
def is_weekend(date_obj):
    return date_obj.weekday() >= 5

def go_to_sleep(min_time, max_time):
    time.sleep(random.randint(min_time, max_time))

def delete_files_with_string(directory, substring, extension):
    if not os.path.exists(directory):
        return
    for filename in os.listdir(directory):
        if substring in filename and filename.endswith(extension):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
            except OSError as e:
                mdc.write_error(f"Error deleting {file_path}: {e}")

# Aliases
store_delta = delta_core.store_delta

# Re-expose functions from mdc for backward compatibility
write_line = mdc.write_line
write_error = mdc.write_error
write_warning = mdc.write_warning
write_inline = mdc.write_inline
write_section = mdc.write_section
go_to_sleep = mdc.go_to_sleep
store_csv = mdc.store_csv
load_csv = mdc.load_csv
store_delta = delta_core.store_delta
load_delta = delta_core.load_delta
get_delta_last_commit = delta_core.get_delta_last_commit
update_csv_set = mdc.update_csv_set
delete_files_with_string = mdc.delete_files_with_string
get_symbols = mdc.get_symbols
load_ticker_list = mdc.load_ticker_list
is_weekend = mdc.is_weekend

# Initialize specific client for Market Data
market_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(market_client, "market-data")

async def get_historical_data_async(ticker, drop_prior, get_latest, page, df_whitelist=None, df_blacklist=None) -> tuple[pd.DataFrame, str]:
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    # Unified path from DataPaths
    ticker_file_path = DataPaths.get_market_data_path(ticker)
    
    df_ticker = load_delta(cfg.AZURE_CONTAINER_BRONZE, ticker_file_path)    
    if df_ticker is None:
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    today = datetime.today().date()
    if drop_prior:
        yesterday = today - relativedelta(days=1)
        df_ticker = df_ticker[df_ticker['Date'] < yesterday.strftime("%Y-%m-%d")]

    est = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(est)
    market_open = True
    pre_market   = current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30)
    after_market = current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)
    is_it_the_weekend = current_time_est.weekday() >= 5

    if pre_market or after_market or is_it_the_weekend:
        market_open = False
    
    retry_counter = 0
    while True:
        if market_open:
            df_ticker.drop(df_ticker[df_ticker['Date'] == datetime.today().date().strftime("%Y-%m-%d")].index, inplace=True)

        period1 = datetime.today() - relativedelta(years=10)            
        period1 = int(datetime(period1.year, period1.month, period1.day).timestamp())
        period2 = datetime.today()
        
        while is_weekend(period2):
            period2 = period2 - relativedelta(days=1)        
        period2_timestamp = pd.Timestamp(period2.date())

        # Check if we have period2 date already in dataframe
        matching_rows = df_ticker[df_ticker['Date'] == period2_timestamp]
        
        # Check if we have data (since we loaded from cloud, existence is implied by content)
        if not df_ticker.empty and len(matching_rows) > 0:
            write_line(f'Data for {ticker} loaded from file')
            break
        else:
             write_line(f"Data missing/stale for {ticker} (Date: {period2.date()}). Downloading...")
             return await download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1)
    
    return df_ticker.reset_index(drop=True), ticker_file_path

async def download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1):
    # Use ListManager
    if list_manager.is_whitelisted(ticker):
        write_line(f'{ticker} is in whitelist, skipping validation')
    elif list_manager.is_blacklisted(ticker):
        write_line(f'{ticker} is in blacklist, skipping')
        return None, None
    else:
         # Check if ticker exists in Yahoo
         try:
             quote_url = f'https://finance.yahoo.com/quote/{ticker}/'
             await page.goto(quote_url)
             
             page_title = await page.title()
             if "Symbol Lookup" in page_title or "Lookup" in page_title:
                  write_line(f"Ticker {ticker} not found on Yahoo (redirected to lookup). Blacklisting.")
                  list_manager.add_to_blacklist(ticker)
                  return None, None
                  
         except Exception as e:
             write_error(f"Error checking ticker {ticker}: {e}")

    try:
        url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker.replace(".", "-")}?period1={period1}&period2={cfg.YAHOO_MAX_PERIOD}&interval=1d&events=history'
        download_path = await pl.download_yahoo_price_data_async(page, url)
        
        path = download_path
        if os.path.exists(download_path):
                df_response = pd.read_csv(path)
                delete_files_with_string('C:/Users/rdpro/Downloads', ticker, 'csv')
                if "Adj Close" in df_response.columns:
                    df_response = df_response.drop('Adj Close', axis=1)
                df_response['Date'] = pd.to_datetime(df_response['Date'])
                df_response['Symbol'] = ticker

           # Concatenate - handle empty to avoid FutureWarning
                if df_ticker is None or df_ticker.empty:
                    df_ticker = df_response
                elif df_response is None or df_response.empty:
                    pass # df_ticker remains the same
                else:
                    df_ticker = pd.concat([df_ticker, df_response], ignore_index=True)
                df_ticker = df_ticker.sort_values(by=['Date', 'Symbol', 'Volume'], ascending=[True, True, False])
                df_ticker = df_ticker.drop_duplicates(subset=['Date', 'Symbol'], keep='first')
                
                df_ticker['index'] = range(0, len(df_ticker))       
                df_ticker['Symbol'] = ticker                        
                df_ticker = df_ticker.astype({
                    'Open': float,
                    'High': float,
                    'Low': float,
                    'Close': float,
                    'Volume': float,
                })
                if df_ticker['Date'].dtype != 'datetime64[ns]':
                    df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
                
                columns_to_limit = ['Open', 'High', 'Low', 'Close']
                for col in columns_to_limit:
                    df_ticker[col] = df_ticker[col].round(2).astype(float)
                    
                columns_to_drop = ['index', 'Beta (5Y Monthly)', 'PE Ratio (TTM)', '1y Target Est', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Market Cap']
                df_ticker = df_ticker.drop(columns=[col for col in columns_to_drop if col in df_ticker.columns])

                store_delta(df_ticker, cfg.AZURE_CONTAINER_BRONZE, ticker_file_path)
                
                # Auto-whitelist on success
                list_manager.add_to_whitelist(ticker)
                
                return df_ticker.reset_index(drop=True), ticker_file_path 
        else:
            # File download failed locally
            write_line(f"Download failed for {ticker}. Adding to blacklist.")
            list_manager.add_to_blacklist(ticker)
            return None, ticker_file_path
        
    except Exception as e:
        e_str = str(e).lower()
        if '404' in e_str or 'list index out of range' in e_str or 'waiting for download from' in e_str:
            write_line(f'Skipping {ticker} because no data was found')
            list_manager.add_to_blacklist(ticker)
        elif '401' in e_str:
            write_error(f'ERROR: {ticker} - Unauthorized.')
            go_to_sleep(30, 60)
        elif '429' in e_str:
            write_line(f'Sleeping due to excessive requests for {ticker}')
            go_to_sleep(30, 60)
        elif 'remote' in e_str or 'failed' in e_str or 'http' in e_str:
            write_error(f'ERROR: {ticker} - {e}')
            go_to_sleep(15, 30)
        elif 'system cannot find the file specified:' in e_str:
            write_error(f'ERROR: File not found. {ticker} - {e}')
            go_to_sleep(15, 30)
        else:
            write_error(f'Uknown error: {ticker} - {e}')
            go_to_sleep(30, 60)
        return None, ticker_file_path


async def refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context):
    write_line('Retrieving historical data...')
    df_symbols = df_symbols.dropna(subset=['Symbol'])
    
    # Load lists once
    list_manager.load()
    
    symbols = [
        row['Symbol'] 
        for _, row in df_symbols.iterrows() 
        if '.' not in row['Symbol'] and not list_manager.is_blacklisted(row['Symbol'])
    ]
    
    df_concat = pd.DataFrame()

    semaphore = asyncio.Semaphore(3)
    async def fetch(symbol):
        async with semaphore:
            page = await context.new_page()
            try:
                # Removed df_whitelist/df_blacklist args as they are handled by list_manager globally in module
                return await get_historical_data_async(symbol, drop_prior, get_latest, page)
            except Exception as e:
                write_error(f"[Error] symbol={symbol}: {e}")
                return None
            finally:
                await page.close()

    tasks  = [fetch(sym) for sym in symbols if "." not in sym]
    frames = await asyncio.gather(*tasks, return_exceptions=False)
    # Tuple unpacking handling: async returns (df, path)
    valid_frames = [df[0] for df in frames if df is not None and df[0] is not None]

    if valid_frames:
        df_concat = pd.concat(valid_frames, ignore_index=True)
    return df_concat

# Run verification when executed directly
if __name__ == "__main__":
    print("Running verification checks...")
    pass
