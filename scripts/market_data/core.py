
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
from scripts.common import playwright_lib as pl
from scripts.common import config as cfg
from scripts.common import core as mdc  # NEW: Import from common core

# Initialize Storage Client (Optional override or use common)
# We will use mdc.storage_client if we need it, or pass data via mdc functions.

# Suppress warnings
warnings.filterwarnings('ignore')

# Re-expose functions from mdc for backward compatibility (optional but clean)
# or update internal calls to use mdc.
# We will update internal calls.

write_line = mdc.write_line
write_error = mdc.write_error
write_warning = mdc.write_warning
write_inline = mdc.write_inline
write_section = mdc.write_section
go_to_sleep = mdc.go_to_sleep
store_csv = mdc.store_csv
load_csv = mdc.load_csv
store_parquet = mdc.store_parquet
load_parquet = mdc.load_parquet
update_csv_set = mdc.update_csv_set
delete_files_with_string = mdc.delete_files_with_string
get_symbols = mdc.get_symbols
load_ticker_list = mdc.load_ticker_list
is_weekend = mdc.is_weekend

# Initialize specific client for Market Data
market_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_NAME)


async def get_historical_data_async(ticker, drop_prior, get_latest, page, df_whitelist=None, df_blacklist=None) -> tuple[pd.DataFrame, str]:
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    # Use unified path construction that load_csv understands
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.parquet')
    df_ticker = load_parquet(ticker_file_path, client=market_client)    
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
             return await download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1, df_whitelist, df_blacklist)
    
    return df_ticker.reset_index(drop=True), ticker_file_path

async def download_and_process_yahoo_data(ticker, df_ticker, ticker_file_path, page, period1, df_whitelist=None, df_blacklist=None):
    black_path = 'market_data_blacklist.csv'
    white_path = 'market_data_whitelist.csv'

    # check if ticker exists in whitelist
    if df_whitelist is not None and not df_whitelist.empty:
        if ticker in df_whitelist['Symbol'].values:
            write_line(f'{ticker} is in whitelist, skipping validation')
            pass # proceed to download
        else:
             # Check if ticker exists in blacklist
             if df_blacklist is not None and not df_blacklist.empty:
                 if ticker in df_blacklist['Symbol'].values:
                     write_line(f'{ticker} is in blacklist, skipping')
                     return None, None
     
             # Check if ticker exists in Yahoo
             try:
                 quote_url = f'https://finance.yahoo.com/quote/{ticker}/'
                 await page.goto(quote_url)
                 
                 page_title = await page.title()
                 if "Symbol Lookup" in page_title or "Lookup" in page_title:
                      write_line(f"Ticker {ticker} not found on Yahoo (redirected to lookup). Blacklisting.")
                      update_csv_set(black_path, ticker, client=market_client)
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

                store_parquet(df_ticker, ticker_file_path, client=market_client)
                
                # Auto-whitelist on success
                update_csv_set(white_path, ticker, client=market_client)
                
                return df_ticker.reset_index(drop=True), ticker_file_path 
        else:
            # File download failed locally
            write_line(f"Download failed for {ticker}. Adding to blacklist.")
            update_csv_set(black_path, ticker, client=market_client)
            return None, ticker_file_path
        
    except Exception as e:
        e_str = str(e).lower()
        if '404' in e_str or 'list index out of range' in e_str or 'waiting for download from' in e_str:
            write_line(f'Skipping {ticker} because no data was found')
            update_csv_set(black_path, ticker, client=market_client)
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
    
    # Paths (standard container)
    black_path = 'market_data_blacklist.csv'
    white_path = 'market_data_whitelist.csv'
    
    symbols_to_remove = set()
    symbols_to_remove.update(mdc.load_ticker_list(black_path, client=market_client))
    
    symbols = [
        row['Symbol'] 
        for _, row in df_symbols.iterrows() 
        if '.' not in row['Symbol'] and row['Symbol'] not in symbols_to_remove
    ]
    
    # Pre-load whitelist and blacklist for caching
    df_whitelist = mdc.load_csv(white_path, client=market_client)
    df_blacklist = mdc.load_csv(black_path, client=market_client) 
    
    # Cloud Path for aggregate
    historical_path_str = 'get_historical_data_output.parquet'
    freshness_threshold = cfg.DATA_FRESHNESS_SECONDS
    df_concat = pd.DataFrame()
    
    # Check cloud freshness
    is_fresh = False
    
    # Check cloud freshness
    is_fresh = False
    
    # Use market_client directly for metadata check
    if market_client:
        last_mod = market_client.get_last_modified(historical_path_str)
        if last_mod:
            # Compare UTC times
            now_utc = datetime.now(timezone.utc)
            # Need strict timezone awareness
            if last_mod.tzinfo is None:
                 last_mod = last_mod.replace(tzinfo=timezone.utc)

            age_seconds = (now_utc - last_mod).total_seconds()
            if age_seconds < freshness_threshold:
                is_fresh = True
                ts = last_mod
                
    if is_fresh:
        print(f"  Using cached historical data ({ts:%Y-%m-%d %H:%M})")
        # Load from cloud
        df_concat = load_parquet(historical_path_str, client=market_client)
    else:
        print("  Cache missing or stale - downloading fresh historical data...")
        semaphore = asyncio.Semaphore(3)
        async def fetch(symbol):
            async with semaphore:
                page = await context.new_page()
                try:
                    return await get_historical_data_async(symbol, drop_prior, get_latest, page, df_whitelist=df_whitelist, df_blacklist=df_blacklist)
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
            # Save to cloud
            store_parquet(df_concat, historical_path_str, client=market_client)
            print(f"  Wrote fresh data to {historical_path_str}")
    return df_concat

# Run verification when executed directly
if __name__ == "__main__":
    print("Running verification checks...")
    pass
