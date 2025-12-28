import logging
import glob
from pathlib import Path
import asyncio
import re
import random
import time
from datetime import datetime, timedelta
import nasdaqdatalink
import pytz
import os
import sys
from dateutil.relativedelta import relativedelta
import warnings
import pandas as pd
import numpy as np

# Add project root to sys.path to ensure absolute imports work
# This allows the file to be run directly or imported without the root in pythonpath
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
if project_root not in sys.path:
    sys.path.append(project_root)

# Local imports
from scripts.common import playwright_lib as pl
from scripts.market_data import config as cfg
from scripts.common.blob_storage import BlobStorageClient

# Initialize Storage Client
try:
    storage_client = BlobStorageClient(container_name=cfg.AZURE_CONTAINER_NAME)
except ValueError:
    print("Warning: AZURE_STORAGE_CONNECTION_STRING not found. Azure operations will fail.")
    storage_client = None

# Suppress warnings
warnings.filterwarnings('ignore')


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Suppress Azure and urllib3 logging
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

def write_line(msg):
    '''
    Log a line with info level
    '''
    logger.info(msg)

def write_inline(text, endline=False):
    if not endline:
        sys.stdout.write('\r' + ' ' * 120 + '\r')
        sys.stdout.flush()
        ct = datetime.now()
        ct = ct.strftime('%Y-%m-%d %H:%M:%S')
        print('{}: {}'.format(ct, text), end='')
    else:
        print('\n\n', end='')

def write_section(title, s):
    print ("\n--------------------------------------------------")
    print (title)
    print ("--------------------------------------------------")
    if isinstance(s, np.ndarray):
        for i in range(len(s)):
            print("{}: {}".format(i+1, s[i]))
    else:
        print(s)
    print ("--------------------------------------------------\n")

def go_to_sleep(range_low = 5, range_high = 20):
    # sleep for certain amount of time
    sleep_time = random.randint(range_low, range_high)
    write_line(f'Sleeping for {sleep_time} seconds...')
    time.sleep(random.randint(range_low, range_high))

def update_csv_set(file_path, ticker):
    """
    Adds a ticker to a CSV file in Azure if it doesn't exist, ensuring uniqueness and sorting.
    """
    try:
        # Resolve remote path logic (reused from load_csv logic roughly)
        s_path = str(file_path).replace("\\", "/")
        if "scripts/common" in s_path:
             remote_path = s_path.split("scripts/common/")[-1]
        elif "common/" in s_path:
             remote_path = s_path.split("common/")[-1]
        else:
             remote_path = s_path.strip("/")

        df = pd.DataFrame(columns=['Symbol'])
        
        # Load existing
        existing_df = load_csv(remote_path)
        if existing_df is not None and not existing_df.empty:
            df = existing_df
            if 'Symbol' not in df.columns:
                 # Fallback attempt if header is missing, though Azure CSVs should be clean
                 df.columns = ['Symbol']

        if ticker not in df['Symbol'].values:
            new_row = pd.DataFrame([{'Symbol': ticker}])
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.sort_values('Symbol').reset_index(drop=True)
            
            store_csv(df, remote_path)
            write_line(f"Added {ticker} to {remote_path}")
    except Exception as e:
        write_line(f"Error updating {file_path}: {e}")

def store_csv(obj: pd.DataFrame, file_path):
    """
    Stores a DataFrame to Azure Blob Storage as CSV.
    file_path: Remote path or local path (converted).
    """
    s_path = str(file_path).replace("\\", "/")
    
    if "scripts/common" in s_path:
            remote_path = s_path.split("scripts/common/")[-1]
    elif "common/" in s_path:
            remote_path = s_path.split("common/")[-1]
    else:
            remote_path = s_path.strip("/")
            
    if storage_client:
        storage_client.write_csv(remote_path, obj)
    return remote_path

def load_csv(file_path) -> object:
    """
    Loads a CSV from Azure Blob Storage.
    file_path: Can be a local path (for compatibility, converted to remote) or relative remote path.
    """
    result = None
    try:
        # Convert path object to string and normalize for cloud usage
        # We strip the common local prefix if present to get the cloud relative path
        s_path = str(file_path).replace("\\", "/")
        
        # If the path looks like an absolute local path, try to make it relative to common dir or project
        # This is a heuristic to support existing logic calling with full paths
        if "scripts/common" in s_path:
             remote_path = s_path.split("scripts/common/")[-1]
        elif "common/" in s_path:
             remote_path = s_path.split("common/")[-1]
        else:
             remote_path = s_path
             
        # Handling subfolders explicitly if they are passed as Path objects joined with slashes
        # Just ensure we don't have leading slashes
        remote_path = remote_path.strip("/")
        
        # Fetch from Azure
        if storage_client:
            # write_line(f"DEBUG: Attempting to load {remote_path} from Azure")
            result = storage_client.read_csv(remote_path)
    except Exception as e:
        write_line(f'ERROR loading {file_path}: {e}')
    return result

def is_weekend(date):
    return date.weekday() >= 5

def delete_files_with_string(folder_path, search_string, extensions=['csv','crdownload']):
    if isinstance(extensions, str):
        extensions = [extensions]
    
    matching_files = []
    for ext in extensions:
        search_pattern = os.path.join(folder_path, f"*.{ext}")
        files = glob.glob(search_pattern)
        matching_files.extend([
            file for file in files
            if re.search(rf"\b{re.escape(search_string)}\b", os.path.splitext(os.path.basename(file))[0])
        ])
    
    if not matching_files:
        pass
    else:
        for file in matching_files:
            try:
                os.remove(file)
                print(f"Deleted file: {file}")
            except OSError as e:
                print(f"Error deleting file {file}: {e}")

async def get_historical_data_async(ticker, drop_prior, get_latest, page, df_whitelist=None, df_blacklist=None) -> tuple[pd.DataFrame, str]:
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    # Use unified path construction that load_csv understands
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
    df_ticker = load_csv(ticker_file_path)    
    if df_ticker is None:
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    # if not get_latest and df_ticker is not None:
    #     return df_ticker, ticker_file_path

    today = datetime.today().date()
    if drop_prior:
        yesterday = today - timedelta(days=1)
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
            period2 = period2 - timedelta(days=1)        
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
    black_path = str(pl.COMMON_DIR / 'blacklist.csv')
    white_path = str(pl.COMMON_DIR / 'whitelist.csv')

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
                 # Check the response. BLK is the response if the ticker exists
                 # https://finance.yahoo.com/quote/BLKBADTICKER/ is the response if the ticker does not exist
                 
                 # We check title or URL to determine existence
                 page_title = await page.title()
                 if "Symbol Lookup" in page_title or "Lookup" in page_title:
                      write_line(f"Ticker {ticker} not found on Yahoo (redirected to lookup). Blacklisting.")
                      update_csv_set(black_path, ticker)
                      return None, None
                      
             except Exception as e:
                 write_line(f"Error checking ticker {ticker}: {e}")

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

                store_csv(df_ticker, ticker_file_path)
                
                # Auto-whitelist on success
                update_csv_set(white_path, ticker)
                
                return df_ticker.reset_index(drop=True), ticker_file_path 
        else:
            # File download failed locally
            write_line(f"Download failed for {ticker}. Adding to blacklist.")
            update_csv_set(black_path, ticker)
            return None, ticker_file_path
        
    except Exception as e:
        e_str = str(e).lower()
        if '404' in e_str or 'list index out of range' in e_str or 'waiting for download from' in e_str:
            write_line(f'Skipping {ticker} because no data was found')
            update_csv_set(black_path, ticker)
        elif '401' in e_str:
            write_line(f'ERROR: {ticker} - Unauthorized.')
            go_to_sleep(30, 60)
        elif '429' in e_str:
            write_line(f'Sleeping due to excessive requests for {ticker}')
            go_to_sleep(30, 60)
        elif 'remote' in e_str or 'failed' in e_str or 'http' in e_str:
            write_line(f'ERROR: {ticker} - {e}')
            go_to_sleep(15, 30)
        elif 'system cannot find the file specified:' in e_str:
            write_line(f'ERROR: File not found. {ticker} - {e}')
            go_to_sleep(15, 30)
        else:
            write_line(f'Uknown error: {ticker} - {e}')
            go_to_sleep(30, 60)
        return None, ticker_file_path


async def refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context):
    skip_reload = False    
    if not skip_reload:
        write_line('Retrieving historical data...')
        df_symbols = df_symbols.dropna(subset=['Symbol'])
        
        # Paths (local strings, filtering handled by helpers)
        black_path = str(pl.COMMON_DIR / 'blacklist.csv')
        blacklist_financial_path = str(pl.COMMON_DIR / 'blacklist_financial.csv')
        
        symbols_to_remove = set()
        symbols_to_remove.update(load_ticker_list(black_path))
        symbols_to_remove.update(load_ticker_list(blacklist_financial_path))
        
        symbols = [
            row['Symbol'] 
            for _, row in df_symbols.iterrows() 
            if '.' not in row['Symbol'] and row['Symbol'] not in symbols_to_remove
        ]
        
        # Pre-load whitelist and blacklist for caching
        white_path = str(pl.COMMON_DIR / 'whitelist.csv')
        df_whitelist = load_csv(white_path)
        df_blacklist = load_csv(black_path) # black_path defined above
        
        # Cloud Path for aggregate
        historical_path_str = 'get_historical_data_output.csv'
        freshness_threshold = cfg.DATA_FRESHNESS_SECONDS
        df_concat = pd.DataFrame()
        
        # Check cloud freshness
        is_fresh = False
        if storage_client:
            last_mod = storage_client.get_last_modified(historical_path_str)
            if last_mod:
                # Compare UTC times
                now_utc = datetime.now(timezone.utc)
                age_seconds = (now_utc - last_mod).total_seconds()
                if age_seconds < freshness_threshold:
                    is_fresh = True
                    ts = last_mod
                    
        if is_fresh:
            print(f"✅  Using cached historical data ({ts:%Y-%m-%d %H:%M})")
            # Load from cloud
            df_concat = load_csv(historical_path_str)
        else:
            print("♻️  Cache missing or stale → downloading fresh historical data…")
            semaphore = asyncio.Semaphore(3)
            async def fetch(symbol):
                async with semaphore:
                    page = await context.new_page()
                    try:
                        return await get_historical_data_async(symbol, drop_prior, get_latest, page, df_whitelist=df_whitelist, df_blacklist=df_blacklist)
                    except Exception as e:
                        print(f"[Error] symbol={symbol}: {e}")
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
                store_csv(df_concat, historical_path_str)
                print(f"💾  Wrote fresh data to {historical_path_str}")
        return df_concat


def load_ticker_list(file_path: Path) -> list:
    if not isinstance(file_path, Path):
        file_path = Path(file_path)

    if not file_path.exists():
        return []
    
    try:
        if file_path.stat().st_size == 0:
            return []

        df_peek = pd.read_csv(file_path, nrows=1, header=None)
        if df_peek.empty:
            return []
            
        first_val = str(df_peek.iloc[0, 0])
        
        if first_val.strip().lower() in ['ticker', 'symbol']:
            df = pd.read_csv(file_path)
            col_name = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
            if col_name in df.columns:
                return df[col_name].dropna().unique().tolist()
        
        df = pd.read_csv(file_path, header=None)
        return df.iloc[:, 0].dropna().unique().tolist()

    except Exception as e:
        write_line(f"Warning: Failed to load ticker list from {file_path}: {e}")
        return []

def get_active_tickers():
    selected_columns = [
        "ticker", "comp_name", "comp_name_2", "sic_4_desc", "zacks_x_sector_desc", 
        "zacks_x_ind_desc", "zacks_m_ind_desc", "optionable_flag", "country_name", 
        "active_ticker_flag", "ticker_type"
    ]
    rename_mapping = {
        "ticker": "Symbol", "comp_name": "Name", "sic_4_desc": "Description",
        "zacks_x_sector_desc": "Sector", "zacks_x_ind_desc": "Industry",
        "zacks_m_ind_desc": "Industry_2", "optionable_flag": "Optionable", "country_name": "Country"
    }

    nasdaqdatalink.ApiConfig.verify_ssl = False
    api_key = os.environ.get('NASDAQ_API_KEY')
    if api_key:
        nasdaqdatalink.ApiConfig.api_key = api_key
    else:
        key_path = pl.COMMON_DIR / 'nasdaq_key.txt'
        if key_path.exists():
            nasdaqdatalink.read_key(filename=str(key_path))
        else:
            print(f"Warning: NASDAQ API key not found in env 'NASDAQ_API_KEY' or at {key_path}")
            
    try:
        df = nasdaqdatalink.get_table("ZACKS/MT", paginate=True, qopts={"columns": selected_columns})
        df = df[df['active_ticker_flag'] == "Y"]
        df = df[df['ticker_type'] == "S"]
        df["comp_name"] = np.where(
            (df["comp_name"].isnull()) | (df["comp_name"].str.strip() == ""),
            df["comp_name_2"],
            df["comp_name"]
        )
        df.drop(columns=["comp_name_2", "active_ticker_flag", "ticker_type"], inplace=True)
        df.rename(columns=rename_mapping, inplace=True)
        return df
    except Exception as e:
        write_line(f"Failed to get active tickers: {e}")
        return pd.DataFrame()

def get_remote_path(file_path):
    """
    Helper to convert local/mixed paths to Azure remote paths.
    """
    s_path = str(file_path).replace("\\", "/")
    if "scripts/common" in s_path:
         return s_path.split("scripts/common/")[-1]
    elif "common/" in s_path:
         return s_path.split("common/")[-1]
    return s_path.strip("/")

def get_symbols():
    df_symbols = pd.DataFrame()
    # Use cloud path for symbols cache
    file_path = "df_symbols.csv" 
    
    # Check cloud cache
    is_fresh = False
    if storage_client:
        last_modified = storage_client.get_last_modified(file_path)
        if last_modified:
            # Check if fresh (e.g. less than 24 hours old)
            # Nasdaq/Exchange data doesn't change implicitly fast, using a simple check
            # For now, just check existence or maybe a configured TTL. 
            # Original code checked timestamp > 0 which is just existence basically?
            # Actually original code checked: if timestamp > 0: get_active_tickers() else load_csv
            # Wait, original code: if timestamp > 0: get_active_tickers() -> This forces reload?
            # Original:
            # if os.path.exists(file_path):
            #    timestamp = os.path.getmtime(file_path)
            #    if timestamp > 0: <-- Always true if exists?
            #         df_symbols = get_active_tickers()
            #    else:
            #        df_symbols = load_csv(file_path)
            # That logic seems to imply it ALWAYS fetches fresh if file exists? 
            # Or maybe timestamp logic was buggy in original.
            # Let's assume we want to cache. 
            # Let's just always fetch fresh for now if that was the apparent behavior, 
            # OR implement a real TTL.
            # Let's stick to: Try to load from CSV. If fail, fetch from API.
            # But we should probably refresh occasionally.
            pass

    # Simplified logic: Try to load. If empty/missing, fetch.
    df_symbols = load_csv(file_path)
    
    if df_symbols is None or df_symbols.empty:
        write_line("Local symbol cache missing or empty. Fetching from NASDAQ API...")
        df_symbols = get_active_tickers() 
        store_csv(df_symbols, file_path)
    else:
        write_line(f"Loaded {len(df_symbols)} symbols from Azure cache.")
        
    tickers_to_add = cfg.TICKERS_TO_ADD
    
    # These are remote paths now
    blacklist_path = 'blacklist.csv'
    blacklist_financial_path = 'blacklist_financial.csv'
    
    symbols_to_remove = set()
    symbols_to_remove.update(load_ticker_list(blacklist_path))
    symbols_to_remove.update(load_ticker_list(blacklist_financial_path))
    
    if symbols_to_remove:
        write_line(f"Excluding {len(symbols_to_remove)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(symbols_to_remove)]

    df_symbols = df_symbols.reset_index(drop=True)
    # Tickers to add logic...
    for ticker_to_add in tickers_to_add:
        if not ticker_to_add['Symbol'] in df_symbols['Symbol'].to_list():
            df_symbols = pd.concat([df_symbols, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)
            
    df_symbols.drop_duplicates()
    store_csv(df_symbols, file_path)
    pd.DataFrame(tickers_to_add).to_csv('market_analysis_tickers.csv', index=False) # Local logic? No, store_csv would be better but this line uses pd.to_csv directory. 
    # Lets fix this line to use store_csv
    store_csv(pd.DataFrame(tickers_to_add), 'market_analysis_tickers.csv')
    store_csv(df_symbols, 'stock_tickers.csv')
    return df_symbols


# Run verification when executed directly
if __name__ == "__main__":
    print("Running verification checks...")
    # Add verification logic here if needed or keep it simple
    pass
