import os
import sys
import glob
from pathlib import Path
import asyncio
import re
import random
import time
import math
import uuid
import csv
import copy
from datetime import datetime, timedelta
import nasdaqdatalink
from multiprocessing import Pool, cpu_count
import concurrent.futures
import pytz
import fnmatch
import warnings
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator
import matplotlib.ticker as ticker
from bs4 import BeautifulSoup
import requests
import pickle
from dateutil.relativedelta import relativedelta
import ta
import itertools
import shutil
import pandas as pd
import numpy as np

# Local imports
from scripts.common import playwright_lib as pl
from scripts.market_analysis import ta_lib
from scripts.market_analysis import backtest_models
from scripts.market_analysis.backtest_models import Portfolio, Position, Strategy, BacktestResult, MetricsSnapshot, Trade
from scripts.market_analysis import config as cfg

# Suppress warnings
warnings.filterwarnings('ignore')

def write_line(msg):
    '''
    Print a line to the console w/ a timestamp
    Parameters:
        str:
    '''
    ct = datetime.now()
    ct = ct.strftime('%Y-%m-%d %H:%M:%S')
    print('{}: {}'.format(ct, msg))

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

def add_line_to_file(file_path, text_line):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'a') as file:
        file.write(text_line + '\n')

def store_csv(obj: pd.DataFrame, file_path):
    target = Path(file_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)   # create dirs
    obj.to_csv(target, index=False)
    return target

def load_csv(file_path) -> object:
    result = None
    try:
        if os.path.exists(file_path):
            result = pd.read_csv(file_path)
    except Exception as e:
        write_line(f'ERROR: {e}')
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

async def get_historical_data_async(ticker, drop_prior, get_latest, page) -> tuple[pd.DataFrame, str]:
    # Load df_ticker
    ticker = ticker.replace('.', '-')
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
    df_ticker = load_csv(ticker_file_path)    
    if isinstance(df_ticker, type(None)):
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    # if not get_latest and df_ticker is not None:
    #     return df_ticker, ticker_file_path

    if drop_prior:
        yesterday = datetime.today().date() - timedelta(days=1)
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
        if os.path.exists(ticker_file_path) and len(matching_rows) > 0:
            write_line(f'Data for {ticker} loaded from file')
            break
        else:
            try:
                black_path = str(pl.COMMON_DIR / 'blacklist.csv')
                period2 = int(datetime(period2.year, period2.month, period2.day).timestamp())
                url = f'https://query1.finance.yahoo.com/v7/finance/download/{ticker.replace(".", "-")}?period1={period1}&period2=99999999999&interval=1d&events=history'
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
                        break
                else:
                    add_line_to_file(black_path, ticker)
                    return
                
            except Exception as e:
                e_str = str(e).lower()
                retry_counter += 1
                if retry_counter >= 5 or 'ailed to download after 3 attempts' in str(e):
                    add_line_to_file(black_path, ticker)
                    break
                elif '404' in e_str or 'list index out of range' in e_str:
                    write_line(f'Skipping {ticker} because no data was found')
                    break
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
    
    return df_ticker.reset_index(drop=True), ticker_file_path 

def get_historical_data(ticker, drop_prior=False, get_latest=False, page=None) -> tuple[pd.DataFrame, str]:
    # Synchronous version wrapper.
    # Note: Logic is simplified here as we primarily use async in the runner.
    
    ticker = ticker.replace('.', '-')
    ticker_file_path = str(pl.COMMON_DIR / 'Yahoo' / 'Price Data' / f'{ticker}.csv')
    df_ticker = load_csv(ticker_file_path)    
    if isinstance(df_ticker, type(None)):
        df_ticker = pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol'])
    df_ticker = df_ticker[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Symbol']]
    
    if 'Date' in df_ticker.columns:
        df_ticker['Date'] = pd.to_datetime(df_ticker['Date'])
        
    if not get_latest and df_ticker is not None:
        return df_ticker, ticker_file_path

    try:
        csv_path = pl.get_yahoo_price_data(page, ticker)
        if csv_path:
            df = pd.read_csv(csv_path)
            return df, ticker
    except Exception as e:
        write_line(f"Error fetching data for {ticker}: {e}")
    return None, ticker

def get_historical_data_wrapper(args):
    return get_historical_data(*args)

async def refresh_stock_data_async(df_symbols, lookback_bars, drop_prior, get_latest, browser, page, context):
    skip_reload = False    
    if not skip_reload:
        write_line('Retrieving historical data...')
        df_symbols = df_symbols.dropna(subset=['Symbol'])
        symbols = [
            row['Symbol'] 
            for _, row in df_symbols.iterrows() 
            if '.' not in row['Symbol']
        ]

        historical_path     = pl.COMMON_DIR / 'get_historical_data_output.csv'
        freshness_threshold = 4 * 60 * 60
        df_concat = pd.DataFrame()

        if historical_path.exists() and (time.time() - historical_path.stat().st_mtime) < freshness_threshold:
            ts  = datetime.fromtimestamp(historical_path.stat().st_mtime)
            print(f"✅  Using cached historical data ({ts:%Y-%m-%d %H:%M})")
        else:
            print("♻️  Cache missing or stale → downloading fresh historical data…")
            semaphore = asyncio.Semaphore(1)
            async def fetch(symbol):
                async with semaphore:
                    page = await context.new_page()
                    try:
                        return await get_historical_data_async(symbol, drop_prior, get_latest, page)
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
                historical_path.parent.mkdir(parents=True, exist_ok=True)
                df_concat.to_csv(historical_path, index=False)
                print(f"💾  Wrote fresh data to {historical_path}")
        return df_concat

def monitor_stock_data(df_symbols, lookback_bars, drop_prior, get_latest):
    data_tuples = [(row['Symbol'], drop_prior, get_latest, True) for _, row in df_symbols.iterrows()]
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        results = list(executor.map(get_historical_data_wrapper,  data_tuples))
    results = [tup[0] for tup in results]
    concatenated_df = pd.concat(results)
    return concatenated_df

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

def get_symbols():
    df_symbols = pd.DataFrame()
    file_path = pl.COMMON_DIR / 'df_symbols.csv'
    
    if os.path.exists(file_path):
        timestamp = os.path.getmtime(file_path)
        if timestamp > 0:
             df_symbols = get_active_tickers()
        else:
            df_symbols = load_csv(file_path)
    else:
        df_symbols = get_active_tickers() 
        store_csv(df_symbols, file_path)
        
    tickers_to_add = cfg.TICKERS_TO_ADD
    
    blacklist_path = pl.COMMON_DIR / 'blacklist.csv'
    blacklist_financial_path = pl.COMMON_DIR / 'blacklist_financial.csv'
    
    symbols_to_remove = set()
    symbols_to_remove.update(load_ticker_list(blacklist_path))
    symbols_to_remove.update(load_ticker_list(blacklist_financial_path))
    
    if symbols_to_remove:
        write_line(f"Excluding {len(symbols_to_remove)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(symbols_to_remove)]

    df_symbols = df_symbols.reset_index(drop=True)
    for ticker_to_add in tickers_to_add:
        if not ticker_to_add['Symbol'] in df_symbols['Symbol'].to_list():
            df_symbols = pd.concat([df_symbols, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)
            
    df_symbols.drop_duplicates()
    store_csv(df_symbols, file_path)
    pd.DataFrame(tickers_to_add).to_csv(pl.COMMON_DIR / 'market_analysis_tickers.csv', index=False)
    df_symbols.to_csv(pl.COMMON_DIR / 'stock_tickers.csv', index=False)
    return df_symbols


def add_ta(df) -> pd.DataFrame:   
    try:
        df = df[['Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df = df[df['Volume'] != 0]
        # ta lib call
        ta.add_all_ta_features(df, 'Open', 'High', 'Low', 'Close', 'Volume')
        
        # remove columns
        columns_to_remove = cfg.COLUMNS_TO_REMOVE_TA
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
        
        df = df.set_index(['Symbol', 'Date'])
        
        # Logic to adjust indicators if needed (simplified from original which iterated list from CSV)
        # Assuming ta features are sufficient or custom logic here is minimal for now.
        # If needed, we can re-implement the specific NVI/TRIX adjustments.
        
        df = df.reset_index()
    except Exception as e:
        write_line(f'ERROR: Failed adding technical analysis to {df["Symbol"].iloc[-1] if not df.empty else "unknown"}: {e}')
        return None
    return df

def perform_technical_analysis(lookback_bars, ticker, df_risk_free): 
    write_line(f'Performing technical analysis on  {ticker}')
    df_historical, ticker_pickle_path = get_historical_data(ticker, False, False, None)
    if isinstance(df_historical, type(None)) or len(df_historical) == 0:
        write_line(f'Nothing found for ticker {ticker}')
        return None
    
    df_historical = df_historical.sort_values(['Symbol', 'Date'])
    df_historical = df_historical.drop_duplicates(subset='Date')

    if len(df_historical) > 50:
        rolling_avg = df_historical['Volume'].rolling(window=lookback_bars).mean()
        df_historical['Normalized_Volume'] = df_historical['Volume'] / rolling_avg
        
        try:
            df_historical['Diff%'] = df_historical['Close'] / df_historical['Close'].shift(1) - 1
            if df_risk_free is not None and ticker != 'SPY':
                df_risk_free['Diff%'] = df_risk_free['Close'] / df_risk_free['Close'].shift(1) - 1
                df_historical = pd.merge(df_historical, df_risk_free[['Date', 'Diff%']], on='Date', suffixes=('', '_RiskFree'))
            else:
                df_historical['Diff%_RiskFree'] = df_historical['Diff%']
                
            df_historical = df_historical.loc[:, ~df_historical.columns.duplicated()]
            
            if df_risk_free is not None:
                # Use ta_lib functions
                df_historical['Jensen_20'] = ta_lib.calculate_rolling_jensens_alpha(df_historical, df_risk_free, .035, 20)
                df_historical['Jensen_50'] = ta_lib.calculate_rolling_jensens_alpha(df_historical, df_risk_free, .035, 50)
            else:
                df_historical['Jensen_20'] = 0
                df_historical['Jensen_50'] = 0
            
            df_historical['MACD_Signal_Line'] = ta_lib.calculate_macd(df_historical)
            df_historical['Standardized_MACD'] = ta_lib.standardize_macd(df_historical)
            df_historical = ta_lib.calculate_bollinger_bands(df_historical, period=36)
            df_historical['OBV'] = ta_lib.calculate_obv(df_historical)
            df_historical['ADL'] = ta_lib.calculate_adl(df_historical)
            df_historical['RSI'] = ta_lib.calculate_rsi(df_historical)
            df_historical = ta_lib.calculate_stochastic_oscillator(df_historical, period=36)
            df_historical['CCI'] = ta_lib.calculate_cci(df_historical)
            
            # Additional calcs
            df_historical['Boll_Stoch_Diff'] = (df_historical['Bollinger_High'] - df_historical['Bollinger_Low']) / (df_historical['Stochastic_High'] - df_historical['Stochastic_Low'])
            
            # Save
            store_csv(df_historical, ticker_pickle_path)    
        except Exception as e:
            write_line(f'ERROR - Failed stock analysis for {ticker}: {e}')
    return df_historical

# Placeholder for less used functions to ensure module runs
def process_chunk(chunk):
    return chunk

def get_bail_trend(bar_count):
    # Implementation optional if not used in main runner, but kept for completeness
    pass
