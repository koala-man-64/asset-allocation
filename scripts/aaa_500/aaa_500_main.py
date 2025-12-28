import os
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.aaa_500 import aaa_500_lib as a500lb
from scripts.common import playwright_lib as pl

import time
import pandas as pd
import warnings
from datetime import datetime, timedelta
import ta as ta
import matplotlib.pyplot as plt
import itertools
import multiprocessing as mp
import concurrent.futures 
from multiprocessing import Pool, cpu_count
import dask.dataframe as dd
import pytz
import tempfile
import asyncio
import numpy as np
import copy 
import pandas.api.types as ptypes

warnings.filterwarnings('ignore')


# def main_sync(df_symbols: pd.DataFrame):
#     
#     # Initialize playwright objects
#     playwright, browser, context, page = pl.get_playwright_browser(headless=False)
#     pl.pw_load_cookies(context, "Data/pw_cookies.json")
#     page.reload()
#     pl.pw_login_to_yahoo(page, context)
#     pl.pw_save_cookies(context, "Data/pw_cookies.json")
#     
#     # For debugging 
#     debug_symbols = ['NXTC']#['A', 'AA', 'AAP', 'AAPL'] # pd.read_csv('Data/market_analysis_tickers.csv')['Symbol'].to_list()#[ 'DIA', 'SPY', 'QQQ', 'IWM', 'UST', '^VIX', 'IWC', 'VB', 'VO', 'VV']
#     if len(debug_symbols) > 0:
#         debug_symbols.append('SPY')
#         df_symbols = df_symbols[df_symbols['Symbol'].isin(debug_symbols)]
# 
#     est = pytz.timezone('US/Eastern')
#     current_time_est = datetime.now(est)
#     
#     pre_market = True # 
#     market_open = False
#     post_market = False
#     if not(
#         (current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)) or 
#         (current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30))
#         ):
#         market_open = False
#     drop_prior = 'n'
#     #input('Drop yesterday\'s price (y/[n])? ')
#     if drop_prior == 'n':
#         drop_prior = False
#     else:
#         drop_prior = True
#     get_latest = 'y'#input('Get latest([y]/n)? ')
#     if get_latest != 'n':
#         get_latest = True
#     else:
#         get_latest = False
#         
#     # Define temp file path
#     temp_dir = str(pl.COMMON_DIR) # tempfile.gettempdir()
#     temp_file = os.path.join(temp_dir, "df_combined.csv")
# 
#     # Check if file exists and is fresh (modified within 24 hours)
#     if False and os.path.exists(temp_file) and datetime.fromtimestamp(os.path.getmtime(temp_file)) > datetime.now() - timedelta(hours=24):
#         # --- 1. Inspect a quick sample ---------------------------------------------
#         sample = pd.read_csv(temp_file, nrows=1000)
# 
#         dtype_map = {}
#         for col in sample.columns:
#             if col == 'Date':
#                 continue                       # handled by parse_dates
#             elif col in ['Industry', 'Sector', 'Symbol']:
#                 dtype_map[col] = 'object'      # string
#             else:
#                 dtype_map[col] = 'float64'     # force all-float
# 
#         # --- 2. Load full file with consistent dtypes ------------------------------
#         df_combined = dd.read_csv(
#             temp_file,
#             parse_dates=['Date'],
#             dtype=dtype_map,
#             assume_missing=True                # still handy for int→float promotion
#         )
#         pl.write_line("Computing df_combined …")
#         df_combined = df_combined.compute()
#     else:
#         df_combined = await a500lb.refresh_stock_data2(df_symbols, 60, drop_prior, get_latest, page)
#         df_combined.to_csv(temp_file, index=False)  # Save to temp file
# 
#     # --- Final ordering ---------------------------------------------------------
#     # only sort if out of order
#     if not df_combined.index.is_monotonic_increasing or not df_combined['Date'].is_monotonic_increasing:
#         pl.write_line("Sorting combined DataFrame by Symbol and Date …")
#         df_combined.sort_values(by=['Symbol', 'Date'], inplace=True)
# 
#     pl.write_line("Feature-engineering complete.")
#     
#     # Clean dataframe and write to a file            
#     df_combined.replace([np.inf, -np.inf], 0, inplace=True)
#     df_combined.fillna(0, inplace=True)
#     df_combined.to_csv(pl.COMMON_DIR / 'df_combined.csv', index=False)
# 
#     print("\nParallel cross-sectional ranking by Date is complete. Multiple TotalRank columns added.")

async def main_async(df_symbols: pd.DataFrame):
    # Initialize playwright objects
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    await pl.pw_load_cookies_async(context, str(pl.COMMON_DIR / 'pw_cookies.json'))
    await page.reload()
    await pl.pw_login_to_yahoo_async(page, context)
    await pl.pw_save_cookies_async(context, str(pl.COMMON_DIR / 'pw_cookies.json'))
    
    # For debugging 
    debug_symbols = []#['A', 'AA', 'AAP', 'AAPL'] # pd.read_csv('Data/market_analysis_tickers.csv')['Symbol'].to_list()#[ 'DIA', 'SPY', 'QQQ', 'IWM', 'UST', '^VIX', 'IWC', 'VB', 'VO', 'VV']
    if len(debug_symbols) > 0:
        debug_symbols.append('SPY')
        df_symbols = df_symbols[df_symbols['Symbol'].isin(debug_symbols)]

    # Set timezone
    est = pytz.timezone('US/Eastern')
    current_time_est = datetime.now(est)
    
    pre_market = True # 
    market_open = False
    post_market = False
    if not(
        (current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)) or 
        (current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30))
        ):
        market_open = False
    drop_prior = False
    get_latest = True
        
    # Define temp file path
    temp_dir = str(pl.COMMON_DIR) # tempfile.gettempdir()
    temp_file = os.path.join(temp_dir, "df_combined.csv")
    
    # Check if file exists and is fresh (modified within 24 hours)
    if os.path.exists(temp_file) and datetime.fromtimestamp(os.path.getmtime(temp_file)) > datetime.now() - timedelta(hours=24):
        # --- 1. Inspect a quick sample ---------------------------------------------
        sample = pd.read_csv(temp_file, nrows=1000)

        dtype_map = {}
        for col in sample.columns:
            if col == 'Date':
                continue                       # handled by parse_dates
            elif col in ['Industry', 'Sector', 'Symbol']:
                dtype_map[col] = 'object'      # string
            else:
                dtype_map[col] = 'float64'     # force all-float

        # --- 2. Load full file with consistent dtypes ------------------------------
        df_combined = dd.read_csv(
            temp_file,
            parse_dates=['Date'],
            dtype=dtype_map,
            assume_missing=True                # still handy for int→float promotion
        )
        pl.write_line("Computing df_combined …")
        df_combined = df_combined.compute()
    else:
        df_combined = await a500lb.refresh_stock_data_async(df_symbols, 60, drop_prior, get_latest, browser, page, context)
        # df_combined.to_csv(temp_file, index=False)  # Save to temp file
    

if __name__ == "__main__":
    
    df_symbols = a500lb.get_symbols()
    asyncio.run(main_async(df_symbols))
    exit()
    
    # Get mode
    a500lb.write_line('Modes: refresh/1, run_test/2, evaluate_current/3, find_short_term/4, refresh+find_short_term/5')    
    mode = 'refresh'#'evaluate_current'
    
    if len(mode) > 0:
        
        # load symbols and df_combined (contains all stockd data)
        

        # async:
        # playwright, browser, context, page = await get_playwright_browser(headless=True, use_async=True)

        # refresh stock data
        if mode == 'run_test' or mode == '2':
            
            df_combined = a500lb.load_df_combined(df_symbols)
            
            # init backtest result
            backtest_result = a500lb.BacktestResult()
            backtest_result.StartDate = datetime(year=2022, month=1, day=1)
            backtest_result.EndDate = min(datetime.today(), df_combined['Date'].max())

            # init strategy
            strat = a500lb.Strategy()
            strat.LookbackBars = 45
            strat.RiskFreeTicker = 'SPY'
            strat.TopNPerGroup = 3
            strat.TopNSectors = 5
            strat.YearRangeThreshold = .25
            strat.VolumeThreshold = 100000
            strat.PositionsToMaintain = 3
            strat.ReallocateThreshold = 5
            strat.StopLossThreshold = .05
            strat.TakeProfitThreshold = -1 # -1 effectively disables takeprofit
            strat.StopLossThreshold = 1 # 1 effectively disables stoploss
            
            backtest_result.Strategy = strat
            backtest_result = a500lb.run_test(df_symbols, df_combined, backtest_result)
            
        # evaluate most recent data
        elif mode == 'evaluate_current' or mode == '3':
            
            # init strat
            strat = a500lb.Strategy()
            strat.LookbackBars = 30
            strat.RiskFreeTicker = 'SPY'
            strat.TopNPerGroup = 5
            strat.TopNSectors = 20
            strat.ReturnThreshold = .01
            strat.YearRangeThreshold = .95
            strat.VolumeThreshold = 50000
            strat.PositionsToMaintain = 4
            df_combined = a500lb.load_df_combined(df_symbols)
            a500lb.evaluate_current(df_symbols, df_combined, strat, True)
            
        elif mode == 'find_short_term' or mode == '4':
            # init strat
            strat = a500lb.Strategy()
            strat.YearRangeThreshold = .9
            strat.PriceThreshold = 1
            strat.VolumeThreshold = 100000
            df_combined = a500lb.load_df_combined(df_symbols)
            a500lb.find_short_term(df_symbols, df_combined, strat)
        
        elif mode == '5':
            est = pytz.timezone('US/Eastern')
            current_time_est = datetime.now(est)
            
            pre_market = True # 
            market_open = False
            post_market = False
            if not(
                (current_time_est.hour > 16 or (current_time_est.hour == 16 and current_time_est.minute > 0)) or 
                (current_time_est.hour < 9 or (current_time_est.hour == 9 and current_time_est.minute < 30))
                ):
                market_open = False
            drop_prior = input('Drop yesterday\'s price (y/[n])? ')
            if drop_prior != 'n':
                drop_prior = False
            else:
                drop_prior = True
            get_latest = input('Get latest([y]/n)? ')
            if get_latest != 'n':
                get_latest = True
            else:
                get_latest = False
            
            
            # init strat
            strat = a500lb.Strategy()
            strat.YearRangeThreshold = .5
            strat.VolumeThreshold = 50000
            strat.ReturnThreshold = -1
            while True:
                df_combined = a500lb.refresh_stock_data2(df_symbols, 30, drop_prior, get_latest)
                df_combined = a500lb.load_df_combined(df_symbols)
                a500lb.find_short_term(df_symbols, df_combined, strat)
                a500lb.go_to_sleep(30, 60)
        else:
            a500lb.write_line("Invalid mode entered")
    else:
        print("No mode provided")

    #input("Press enter to exit...")
    exit()