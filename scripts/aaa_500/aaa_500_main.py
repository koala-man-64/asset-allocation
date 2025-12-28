import os
import sys

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.aaa_500 import aaa_500_lib as a500lb
from scripts.common import playwright_lib as pl

import asyncio
import pytz
import dask.dataframe as dd
import warnings
from datetime import datetime, timedelta
import pandas as pd
import numpy as np # Used in main_async? Let's check. 
# It seems np is not used in main_async (only in commented out sync code), wait line 108 used np.inf, but that's commented out.
# wait, line 167 'assume_missing=True' handles float conversion.
# Let's keep numpy just in case or check deeper. 
# actually line 108/109 are commented out.


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
    # Unreachable code removed
