import sys
import os
import asyncio
import glob
import multiprocessing as mp
import warnings
import re
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Optional, List

import pandas as pd
import numpy as np

# Local imports
# Adjust path to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from scripts.common import core as mdc
from scripts.common import config as cfg
from scripts.common import playwright_lib as pl

warnings.filterwarnings('ignore')

# ----------------------------
# Helper Functions
# ----------------------------

def _load_price_history(symbol: str) -> pd.DataFrame:
    """
    Load price history for a symbol.
    TODO: Verify the correct parquet path for price data in this project structure.
    """
    # Attempt to load from standard market_data location
    # This is a placeholder adaptation of legacy 'alb.load_df_combined'
    try:
        # Assuming market_data stores as parquet in a standard location
        # If not found, returns empty DF to gracefully handle missing data
        remote_path = f"market_data/{symbol}.parquet" 
        # Using load_parquet from common core to fetch from Azure or local cache
        df = mdc.load_parquet(remote_path)
        if df is not None:
             # Ensure columns match what consumer expects
             if 'Symbol' not in df.columns:
                 df['Symbol'] = symbol
             return df
    except Exception as e:
        mdc.write_line(f"Error loading price history for {symbol}: {e}")
    
    # Return empty DataFrame with expected columns if load fails
    return pd.DataFrame(columns=['Date', 'Open', 'High', 'Low', 'Close', 'Symbol'])

def process_earnings_dates(row, all_dates):
    """
    Processes a single row to find the previous and next earnings dates.
    """
    symbol = row['Symbol']
    date = row['Date_parsed']

    # mdc.write_line(f"Processing earnings dates for {symbol} {row['EarningsDate']}")

    previous_dates = [d for d in all_dates if d <= date]
    next_dates = [d for d in all_dates if d > date]

    return {
        "Index": row.name,
        "PreviousEarningsDate": previous_dates[-1] if previous_dates else pd.NaT,
        "NextEarningsDate": next_dates[0] if next_dates else pd.NaT
    }

def add_proximity_to_earnings(df_earnings: pd.DataFrame) -> pd.DataFrame:
    """
    Adds next_earnings_date and days_until_next_earnings columns to the
    price_analysis DataFrame, per Symbol and sorted by Date.
    """   
    if df_earnings.empty:
        return pd.DataFrame()

    symbol = df_earnings['Symbol'].iloc[0]
    
    # Load required dataframes    
    mdc.write_line(f"[{symbol}] Loading required dataframes for proximity calc...")
    df_earnings_data = df_earnings
    df_price_data = _load_price_history(symbol)[['Date', 'Open', 'High', 'Low', 'Close', 'Symbol']]
    
    if df_price_data.empty:
        mdc.write_line(f"[{symbol}] No price data found. Skipping proximity.")
        return df_earnings_data # Or return empty? returning original for safety.

    mdc.write_line(f"[{symbol}] Calculating Proximity to Earnings")    
    
    # Setting parameters
    date_col = 'Date'
    col_next_date = 'Next Earnings Date'
    col_last_performance = 'Last Earnings Performance'
    col_prev_date = 'Previous Earnings Date'
    col_days_until = 'Days Until Next Earnings'
    
    # Sort by date                
    df_earnings_data = df_earnings_data.sort_values(date_col, kind="mergesort")
    
    # earnings calendar (sorted, unique)
    all_dates_np = (
        pd.to_datetime(df_earnings_data['Date'])
        .dropna().sort_values().unique()
        .astype('datetime64[ns]')
    )

    # dates to annotate (price dates)
    dates_np = pd.to_datetime(df_price_data[date_col]).to_numpy(dtype='datetime64[ns]')
    surprise_np  = df_earnings_data['Surprise'].to_numpy(dtype='float64')
    
    # Logic to find earnings date positions relative to price dates
    next_pos = np.searchsorted(all_dates_np, dates_np, side='left')
    prev_pos = next_pos - 1

    prev_ = np.full(dates_np.shape, np.datetime64('NaT', 'ns'), dtype='datetime64[ns]')
    mask_prev = prev_pos >= 0
    prev_[mask_prev] = all_dates_np[prev_pos[mask_prev]]

    prev_surprise = np.full(dates_np.shape, np.nan, dtype='float64')
    
    # Safety check for bounds
    if len(surprise_np) == len(all_dates_np):
         prev_surprise[mask_prev] = surprise_np[prev_pos[mask_prev]]

    next_ = np.full(dates_np.shape, np.datetime64('NaT', 'ns'), dtype='datetime64[ns]')
    mask_next = next_pos < len(all_dates_np)
    next_[mask_next] = all_dates_np[next_pos[mask_next]]

    df_price_data[col_next_date] = next_
    df_price_data[col_prev_date] = prev_
    df_price_data[col_last_performance] = prev_surprise
    
    # Use pandas to_datetime
    df_price_data[date_col]      = pd.to_datetime(df_price_data[date_col], errors="coerce")
    df_price_data[col_next_date] = pd.to_datetime(df_price_data[col_next_date], errors="coerce")
    df_price_data[col_days_until] = (df_price_data[col_next_date] - df_price_data[date_col]).dt.days

    # make dtype predictable (NaT → NaN -> float)
    df_price_data[col_days_until] = df_price_data[col_days_until].astype("float64")

    # NOTE: Original code returned 'df_price_data' (DataFrame with daily rows) 
    # but the function name 'add_proximity_to_earnings' implies modifying earnings DF or returning enriched data.
    # The consuming helper `_load_one_earnings` calls this and immediately returns the result.
    # So returning `df_price_data` (daily series with earnings metadata attached) seems to be the intended behavior.
    return df_price_data

def _load_one_earnings(path: str):
    """Return a DataFrame with Symbol added, or None on failure/missing."""
    if not os.path.exists(path):
        return None
    try:
        mdc.write_line(f"Loading earnings file {path}")
        df_earnings_data = pd.read_csv(path)
        # Extract symbol from filename: df_earnings_AAPL.csv -> AAPL
        symbol = os.path.splitext(os.path.basename(path))[0].split('_')[-1]
        
        # Load price data/proximity
        # Original logic: load earnings -> load price -> merge -> return merged
        # So we invoke add_proximity_to_earnings which does the merger
        if len(df_earnings_data) == 0:
            return None # Skip empty earnings files?
        
        df_earnings_data['Symbol'] = symbol
        df_combined = add_proximity_to_earnings(df_earnings_data)
        return df_combined
    except Exception as e:
        mdc.write_line(f"[earnings] Failed {path}: {e}")
        return None

def load_earnings_parallel(earnings_files, return_concat=False):
    # Use safe CPU count
    workers = max(1, mp.cpu_count() - 1)
    with ProcessPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(_load_one_earnings, earnings_files))
    
    # Filter valid results
    frames = [df for df in results if isinstance(df, pd.DataFrame) and not df.empty]
    
    if return_concat:
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return frames               

# ----------------------------
# Main Async Logic
# ----------------------------

async def main_async(df_symbols: pd.DataFrame):
    # Initialize playwright objects
    playwright, browser, context, page = await pl.get_playwright_browser(headless=False, use_async=True)
    
    # Paths (Configured)
    # Using local paths for now, relative to DATA_DIR defined in config
    cookies_path = cfg.USER_DATA_DIR / "pw_cookies_yahoo.json" 
    earnings_dir = cfg.DATA_DIR / "earnings"
    earnings_dir.mkdir(parents=True, exist_ok=True)
    
    if cookies_path.exists():
         await pl.pw_load_cookies_async(context, str(cookies_path))
    
    await page.reload()
    
    # Login if needed (Yahoo logic - check if login required)
    # await pl.pw_login_to_yahoo_async(page, context)
    # await pl.pw_save_cookies_async(context, str(cookies_path))
    
    # Filter out symbols containing dots (often different class shares or warrants)
    symbols = [
            row['Symbol'] 
            for _, row in df_symbols.iterrows() 
            if '.' not in str(row['Symbol'])
    ]
    
    # Debug override
    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        
    # Setup dates to scan for earnings
    today = pd.to_datetime(datetime.now().date())
    one_year_ago = today - pd.DateOffset(years=1)
    
    semaphore = asyncio.Semaphore(3) # Limit concurrency to avoid rate limits/detection
    
    async def fetch(symbol):
        async with semaphore:
            dates_without_earnings = pd.DataFrame()
            earnings_file_path = earnings_dir / f"df_earnings_{symbol}.csv"

            # Load and check existing
            df_symbol_earnings = pd.DataFrame()
            if earnings_file_path.exists():
                try:
                    df_symbol_earnings = pd.read_csv(earnings_file_path, encoding="utf-8")
                except Exception:
                    pass
                
            if len(df_symbol_earnings) > 0:
                if 'Date' in df_symbol_earnings.columns:
                    df_symbol_earnings['Date'] = pd.to_datetime(df_symbol_earnings['Date'], errors='coerce')
                    # Check if we have recent earnings or missing data that needs refetching
                    # Logic: If date is passed and no reported EPS/Surprise, and date is recent enough.
                    dates_without_earnings = df_symbol_earnings[
                        (df_symbol_earnings['Date'] < today) &          
                        (pd.isna(df_symbol_earnings.get('Reported EPS', np.nan))) & 
                        (pd.isna(df_symbol_earnings.get('Surprise', np.nan))) &     
                        (df_symbol_earnings['Date'] >= one_year_ago)    
                    ]
                
                # Get last-modified time
                if earnings_file_path.exists():
                    mtime = datetime.fromtimestamp(os.path.getmtime(earnings_file_path))
                    # If file updated recently (24h) and we have no 'missing' confirmed past earnings, likely up to date.
                    if len(dates_without_earnings) == 0 and (datetime.now() - mtime < timedelta(hours=24)):
                        mdc.write_line(f"Skipping {symbol}: upcoming earnings already recorded / recently checked")
                        return
        
            page = await context.new_page()
            try:
                # Retrieve earnings data
                # Using playwright_lib helper
                df_new = await pl.get_yahoo_earnings_data(page, symbol, timeout=30000)
                
                if df_new is not None and not df_new.empty:
                    # Cleanup columns
                    df_new = df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in col], errors='ignore')
                    
                    df_new.to_csv(earnings_file_path, index=False)
                    mdc.write_line(f"Saved earnings for {symbol}")
                else:
                    mdc.write_line(f"No earnings data found for {symbol}")
            except Exception as e:
                mdc.write_line(f"Error retrieving earnings data for {symbol}: {str(e)}")
            finally:                
                await page.close()
            
    # kick off all fetches
    mdc.write_line(f"Starting fetch for {len(symbols)} symbols...")
    tasks  = [fetch(sym) for sym in symbols]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregation
    mdc.write_line("Aggregating earnings data...")
    earnings_files = glob.glob(str(earnings_dir / "df_earnings_*.csv"))

    if earnings_files:
        df_all_earnings = load_earnings_parallel(earnings_files, return_concat=True)
        
        output_path = cfg.DATA_DIR / "all_earnings_data.csv"
        df_all_earnings.to_csv(output_path, index=False)
        mdc.write_line(f"df_all_earnings written to {output_path}.")
    else:
        mdc.write_line("No earnings files found to aggregate.")
           
    if browser:
        await browser.close()

# ----------------------------
# Entry Point
# ----------------------------

if __name__ == "__main__":
    # Ensure event loop policy for Windows if needed (sometimes helps with asyncio subprocesses)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load symbols
    mdc.write_line("Fetching symbols...")
    df_symbols = mdc.get_symbols()
    
    # Filter blacklisted (Placeholder for blacklist logic)
    # blacklist_file = cfg.DATA_DIR / "common" / "blacklist_earnings.csv"
    # if blacklist_file.exists():
    #      blacklist_df = pd.read_csv(blacklist_file)
    #      blacklist = set(blacklist_df['Ticker'].unique())
    #      df_symbols = df_symbols[~df_symbols['Symbol'].isin(blacklist)]

    try:
        asyncio.run(main_async(df_symbols))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Main loop error: {e}")
