import nasdaqdatalink
import pandas as pd
import numpy as np
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import warnings
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import List, Optional

# Local imports
# Adjust path if necessary to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from scripts.common import core as mdc
from scripts.common import config as cfg

warnings.filterwarnings('ignore')

# Constants for Cloud Storage (Relative paths to Azure Container root)
CSV_FOLDER = "price_targets"
BLACKLIST_FILE = "blacklist_price_targets.csv"
NASDAQ_KEY_FILE = "nasdaq_key.txt"
OUTPUT_FILE = "df_price_targets.parquet"
# DF_COMBINED_PATH = "df_combined.parquet" # [Mechanical cleanup] Unused constant
BATCH_SIZE = 50

def setup_nasdaq_key():
    """Attempts to load Nasdaq Data Link key from Environment or Cloud."""
    key = os.environ.get('NASDAQ_API_KEY')
    if key:
        nasdaqdatalink.ApiConfig.api_key = key
    else:
        # Fetch from cloud
        key_content = mdc.get_file_text(NASDAQ_KEY_FILE)
        if key_content:
            nasdaqdatalink.ApiConfig.api_key = key_content.strip()
        else:
            mdc.write_line("WARNING: Nasdaq API key not found in Env or Cloud.")

def transform_symbol_data(symbol: str, target_price_data: pd.DataFrame, existing_price_targets: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Transforms raw API data for a single symbol: sorts, resamples, carries forward, 
    merges with existing data, and saves to cloud.
    """
    column_names = [
        "ticker", "obs_date", "tp_mean_est", "tp_std_dev_est", 
        "tp_high_est", "tp_low_est", "tp_cnt_est", 
        "tp_cnt_est_rev_up", "tp_cnt_est_rev_down"
    ]
    price_target_cloud_path = f"{CSV_FOLDER}/{symbol}.parquet"

    try:
        # Ensure timestamp
        target_price_data['obs_date'] = pd.to_datetime(target_price_data['obs_date'])
        
        # Sort
        target_price_data = target_price_data.sort_values(by='obs_date')
        
        # Carry Forward Logic
        latest_obs_date = target_price_data['obs_date'].max()
        today = pd.to_datetime("today").normalize()

        # If last observation is old, we might want to carry it forward to today
        # Only do this if we actually have data
        if not target_price_data.empty and latest_obs_date < today:
            all_dates = pd.date_range(start=target_price_data['obs_date'].min(), end=today)
            df_all_dates = pd.DataFrame({'obs_date': all_dates})
            target_price_data = df_all_dates.merge(target_price_data, on='obs_date', how='left')
            target_price_data = target_price_data.ffill()
        
        # Ensure columns exist
        for col in column_names:
            if col not in target_price_data.columns:
                 target_price_data[col] = np.nan
        
        target_price_data = target_price_data[column_names]

        # Resample to Daily to fill gaps
        target_price_data.set_index('obs_date', inplace=True)
        # Handle duplicates if any (though API usually returns unique per date? safety check)
        target_price_data = target_price_data[~target_price_data.index.duplicated(keep='last')]
        
        full_date_range = pd.date_range(start=target_price_data.index.min(), end=target_price_data.index.max(), freq='D')
        target_price_data = target_price_data.reindex(full_date_range)
        target_price_data.ffill(inplace=True)
        target_price_data.reset_index(inplace=True)
        target_price_data = target_price_data.rename(columns={'index': 'obs_date'})
        
        # Restore ticker column if lost during reindex/ffill (it might become nan if first row was not start of range? No, ffill handles it)
        # But if reindex introduced new rows at START, they are NaN. 
        # Actually logic above: 'start=target_price_data.index.min()' ensures we start where data starts.
        # But 'ticker' column needs to be filled.
        target_price_data['ticker'] = symbol

        # Merge with existing
        updated_earnings = pd.concat([existing_price_targets, target_price_data], ignore_index=True)
        updated_earnings = updated_earnings.drop_duplicates(subset=['obs_date', 'ticker'], keep='last')
        updated_earnings = updated_earnings.sort_values(by=['obs_date', 'ticker']).reset_index(drop=True)

        # Save
        mdc.store_parquet(updated_earnings, price_target_cloud_path)
        # mdc.write_line(f"  Uploaded updated data for {symbol}")

        return updated_earnings

    except Exception as e:
        mdc.write_line(f"Error transforming data for {symbol}: {e}")
        return None

def process_symbols_batch(symbols: List[str]) -> List[pd.DataFrame]:
    """
    Processes a batch of symbols. checks freshness, and batches API calls for the rest.
    """
    results = []
    stale_symbols = []
    existing_data_map = {} # symbol -> existing_df
    
    column_names = [
        "ticker", "obs_date", "tp_mean_est", "tp_std_dev_est", 
        "tp_high_est", "tp_low_est", "tp_cnt_est", 
        "tp_cnt_est_rev_up", "tp_cnt_est_rev_down"
    ]

    # 1. Freshness Check
    for symbol in symbols:
        price_target_cloud_path = f"{CSV_FOLDER}/{symbol}.parquet"
        is_fresh = False
        
        if mdc.storage_client:
             last_mod = mdc.storage_client.get_last_modified(price_target_cloud_path)
             if last_mod:
                 now_utc = datetime.now(timezone.utc)
                 if last_mod.tzinfo is None:
                     last_mod = last_mod.replace(tzinfo=timezone.utc)
                 if now_utc - last_mod < timedelta(days=7):
                     is_fresh = True
        
        if is_fresh:
            loaded_df = mdc.load_parquet(price_target_cloud_path)
            if loaded_df is not None:
                if 'obs_date' in loaded_df.columns:
                    loaded_df['obs_date'] = pd.to_datetime(loaded_df['obs_date'])
                results.append(loaded_df)
        else:
            stale_symbols.append(symbol)
            # Init empty existing df for stale symbols, or try to load what WAS there?
            # Ideally we want to append new data to old data.
            # So let's try to load "stale" data to merge with it, rather than starting empty.
            existing_df = mdc.load_parquet(price_target_cloud_path)
            if existing_df is None or existing_df.empty:
                existing_df = pd.DataFrame(columns=column_names)
            elif 'obs_date' in existing_df.columns:
                 existing_df['obs_date'] = pd.to_datetime(existing_df['obs_date'])
            
            existing_data_map[symbol] = existing_df

    if not stale_symbols:
        return results

    # 2. Batch API Call
    min_date = date(2020, 1, 1) # Or derive from existing data? simpler to just fetch all > 2020 for now.
    
    mdc.write_line(f"Fetching batch of {len(stale_symbols)} symbols from API...")
    
    try:
        # Pass comma-separated string
        tickers_str = ",".join(stale_symbols)
        batch_df = nasdaqdatalink.get_table(
            'ZACKS/TP',
            ticker=tickers_str,
            obs_date={'gte': min_date.strftime('%Y-%m-%d')}
        )
    except Exception as e:
        mdc.write_line(f"API Batch Error: {e}")
        # If batch fails, we could fallback to single, but let's just fail this batch for now.
        return results

    # 3. Process each symbol in batch
    processed_count = 0
    
    # Identify which symbols were returned
    found_tickers = set()
    if not batch_df.empty:
        # Group by ticker
        # batch_df['ticker'] might be mixed case? API usually returns per request.
        # Ensure we match `stale_symbols`.
        
        # Iterate over unique tickers in response
        for symbol, group_df in batch_df.groupby('ticker'):
            symbol = str(symbol) # ensure string
            if symbol in existing_data_map:
                processed_df = transform_symbol_data(symbol, group_df.copy(), existing_data_map[symbol])
                if processed_df is not None:
                    results.append(processed_df)
                    processed_count += 1
                found_tickers.add(symbol)
    
    # 4. Handle Missing Symbols (Blacklist)
    for symbol in stale_symbols:
        if symbol not in found_tickers:
            # No data returned for this symbol
            existing_df = existing_data_map[symbol]
            if existing_df.empty:
                mdc.write_line(f"Blacklisting {symbol} (No data).")
                mdc.update_csv_set(BLACKLIST_FILE, symbol)
            else:
                # We have old data but no new data. Just use old.
                results.append(existing_df)

    if processed_count > 0:
        mdc.write_line(f"Batch processed {processed_count}/{len(stale_symbols)} stale symbols updated.")

    return results

def run_batch_processing():
    setup_nasdaq_key()
    
    # 1. Get Symbols
    df_symbols = mdc.get_symbols()
    
    # Cloud-aware blacklist filtering
    blacklist_list = mdc.load_ticker_list(BLACKLIST_FILE)
    if blacklist_list:
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(blacklist_list)]
        
    # Apply Debug Symbols Filter
    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Filtering for {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]

    symbols = list(df_symbols['Symbol'].unique())
    mdc.write_line(f"Found {len(symbols)} unique symbols.")

    # 2. Worker config
    
    # We will submit chunks of symbols to the executor.
    # Each valid 'task' is now a batch of BATCH_SIZE symbols.
    
    chunked_symbols = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    num_cores = os.cpu_count() or 1
    # Reduce workers slightly since each worker does more heavy lifting? 
    # Or keep same. API parallelism is still limited by network/rate limits?
    # Nasdaq rate limits might be triggered.
    num_workers = max(1, int(num_cores * 0.75))
    mdc.write_line(f"Using {num_workers} worker threads for {len(chunked_symbols)} batches (Batch Size: {BATCH_SIZE}).")

    updated_symbol_dfs = []

    # 3. Parallel Execution
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_batch = {executor.submit(process_symbols_batch, batch): batch for batch in chunked_symbols}
        
        for future in as_completed(future_to_batch):
            try:
                res_list = future.result()
                if res_list:
                    updated_symbol_dfs.extend(res_list)
            except Exception as e:
                mdc.write_line(f"Exception for batch: {e}")

    # 4. Save Final Aggregation
    if updated_symbol_dfs:
         updates_df = pd.concat(updated_symbol_dfs, ignore_index=True)
         updates_df.rename(columns={'ticker': 'Symbol', 'obs_date': 'Date'}, inplace=True)
         updates_df['Date'] = pd.to_datetime(updates_df['Date'])
         
         mdc.store_parquet(updates_df, OUTPUT_FILE)
         mdc.write_line(f"Saved aggregated price targets to Cloud: {OUTPUT_FILE}")
    else:
        mdc.write_line("No updates generated.")


def run_interactive_mode(df=None):
    """
    Interactive exploration mode.
    """
    setup_nasdaq_key()
    
    if df is None:
        mdc.write_line("Loading aggregated data from Cloud...")
        df = mdc.load_parquet(OUTPUT_FILE)
        if df is not None:
             df['Date'] = pd.to_datetime(df['Date'])
        else:
             print("No data available for interactive mode.")
             return

    symbols = df['Symbol'].unique().tolist()
    mdc.write_line("Here are 5 random symbols:")
    try:
        mdc.write_line(random.sample(symbols, min(len(symbols), 5)))
    except ValueError:
        pass

    while True:
        user_symbol = input("\nEnter symbol (or 'quit'): ").strip().upper()
        if user_symbol.lower() == 'quit':
            break
            
        if user_symbol not in symbols:
            print(f"Symbol '{user_symbol}' not found.")
            continue
            
        symbol_df = df[df['Symbol'] == user_symbol].sort_values(by='Date').reset_index(drop=True)
        
        if 'Close' not in symbol_df.columns:
             print("Close price not in dataset (this column requires price history).")
             
        # Fetch fresh TP diff check
        try:
            tp_data = nasdaqdatalink.get_table('ZACKS/TP', ticker=user_symbol)
            if not tp_data.empty:
                 print(tp_data.head())
            else:
                 print("No API data.")
        except Exception as e:
            print(f"Error: {e}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        run_interactive_mode()
    else:
        run_batch_processing()

if __name__ == "__main__":
    main()
