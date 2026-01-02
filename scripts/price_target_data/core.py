
import sys
import os
import warnings
from datetime import datetime, timezone, date
from pathlib import Path
from typing import List, Optional
import pandas as pd
import numpy as np
import nasdaqdatalink
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from scripts.common import core as mdc
from scripts.common import config as cfg
from scripts.common import delta_core

warnings.filterwarnings('ignore')

# Constants for Cloud Storage (Relative paths to Azure Container root)
DATA_FOLDER = "bronze/price_targets"
WHITELIST_FILE = "price_target_data_whitelist.csv"
BLACKLIST_FILE = "price_target_data_blacklist.csv"
BATCH_SIZE = 50

# Initialize Client
_pt_client = None

def get_client():
    """Lazy loader for the Azure Storage Client."""
    global _pt_client
    if _pt_client is None:
        _pt_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_TARGETS)
    return _pt_client

def setup_nasdaq_key():
    """Attempts to load Nasdaq Data Link key from Environment."""
    key = os.environ.get('NASDAQ_API_KEY')
    if key:
        nasdaqdatalink.ApiConfig.api_key = key
    else:
        raise ValueError("NASDAQ_API_KEY environment variable is required.")

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
    price_target_cloud_path = f"{DATA_FOLDER}/{symbol}"

    try:
        # Ensure timestamp
        target_price_data['obs_date'] = pd.to_datetime(target_price_data['obs_date'])
        
        # Sort
        target_price_data = target_price_data.sort_values(by='obs_date')
        
        # Carry Forward Logic
        latest_obs_date = target_price_data['obs_date'].max()
        today = pd.to_datetime("today").normalize()

        # If last observation is old, we might want to carry it forward to today
        if not target_price_data.empty and latest_obs_date < today:
            all_dates = pd.date_range(start=target_price_data['obs_date'].min(), end=today)
            df_all_dates = pd.DataFrame({'obs_date': all_dates})
            target_price_data = df_all_dates.merge(target_price_data, on='obs_date', how='left')
            target_price_data = target_price_data.ffill()
        
        # Ensure ticker column is set correctly
        target_price_data['ticker'] = symbol
        
        # Ensure columns exist
        for col in column_names:
            if col not in target_price_data.columns:
                 target_price_data[col] = np.nan
        
        target_price_data = target_price_data[column_names]

        # Resample to Daily to fill gaps
        target_price_data.set_index('obs_date', inplace=True)
        # Handle duplicates if any (safety check)
        target_price_data = target_price_data[~target_price_data.index.duplicated(keep='last')]
        
        full_date_range = pd.date_range(start=target_price_data.index.min(), end=target_price_data.index.max(), freq='D')
        target_price_data = target_price_data.reindex(full_date_range)
        target_price_data.ffill(inplace=True)
        target_price_data.reset_index(inplace=True)
        target_price_data = target_price_data.rename(columns={'index': 'obs_date'})
        
        target_price_data['ticker'] = symbol

        # Merge with existing
        updated_earnings = pd.concat([existing_price_targets, target_price_data], ignore_index=True)
        updated_earnings = updated_earnings.drop_duplicates(subset=['obs_date', 'ticker'], keep='last')
        updated_earnings = updated_earnings.sort_values(by=['obs_date', 'ticker']).reset_index(drop=True)

        # Save
        delta_core.store_delta(updated_earnings, cfg.AZURE_CONTAINER_TARGETS, price_target_cloud_path)

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
        price_target_cloud_path = f"{DATA_FOLDER}/{symbol}"
        is_fresh = False
        
        last_ts = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_TARGETS, price_target_cloud_path)
        if last_ts:
             now_ts = datetime.now(timezone.utc).timestamp()
             # Compare seconds from epoch
             if (now_ts - last_ts) < (7 * 24 * 3600): # 7 days in seconds
                 is_fresh = True
        
        if is_fresh:
            loaded_df = delta_core.load_delta(cfg.AZURE_CONTAINER_TARGETS, price_target_cloud_path)
            if loaded_df is not None:
                if 'obs_date' in loaded_df.columns:
                    loaded_df['obs_date'] = pd.to_datetime(loaded_df['obs_date'])
                results.append(symbol)
        else:
            stale_symbols.append(symbol)
            # Init empty existing df for stale symbols, or try to load what WAS there?
            existing_df = delta_core.load_delta(cfg.AZURE_CONTAINER_TARGETS, price_target_cloud_path)
            if existing_df is None or existing_df.empty:
                existing_df = pd.DataFrame(columns=column_names)
            elif 'obs_date' in existing_df.columns:
                 existing_df['obs_date'] = pd.to_datetime(existing_df['obs_date'])
            
            existing_data_map[symbol] = existing_df

    if not stale_symbols:
        return results

    # 2. Batch API Call
    min_date = date(2020, 1, 1) 
    
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
        # Iterate over unique tickers in response
        for symbol, group_df in batch_df.groupby('ticker'):
            symbol = str(symbol) # ensure string
            if symbol in existing_data_map:
                processed_df = transform_symbol_data(symbol, group_df.copy(), existing_data_map[symbol])
                if processed_df is not None:
                    results.append(symbol)
                    processed_count += 1
                found_tickers.add(symbol)
    
    # 4. Handle Missing Symbols (Blacklist)
    for symbol in stale_symbols:
        if symbol not in found_tickers:
            # No data returned for this symbol
            existing_df = existing_data_map[symbol]
            if existing_df.empty:
                mdc.write_line(f"Blacklisting {symbol} (No data).")
                mdc.update_csv_set(BLACKLIST_FILE, symbol, client=get_client())
            else:
                # We have old data but no new data. Just use old.
                results.append(symbol)

    if processed_count > 0:
        mdc.write_line(f"Batch processed {processed_count}/{len(stale_symbols)} stale symbols updated.")
    
    # Auto-whitelist successful symbols
    for symbol in found_tickers:
        mdc.update_csv_set(WHITELIST_FILE, symbol, client=get_client())

    return results

def run_batch_processing():
    setup_nasdaq_key()
    
    # 1. Get Symbols
    df_symbols = mdc.get_symbols()
    
    # Cloud-aware list filtering
    whitelist_list = mdc.load_ticker_list(WHITELIST_FILE, client=get_client())
    blacklist_list = mdc.load_ticker_list(BLACKLIST_FILE, client=get_client())
    
    if blacklist_list:
        mdc.write_line(f"Filtering out {len(blacklist_list)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(blacklist_list)]
        
    # Apply Debug Symbols Filter
    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Filtering for {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}")
        df_symbols = df_symbols[df_symbols['Symbol'].isin(cfg.DEBUG_SYMBOLS)]

    symbols = list(df_symbols['Symbol'].unique())
    mdc.write_line(f"Found {len(symbols)} unique symbols.")

    # 2. Worker config
    chunked_symbols = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    num_cores = os.cpu_count() or 1
    num_workers = max(1, int(num_cores * 0.75))
    mdc.write_line(f"Using {num_workers} worker threads for {len(chunked_symbols)} batches (Batch Size: {BATCH_SIZE}).")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_chunk = {executor.submit(process_symbols_batch, chunk): chunk for chunk in chunked_symbols}
        
        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                future.result() 
            except Exception as exc:
                mdc.write_line(f"Batch generation generated an exception: {exc}")
    
    mdc.write_line("Batch processing complete. All symbols updated individually.")


def run_interactive_mode(df=None):
    """
    Interactive exploration mode.
    """
    setup_nasdaq_key()
    
    while True:
        user_symbol = input("\nEnter symbol (or 'quit'): ").strip().upper()
        if user_symbol.lower() == 'quit':
            break
            
        # Load individual file
        file_path = f"{DATA_FOLDER}/{user_symbol}"
        symbol_df = delta_core.load_delta(cfg.AZURE_CONTAINER_TARGETS, file_path)
        
        if symbol_df is None:
             print(f"No local data found for {user_symbol}")
             # Check API?
        else:
            symbol_df['Date'] = pd.to_datetime(symbol_df['obs_date']) # Standardize date col?
            symbol_df['Symbol'] = user_symbol
            symbol_df = symbol_df.sort_values(by='Date').reset_index(drop=True)
        
        if symbol_df is not None and 'Close' not in symbol_df.columns:
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
