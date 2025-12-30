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

# Local imports
# Adjust path if necessary to find 'scripts' when running directly
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))
from scripts.common import core as mdc
from scripts.common import config as cfg

warnings.filterwarnings('ignore')

# Constants for Cloud Storage (Relative paths to Azure Container root)
CSV_FOLDER = "Price Targets"
BLACKLIST_FILE = "blacklist_price_targets.csv"
DF_COMBINED_PATH = "df_combined.csv"
NASDAQ_KEY_FILE = "nasdaq_key.txt"
OUTPUT_FILE = "df_price_targets.csv"

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

def calculate_null_percentage(df):
    """Calculates percentage of null/inf values per column."""
    if df.empty:
        return pd.Series()
    
    total_len = len(df)
    problematic_mask = df.isna() | (df == np.inf) | (df == -np.inf)
    null_percentage = (problematic_mask.sum()) / total_len * 100
    for column, percentage in null_percentage.items():
        mdc.write_line(f'{column}: {percentage:.2f}%')
    return null_percentage

def normalize_column(column):
    if column.max() == column.min():
        return column
    return (column - column.min()) / (column.max() - column.min())

def remove_outliers(df, columns):
    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    return df

def fetch_and_save_target_price_data(symbol, cloud_path):
    try:
        # Fetch data from API
        target_price_data = nasdaqdatalink.get_table('ZACKS/TP', ticker=symbol)
        mdc.write_line(f"    Retrieved {len(target_price_data)} rows for {symbol} from API.")
        
        # Save to Cloud
        mdc.store_csv(target_price_data, cloud_path)
        mdc.write_line(f"    Saved target price data for {symbol} to Cloud ({cloud_path}).")
        return target_price_data
    except Exception as e:
        mdc.write_line(f"    Error fetching data for {symbol}: {e}")
        return pd.DataFrame()

def process_symbol(symbol):
    try:
        column_names = ["ticker", "obs_date", "tp_mean_est",  "tp_std_dev_est",  "tp_high_est",  "tp_low_est",  "tp_cnt_est"  ,"tp_cnt_est_rev_up",  "tp_cnt_est_rev_down"]
        mdc.write_line(f"Processing symbol: {symbol}")
        
        # Cloud Path
        price_target_cloud_path = f"{CSV_FOLDER}/{symbol}.csv"
        
        existing_price_targets = pd.DataFrame(columns=column_names)
        is_fresh = False
        
        # Check freshness using Azure Blob metadata
        if mdc.storage_client:
             last_mod = mdc.storage_client.get_last_modified(price_target_cloud_path)
             if last_mod:
                 now_utc = datetime.now(timezone.utc)
                 if last_mod.tzinfo is None:
                     last_mod = last_mod.replace(tzinfo=timezone.utc)
                 if now_utc - last_mod < timedelta(days=7):
                     is_fresh = True

        if is_fresh:
            # If fresh, load and return immediately
            loaded_df = mdc.load_csv(price_target_cloud_path)
            if loaded_df is not None:
                if 'obs_date' in loaded_df.columns:
                    loaded_df['obs_date'] = pd.to_datetime(loaded_df['obs_date'])
                return loaded_df
        else:
            # If missing or stale, start with empty DF
            existing_price_targets = pd.DataFrame(columns=column_names)

        # Start date
        min_date = date(2020, 1, 1)

        mdc.write_line(f"   Fetching target price data for {symbol} > {min_date}")

        try:
            target_price_data = nasdaqdatalink.get_table(
                'ZACKS/TP',
                ticker=symbol,
                obs_date={'gte': min_date.strftime('%Y-%m-%d')}
            )
            
            if target_price_data.empty:
                 mdc.write_line(f"    No new data for {symbol}.")
            else:
                 target_price_data = target_price_data.sort_values(by='obs_date')

            # Blacklist check if no data found at all
            if target_price_data.empty and existing_price_targets.empty:
                  mdc.write_line(f"Blacklisting {symbol}.")
                  mdc.update_csv_set(BLACKLIST_FILE, symbol)
                  return None
            
            # If data found
            if not target_price_data.empty:
                min_date_found = target_price_data['obs_date'].min()
                max_date_found = target_price_data['obs_date'].max()
                mdc.write_line(f"    Retrieved {len(target_price_data)} rows for {symbol} between {min_date_found} and {max_date_found}.")
                
                target_price_data['obs_date'] = pd.to_datetime(target_price_data['obs_date'])
                latest_obs_date = target_price_data['obs_date'].max()
                
                # Carry forward logic
                today = pd.to_datetime("today").normalize()
                if latest_obs_date < today:
                    all_dates = pd.date_range(start=target_price_data['obs_date'].min(), end=today)
                    df_all_dates = pd.DataFrame({'obs_date': all_dates})
                    
                    target_price_data = df_all_dates.merge(target_price_data, on='obs_date', how='left')
                    target_price_data = target_price_data.ffill()
                    mdc.write_line(f"    Carried forward values for missing dates up to {today.date()}.")

        except Exception as e:
            mdc.write_line(f"    Error fetching data for {symbol}: {e}")
            return None

        if target_price_data.empty:
            return existing_price_targets

        # Ensure columns exist
        for col in column_names:
            if col not in target_price_data.columns:
                 target_price_data[col] = np.nan
                 
        target_price_data = target_price_data[column_names]

        # Resample logic
        target_price_data.set_index('obs_date', inplace=True)
        full_date_range = pd.date_range(start=target_price_data.index.min(), end=target_price_data.index.max(), freq='D')
        target_price_data = target_price_data.reindex(full_date_range)
        target_price_data.ffill(inplace=True)
        target_price_data.reset_index(inplace=True)
        target_price_data = target_price_data.rename(columns={'index': 'obs_date'})
        
        mdc.write_line(f"\nFinished processing {symbol} price target data.")

        # Combine
        updated_earnings = pd.concat([existing_price_targets, target_price_data], ignore_index=True)
        updated_earnings = updated_earnings.drop_duplicates(subset=['obs_date', 'ticker'], keep='last')
        updated_earnings = updated_earnings.sort_values(by=['obs_date', 'ticker']).reset_index(drop=True)

        # Save to Cloud
        mdc.store_csv(updated_earnings, price_target_cloud_path)
        mdc.write_line(f"  Uploaded updated data for {symbol} to {price_target_cloud_path}")

        return updated_earnings
    

    except Exception as e:
        mdc.write_line(f"ERROR: Failed in process_symbol({symbol}) {str(e)}")
        return None

def run_batch_processing():
    setup_nasdaq_key()
    
    # 1. Get Symbols
    df_symbols = mdc.get_symbols()
    
    # Cloud-aware blacklist filtering
    blacklist_list = mdc.load_ticker_list(BLACKLIST_FILE)
    if blacklist_list:
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(blacklist_list)]
        
    symbols = list(df_symbols['Symbol'].unique())
    mdc.write_line(f"Found {len(symbols)} unique symbols.")

    # 2. Worker config
    num_cores = os.cpu_count() or 1
    num_workers = max(1, int(num_cores * 0.75))
    mdc.write_line(f"Using {num_workers} worker threads.")

    updated_symbol_dfs = []

    # 3. Parallel Execution
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        future_to_symbol = {executor.submit(process_symbol, symbol): symbol for symbol in symbols}
        
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                res = future.result()
                if res is not None and not res.empty:
                    updated_symbol_dfs.append(res)
            except Exception as e:
                mdc.write_line(f"Exception for {symbol}: {e}")

    # 4. Save Final Aggregation
    if updated_symbol_dfs:
         updates_df = pd.concat(updated_symbol_dfs, ignore_index=True)
         updates_df.rename(columns={'ticker': 'Symbol', 'obs_date': 'Date'}, inplace=True)
         updates_df['Date'] = pd.to_datetime(updates_df['Date'])
         
         mdc.store_csv(updates_df, OUTPUT_FILE)
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
        df = mdc.load_csv(OUTPUT_FILE)
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
             # Basic implementation implies this field might come from merged data, 
             # but if this script only produces TP data, 'Close' might be missing 
             # unless we also merge with price data. 
             # For now, sticking to logic that exists.
             print("Close price not in dataset (this column requires price history).")
             # continue # Let user see TP data anyway?
             
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