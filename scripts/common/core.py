
import logging
import glob
import os
import sys
import re
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Union, Optional

import pandas as pd
import numpy as np
import nasdaqdatalink

# Local imports
from scripts.common.blob_storage import BlobStorageClient
from scripts.common import config as cfg 
# NOTE: We are importing cfg here. If config depends on core, we have a cycle.
# Checking market_data.core imports: it imports config. 
# market_data.config usually just has constants. Safe.

# Initialize Storage Client
# We keep this initialization here to be shared. 
# If different modules need different containers, this might need refactoring to a factory pattern.
try:
    # Assuming cfg.AZURE_CONTAINER_NAME is available and correct
    storage_client = BlobStorageClient(container_name=cfg.AZURE_CONTAINER_NAME)
except (ValueError, AttributeError):
    # print("Warning: AZURE_STORAGE_CONNECTION_STRING not found or config missing. Azure operations will fail.")
    storage_client = None

# Create a logger for this module
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Logging Utilities
# ------------------------------------------------------------------------------

def write_line(msg):
    '''
    Log a line with info level
    '''
    # Basic fall back if logging isn't configured upstream
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s')
    logger.info(msg)

def write_inline(text, endline=False):
    if not endline:
        sys.stdout.write('\r' + ' ' * 120 + '\r')
        sys.stdout.flush()
        ct = datetime.now()
        ct_str = ct.strftime('%Y-%m-%d %H:%M:%S')
        print('{}: {}'.format(ct_str, text), end='')
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

def get_current_timestamp_str():
    """Returns the current date and time as a formatted string."""
    return datetime.now().strftime("%d-%m-%Y %H:%M")

def go_to_sleep(range_low = 5, range_high = 20):
    # sleep for certain amount of time
    sleep_time = random.randint(range_low, range_high)
    write_line(f'Sleeping for {sleep_time} seconds...')
    time.sleep(sleep_time)

# ------------------------------------------------------------------------------
# File I/O Utilities (Azure Aware)
# ------------------------------------------------------------------------------

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

def store_csv(obj: pd.DataFrame, file_path):
    """
    Stores a DataFrame to Azure Blob Storage as CSV.
    file_path: Remote path or local path (converted).
    """
    remote_path = get_remote_path(file_path)
    
    if storage_client is None:
        raise RuntimeError("Azure Storage Client not initialized. Cannot store CSV.")
        
    storage_client.write_csv(remote_path, obj)
    return remote_path

def load_csv(file_path) -> object:
    """
    Loads a CSV from Azure Blob Storage.
    file_path: Can be a local path (for compatibility, converted to remote) or relative remote path.
    """
    remote_path = get_remote_path(file_path)
    
    if storage_client is None:
         raise RuntimeError("Azure Storage Client not initialized. Cannot load CSV.")

    # Let errors propagate (File not found, permission denied, etc)
    return storage_client.read_csv(remote_path)

def update_csv_set(file_path, ticker):
    """
    Adds a ticker to a CSV file in Azure if it doesn't exist, ensuring uniqueness and sorting.
    """
    try:
        remote_path = get_remote_path(file_path)

        df = pd.DataFrame(columns=['Symbol'])
        
        # Load existing
        existing_df = load_csv(remote_path)
        if existing_df is not None and not existing_df.empty:
            df = existing_df
            if 'Symbol' not in df.columns:
                 df.columns = ['Symbol']

        if ticker not in df['Symbol'].values:
            new_row = pd.DataFrame([{'Symbol': ticker}])
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.sort_values('Symbol').reset_index(drop=True)
            
            store_csv(df, remote_path)
            write_line(f"Added {ticker} to {remote_path}")
    except Exception as e:
        write_line(f"Error updating {file_path}: {e}")

def store_parquet(obj: pd.DataFrame, file_path):
    """
    Stores a DataFrame to Azure Blob Storage as Parquet.
    file_path: Remote path or local path (converted).
    """
    remote_path = get_remote_path(file_path)
    
    if storage_client is None:
        raise RuntimeError("Azure Storage Client not initialized. Cannot store Parquet.")
        
    storage_client.write_parquet(remote_path, obj)
    return remote_path

def load_parquet(file_path) -> object:
    """
    Loads a Parquet file from Azure Blob Storage.
    file_path: Can be a local path (for compatibility, converted to remote) or relative remote path.
    """
    remote_path = get_remote_path(file_path)
    
    if storage_client is None:
         raise RuntimeError("Azure Storage Client not initialized. Cannot load Parquet.")

    # Let errors propagate (File not found, permission denied, etc)
    return storage_client.read_parquet(remote_path)


def get_file_text(file_path: Union[str, Path]) -> Optional[str]:
    """Retrieves file content as text from Azure. Raises error if failed or missing."""
    if storage_client:
        blob_name = get_remote_path(file_path)
        content_bytes = storage_client.download_data(blob_name)
        if content_bytes:
            return content_bytes.decode('utf-8')
    
    # If we get here, either no client or no data
    logger.warning(f"Failed to load {file_path} from cloud.")
    return None

def save_file_text(content: str, file_path: Union[str, Path]) -> None:
    """Saves text content to Azure."""
    if storage_client:
        blob_name = get_remote_path(file_path)
        storage_client.upload_data(blob_name, content.encode('utf-8'), overwrite=True)
    else:
         raise RuntimeError(f"Cannot save {file_path}: Azure Client not initialized.")

import json
def get_json_content(file_path: Union[str, Path]) -> Optional[dict]:
    """Retrieves JSON content from Azure."""
    text = get_file_text(file_path)
    if text:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            write_line(f"Error decoding JSON from {file_path}: {e}")
    return None

def save_json_content(data: dict, file_path: Union[str, Path]) -> None:
    """Saves dictionary as JSON to Azure."""
    text = json.dumps(data, indent=2)
    save_file_text(text, file_path)

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
    
    if matching_files:
        for file in matching_files:
            try:
                os.remove(file)
                print(f"Deleted file: {file}")
            except OSError as e:
                print(f"Error deleting file {file}: {e}")

def load_ticker_list(file_path: Union[str, Path]) -> list:
    """
    Loads a list of tickers from a CSV file in Azure. 
    Assumes file has a header like 'Ticker' or 'Symbol', or is headerless.
    """
    # load_csv handles remote path conversion and Azure loading
    # It now raises errors if failed, which we propagate.
    df = load_csv(file_path)
    
    if df is None or df.empty:
        return []
        
    # Standardize column name check
    col_name = None
    if 'Ticker' in df.columns:
        col_name = 'Ticker'
    elif 'Symbol' in df.columns:
            col_name = 'Symbol'
            
    if col_name:
        return df[col_name].dropna().unique().tolist()
        
    # If no standard header, try first column
    return df.iloc[:, 0].dropna().unique().tolist()

# ------------------------------------------------------------------------------
# Symbol Management
# ------------------------------------------------------------------------------

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
        # Try loading from Azure/Common
        key_content = get_file_text('nasdaq_key.txt')
        if key_content:
            nasdaqdatalink.ApiConfig.api_key = key_content.strip()
            # write_line("Loaded NASDAQ API key from storage.")
        else:
             print(f"Warning: NASDAQ API key not found in Environment or Azure.")
            
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
        return pd.DataFrame(columns=['Symbol'])

def get_symbols():
    df_symbols = pd.DataFrame()
    file_path = "df_symbols.csv" 
    
    # Try to load from Azure Cache
    df_symbols = load_csv(file_path)
    
    if df_symbols is None or df_symbols.empty:
        write_line("Local symbol cache missing or empty. Fetching from NASDAQ API...")
        df_symbols = get_active_tickers() 
        store_csv(df_symbols, file_path)
    else:
        write_line(f"Loaded {len(df_symbols)} symbols from Azure cache.")
        
    if 'Symbol' not in df_symbols.columns:
        df_symbols['Symbol'] = pd.Series(dtype='object')
        
    tickers_to_add = cfg.TICKERS_TO_ADD
    
    # Logic note: We assume calling code might want to apply blacklists *after* getting the raw list,
    # OR we apply it here. Original code applied it here.
    
    blacklist_path = 'scripts/common/blacklist.csv'
    blacklist_fin_path = 'scripts/common/blacklist_financial.csv'
    
    symbols_to_remove = set()
    
    # Update to use load_ticker_list which we will update to be Cloud-Aware
    symbols_to_remove.update(load_ticker_list(blacklist_path))
    symbols_to_remove.update(load_ticker_list(blacklist_fin_path))
    
    if symbols_to_remove:
        write_line(f"Excluding {len(symbols_to_remove)} blacklisted symbols.")
        df_symbols = df_symbols[~df_symbols['Symbol'].isin(symbols_to_remove)]

    df_symbols = df_symbols.reset_index(drop=True)
    
    # Mix in manual additions
    for ticker_to_add in tickers_to_add:
        if not ticker_to_add['Symbol'] in df_symbols['Symbol'].to_list():
            df_symbols = pd.concat([df_symbols, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)
            
    df_symbols.drop_duplicates()
    store_csv(df_symbols, file_path)
    
    # Specific artifact creation
    store_csv(pd.DataFrame(tickers_to_add), 'market_analysis_tickers.csv')
    store_csv(df_symbols, 'stock_tickers.csv')
    
    return df_symbols

def is_weekend(date):
    return date.weekday() >= 5
