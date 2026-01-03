
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
from azure.storage.blob import BlobLeaseClient
from scripts.common import config as cfg 
# NOTE: We are importing cfg here. If config depends on core, we have a cycle.
# Checking market_data.core imports: it imports config. 
# market_data.config usually just has constants. Safe.

def _has_storage_config() -> bool:
    return bool(
        os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
        or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    )

def _init_storage_client(container_name: str, error_context: str, error_types) -> Optional[BlobStorageClient]:
    if not _has_storage_config():
        return None
    try:
        return BlobStorageClient(container_name=container_name)
    except error_types as e:
        print(f"Warning: Failed to initialize {error_context}: {e}")
        return None

# Initialize Storage Client
# We keep this initialization here to be shared. 
# If different modules need different containers, this might need refactoring to a factory pattern.

common_storage_client = _init_storage_client(
    cfg.AZURE_CONTAINER_COMMON,
    "Azure Storage Client",
    (ValueError, AttributeError),
)

def get_storage_client(container_name: str) -> Optional[BlobStorageClient]:
    """Factory method to get a storage client for a specific container."""
    return _init_storage_client(
        container_name,
        f"client for {container_name}",
        (Exception,),
    )

# Create a logger for this module
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Logging Utilities
# ------------------------------------------------------------------------------

def write_line(msg: str):
    """Log a line with info level."""
    # Basic fall back if logging isn't configured upstream
    # Configure logging with separate streams if no handlers exist
    if not logging.getLogger().handlers:
        formatter = logging.Formatter('%(asctime)s: %(message)s')
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)

        # stdout handler for INFO and below (technically all, but we'll see)
        # Actually standard practice: stdout for everything, or filters. 
        # Simplest consistent with container best practice:
        # stdout: INFO, DEBUG
        # stderr: WARNING, ERROR, CRITICAL
        
        # Handler for stdout
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        # Filter to ensure only INFO and DEBUG go here if we want strict separation, 
        # but typically stdout having everything + stderr having errors is also common (duplication).
        # Let's do strict separation for cleaner logs.
        class MaxLevelFilter(logging.Filter):
            def __init__(self, max_level):
                self.max_level = max_level
            def filter(self, record):
                return record.levelno <= self.max_level

        stdout_handler.addFilter(MaxLevelFilter(logging.INFO))
        stdout_handler.setFormatter(formatter)
        root_logger.addHandler(stdout_handler)

        # Handler for stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        stderr_handler.setFormatter(formatter)
        root_logger.addHandler(stderr_handler)

    logger.info(msg)

def write_error(msg: str):
    """Log a line with error level (stderr)."""
    logger.error(msg)

def write_warning(msg: str):
    """Log a line with warning level (stderr)."""
    logger.warning(msg)

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

def store_csv(
    obj: pd.DataFrame,
    file_path: Union[str, Path],
    client: Optional[BlobStorageClient] = None,
) -> str:
    """
    Stores a DataFrame to Azure Blob Storage as CSV.
    file_path: Remote path or local path (converted).
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)

    if client is None:
        raise RuntimeError("Azure Storage Client not provided. Cannot store CSV.")

    client.write_csv(remote_path, obj)
    return remote_path

def load_csv(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> Optional[pd.DataFrame]:
    """
    Loads a CSV from Azure Blob Storage.
    file_path: Can be a local path (for compatibility, converted to remote) or relative remote path.
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)
    
    if client is None:
         raise RuntimeError("Azure Storage Client not provided. Cannot load CSV.")

    # Let errors propagate (File not found, permission denied, etc)

    return client.read_csv(remote_path)

def load_common_csv(file_path):
    """
    Loads a CSV from the COMMON Azure Blob Storage container.
    """
    remote_path = get_remote_path(file_path)
    
    if common_storage_client is None:
         return None

    return common_storage_client.read_csv(remote_path)

def store_common_csv(obj: pd.DataFrame, file_path):
    """
    Stores a DataFrame to the COMMON Azure Blob Storage container as CSV.
    """
    remote_path = get_remote_path(file_path)
    
    if common_storage_client is None:
        raise RuntimeError("Azure Common Storage Client not initialized. Cannot store CSV.")
        
    common_storage_client.write_csv(remote_path, obj)
    return remote_path

def update_common_csv_set(file_path, ticker):
    """
    Adds a ticker to a CSV file in the COMMON Azure container if it doesn't exist.
    """
    try:
        remote_path = get_remote_path(file_path)

        df = pd.DataFrame(columns=['Symbol'])
        
        # Load existing
        existing_df = load_common_csv(remote_path)
        if existing_df is not None and not existing_df.empty:
            df = existing_df
            if 'Symbol' not in df.columns:
                 df.columns = ['Symbol']

        if ticker not in df['Symbol'].values:
            new_row = pd.DataFrame([{'Symbol': ticker}])
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.sort_values('Symbol').reset_index(drop=True)
            
            store_common_csv(df, remote_path)
            write_line(f"Added {ticker} to {remote_path} (Common Container)")
    except Exception as e:
        write_error(f"Error updating common {file_path}: {e}")


def update_csv_set(file_path, ticker, client: Optional[BlobStorageClient] = None):
    """
    Adds a ticker to a CSV file in Azure if it doesn't exist, ensuring uniqueness and sorting.
    client: Optional specific client to use.
    """
    try:
        remote_path = get_remote_path(file_path)

        df = pd.DataFrame(columns=['Symbol'])
        
        # Load existing
        existing_df = load_csv(remote_path, client=client)
        if existing_df is not None and not existing_df.empty:
            df = existing_df
            if 'Symbol' not in df.columns:
                 df.columns = ['Symbol']

        if ticker not in df['Symbol'].values:
            new_row = pd.DataFrame([{'Symbol': ticker}])
            df = pd.concat([df, new_row], ignore_index=True)
            df = df.sort_values('Symbol').reset_index(drop=True)
            
            store_csv(df, remote_path, client=client)
            write_line(f"Added {ticker} to {remote_path}")
    except Exception as e:
        write_error(f"Error updating {file_path}: {e}")

def store_parquet(df: pd.DataFrame, file_path: Union[str, Path], client: Optional[BlobStorageClient] = None):
    """
    Stores a DataFrame as a Parquet file in Azure Blob Storage.
    file_path: Relative path in the container (e.g. 'Yahoo/Price Data/AAPL.parquet')
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)
    
    if client is None:
        return # Skip cloud op

    # Convert to Parquet bytes
    parquet_bytes = df.to_parquet(index=False)
    
    client.write_blob(remote_path, parquet_bytes)
    return remote_path

def load_parquet(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> Optional[pd.DataFrame]:
    """
    Loads a Parquet file from Azure Blob Storage.
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)
    
    if client is None:
        return None
        
    try:
        blob_data = client.read_blob(remote_path)
        if blob_data:
            from io import BytesIO
            return pd.read_parquet(BytesIO(blob_data))
    except Exception:
        pass
        
    return None

def get_file_text(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> Optional[str]:
    """Retrieves file content as text from Azure. Raises error if failed or missing."""
    if client:
        blob_name = get_remote_path(file_path)
        content_bytes = client.download_data(blob_name)
        if content_bytes:
            return content_bytes.decode('utf-8')
    
    # If we get here, either no client or no data
    logger.warning(f"Failed to load {file_path} from cloud (client={client is not None}).")
    return None

def get_common_file_text(file_path: Union[str, Path]) -> Optional[str]:
    """Retrieves file content as text from the COMMON Azure container."""
    if common_storage_client:
        blob_name = get_remote_path(file_path)
        content_bytes = common_storage_client.download_data(blob_name)
        if content_bytes:
            return content_bytes.decode('utf-8')
    
    logger.warning(f"Failed to load {file_path} from common cloud container.")
    return None



def store_file(local_path: str, remote_path: str, client: Optional[BlobStorageClient] = None):
    """
    Stores a generic file (binary) to Azure Blob Storage.
    """
    if client:
        # Read file
        with open(local_path, "rb") as data:
             client.upload_file(data, remote_path)
    else:
        write_line(f"No storage client. File remains local: {local_path}")

def save_file_text(content: str, file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> None:
    """Saves text content to Azure."""
    if client:
        blob_name = get_remote_path(file_path)
        client.upload_data(blob_name, content.encode('utf-8'), overwrite=True)
    else:
         raise RuntimeError(f"Cannot save {file_path}: Azure Client not initialized.")

import json
def get_json_content(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> Optional[dict]:
    """Retrieves JSON content from Azure."""
    text = get_file_text(file_path, client=client)
    if text:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            write_error(f"Error decoding JSON from {file_path}: {e}")
    return None

def get_common_json_content(file_path: Union[str, Path]) -> Optional[dict]:
    """Retrieves JSON content from the COMMON Azure container."""
    text = get_common_file_text(file_path)
    if text:
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            write_error(f"Error decoding JSON from common {file_path}: {e}")
    return None

def save_json_content(data: dict, file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> None:
    """Saves dictionary as JSON to Azure."""
    text = json.dumps(data, indent=2)
    save_file_text(text, file_path, client=client)

def save_common_json_content(data: dict, file_path: Union[str, Path]) -> None:
    """Saves dictionary as JSON to the COMMON Azure container."""
    text = json.dumps(data, indent=2)
    
    if common_storage_client:
        blob_name = get_remote_path(file_path)
        common_storage_client.upload_data(blob_name, text.encode('utf-8'), overwrite=True)
    else:
        raise RuntimeError(f"Cannot save {file_path}: Common Azure Client not initialized.")

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
                write_error(f"Error deleting file {file}: {e}")

def load_ticker_list(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> list:
    """
    Loads a list of tickers from a CSV file in Azure. 
    Assumes file has a header like 'Ticker' or 'Symbol', or is headerless.
    client: Optional specific client to use.
    """
    # load_csv handles remote path conversion and Azure loading
    # It now raises errors if failed, which we propagate.
    df = load_csv(file_path, client=client)
    
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

def load_common_ticker_list(file_path: Union[str, Path]) -> list:
    """
    Loads a list of tickers from a CSV file in the COMMON Azure container.
    """
    df = load_common_csv(file_path)
    
    if df is None or df.empty:
        return []
        
    # Standardize column name check
    col_name = None
    if 'Symbol' in df.columns:
        col_name = 'Symbol'
    elif 'Ticker' in df.columns:
        col_name = 'Ticker'
            
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
         print(f"Warning: NASDAQ_API_KEY environment variable is missing. Active tickers fetch may fail or be limited.")
            
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
        write_error(f"Failed to get active tickers: {e}")
        return pd.DataFrame(columns=['Symbol'])

def get_symbols():
    df_symbols = pd.DataFrame()
    file_path = "df_symbols.csv" 
    
    # Try to load from Azure Cache (Use Common Container)
    df_symbols = load_common_csv(file_path)
    
    if df_symbols is None or df_symbols.empty:
        write_line("Local symbol cache missing or empty. Fetching from NASDAQ API...")
        df_symbols = get_active_tickers() 
        store_common_csv(df_symbols, file_path)
    else:
        write_line(f"Loaded {len(df_symbols)} symbols from Azure cache (Common).")
        
    if 'Symbol' not in df_symbols.columns:
        df_symbols['Symbol'] = pd.Series(dtype='object')
        
    tickers_to_add = cfg.TICKERS_TO_ADD
    
    # Logic note: Each scraper now manages its own whitelist/blacklist for isolation.
    # We return the raw symbols list (including manual additions).
    df_symbols = df_symbols.reset_index(drop=True)
    
    # Mix in manual additions
    for ticker_to_add in tickers_to_add:
        if not ticker_to_add['Symbol'] in df_symbols['Symbol'].to_list():
            df_symbols = pd.concat([df_symbols, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)
            
    df_symbols.drop_duplicates()
    store_common_csv(df_symbols, file_path)
    
    # Specific artifact creation
    store_common_csv(pd.DataFrame(tickers_to_add), 'market_analysis_tickers.csv')
    store_common_csv(df_symbols, 'stock_tickers.csv')
    
    return df_symbols

def is_weekend(date):
    return date.weekday() >= 5

# ------------------------------------------------------------------------------
# Concurrency / Locking (Distributed Lock via Azure Blob Lease)
# ------------------------------------------------------------------------------

class JobLock:
    """
    Context manager for distributed locking using Azure Blob Storage Leases.
    Ensures that only one instance of a job runs at a time.
    """
    def __init__(self, job_name: str, lease_duration: int = 60):
        self.job_name = job_name
        self.lease_duration = lease_duration
        self.lock_blob_name = f"locks/{job_name}.lock"
        self.lease_client = None
        self.blob_client = None

    def __enter__(self):
        write_line(f"Acquiring lock for {self.job_name}...")
        
        if common_storage_client is None:
            write_warning("Common storage client not initialized. Skipping lock check (UNSAFE concurrency).")
            return self

        # 1. Ensure lock file exists
        if not common_storage_client.file_exists(self.lock_blob_name):
            try:
                # Create empty lock file
                common_storage_client.upload_data(self.lock_blob_name, b"", overwrite=False)
            except Exception:
                # Ignore if created by race condition
                pass
        
        # 2. Get Blob Client from the underlying client
        # Note: self.common_storage_client.service_client is usually the account client.
        # We need the blob client for the specific blob.
        # Assuming BlobStorageClient exposes .get_blob_client(blob_name) or we can derive it.
        # Looking at blob_storage.py (inferred), usually it wraps ContainerClient.
        # We need to access the underlying ContainerClient to get a BlobClient.
        
        try:
             # Access internal container client
             container_client = common_storage_client.container_client
             self.blob_client = container_client.get_blob_client(self.lock_blob_name)
             self.lease_client = BlobLeaseClient(self.blob_client)
             
             # 3. Acquire Lease
             self.lease_client.acquire(lease_duration=self.lease_duration)
             write_line(f"Lock acquired for {self.job_name}. Lease ID: {self.lease_client.id}")
             return self
             
        except Exception as e:
            # If acquire fails (Conflict/409), it means locked.
            write_error(f"Failed to acquire lock for {self.job_name}: {e}")
            write_line("Another instance is likely running. Exiting.")
            sys.exit(0) # Graceful exit

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.lease_client:
            try:
                write_line(f"Releasing lock for {self.job_name}...")
                self.lease_client.release()
            except Exception as e:
                write_error(f"Error releasing lock: {e}")
