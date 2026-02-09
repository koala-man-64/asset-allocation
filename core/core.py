
import logging
import glob
import json
import os
import sys
import re
import random
import threading
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Union, Optional
from core.massive_provider import get_complete_ticker_list

import pandas as pd
import numpy as np
import nasdaqdatalink

# Local imports
from .blob_storage import BlobStorageClient
from azure.storage.blob import BlobLeaseClient
from . import config as cfg
from azure.core.exceptions import HttpResponseError, ResourceExistsError
from core.postgres import connect, copy_rows
# NOTE: We are importing cfg here. If config depends on core, we have a cycle.
# Checking market_data.core imports: it imports config. 
# market_data.config usually just has constants. Safe.

def _is_truthy(raw: Optional[str]) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True
    return _is_truthy(os.environ.get("TEST_MODE"))


def _has_storage_config() -> bool:
    val = bool(
        os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
        or os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    )
    if not val:
        logger.warning("Storage Config Missing: Neither AZURE_STORAGE_ACCOUNT_NAME nor AZURE_STORAGE_CONNECTION_STRING is set in environment.")
        # Flush to ensure this warning persists before any potential crash
        sys.stdout.flush()
    return val

def _init_storage_client(container_name: str, error_context: str, error_types) -> Optional[BlobStorageClient]:
    # In tests we avoid creating real Azure clients to prevent network calls and auth flakiness.
    if _is_test_environment():
        return None
    if not _has_storage_config():
        return None
    try:
        return BlobStorageClient(container_name=container_name)
    except error_types as e:
        logger.warning(f"Failed to initialize {error_context}: {e}")
        sys.stdout.flush()
        return None

# Create a logger for this module
logger = logging.getLogger(__name__)

# Initialize Storage Client
# We keep this initialization here to be shared. 
# If different modules need different containers, this might need refactoring to a factory pattern.

if _is_test_environment():
    common_storage_client = None
    logger.info("Test environment detected (PYTEST_CURRENT_TEST or TEST_MODE). Skipping global common_storage_client initialization to prevent network calls.")
else:
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

def log_environment_diagnostics():
    """
    Logs selected environment variables for debugging purposes.

    SECURITY:
    - Does not dump the full environment by default.
    - Redacts values for sensitive keys (secrets/credentials/PII).

    Set ENABLE_ENV_DIAGNOSTICS=true to log a broader (still allowlisted) set.
    """

    def _is_truthy(value: str) -> bool:
        return (value or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}

    write_section("ENVIRONMENT DIAGNOSTICS", "Logging selected environment variables...")

    applied_runtime_config: dict[str, str] = {}
    try:
        from core.runtime_config import apply_runtime_config_to_env, default_scopes_by_precedence

        applied_runtime_config = apply_runtime_config_to_env(
            scopes_by_precedence=default_scopes_by_precedence()
        )
        if applied_runtime_config:
            logger.info(
                "Runtime config overrides loaded from Postgres: %s",
                sorted(applied_runtime_config.keys()),
            )
    except Exception as exc:
        logger.warning("Runtime config refresh skipped: %s", exc)

    if applied_runtime_config:
        try:
            from core.config import reload_settings

            reload_settings()
        except Exception as exc:
            logger.warning("Settings reload skipped: %s", exc)
        try:
            import hashlib
            import json

            digest = hashlib.sha256(
                json.dumps(applied_runtime_config, sort_keys=True, separators=(",", ":")).encode(
                    "utf-8"
                )
            ).hexdigest()
            logger.info("Runtime config applied hash=%s", digest[:12])
        except Exception:
            pass

    try:
        from core.debug_symbols import refresh_debug_symbols_from_db

        debug_symbols = refresh_debug_symbols_from_db()
        if debug_symbols:
            preview = ", ".join(debug_symbols[:8])
            suffix = "..." if len(debug_symbols) > 8 else ""
            logger.info(
                "Debug symbols loaded from Postgres (%s): %s%s",
                len(debug_symbols),
                preview,
                suffix,
            )
            try:
                import hashlib
                import json

                digest = hashlib.sha256(
                    json.dumps(list(debug_symbols), separators=(",", ":"), sort_keys=False).encode(
                        "utf-8"
                    )
                ).hexdigest()
                logger.info("Debug symbols hash=%s", digest[:12])
            except Exception:
                pass
        else:
            logger.info("Debug symbols disabled or empty.")
    except Exception as exc:
        logger.warning("Debug symbols refresh skipped: %s", exc)

    sensitive_patterns = [
        "KEY",
        "SECRET",
        "PASSWORD",
        "TOKEN",
        "CONN",
        "DSN",
        "AUTH",
        "USERNAME",
        "USER",
        "EMAIL",
    ]

    base_allowlist = [
        # Container Apps / runtime context
        "CONTAINER_APP_JOB_NAME",
        "CONTAINER_APP_JOB_EXECUTION_NAME",
        "CONTAINER_APP_REPLICA_NAME",
        "CONTAINER_APP_ENV_DNS_SUFFIX",
        # Storage + container routing (non-secret identifiers)
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_CONTAINER_COMMON",
        "AZURE_CONTAINER_BRONZE",
        "AZURE_CONTAINER_SILVER",
        "AZURE_CONTAINER_GOLD",
        "AZURE_FOLDER_FINANCE",
        "AZURE_FOLDER_MARKET",
        "AZURE_FOLDER_EARNINGS",
        "AZURE_FOLDER_TARGETS",
        "AZURE_CONTAINER_PLATINUM",
        # Job behavior toggles
        "LOG_FORMAT",
        "FEATURE_ENGINEERING_MAX_WORKERS",
        "MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR",
        "MATERIALIZE_YEAR_MONTH",
        "TEST_MODE",
    ]

    verbose_allowlist = [
        "PYTHONUNBUFFERED",
        "PYTHONIOENCODING",
        "LANG",
        "TZ",
    ]

    keys = list(base_allowlist)
    if _is_truthy(os.environ.get("ENABLE_ENV_DIAGNOSTICS", "")):
        keys.extend(verbose_allowlist)

    for key in sorted(set(keys)):
        value = os.environ.get(key, "")
        is_sensitive = any(pattern in key.upper() for pattern in sensitive_patterns)

        if is_sensitive:
            logger.info("%s = [REDACTED]", key)
        else:
            logger.info("%s = %s", key, value)

    sys.stdout.flush()

# ------------------------------------------------------------------------------
# Logging Utilities
# ------------------------------------------------------------------------------

from core.logging_config import configure_logging

# Ensure logging is configured on import
configure_logging()

def write_line(msg: str):
    """Log a line with info level."""
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
    file_path: Relative path in the container (e.g. 'market-data/AAPL.parquet')
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)
    
    if client is None:
        return # Skip cloud op

    # Convert to Parquet bytes (handled by client.write_parquet)
    client.write_parquet(remote_path, df)
    return remote_path

def load_parquet(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> Optional[pd.DataFrame]:
    """
    Loads a Parquet file from Azure Blob Storage.
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)
    
    if client is None:
        return None
    return client.read_parquet(remote_path)

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
        client.upload_file(local_path, remote_path)
    else:
        write_line(f"No storage client. File remains local: {local_path}")

def save_file_text(content: str, file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> None:
    """Saves text content to Azure."""
    if client:
        blob_name = get_remote_path(file_path)
        client.upload_data(blob_name, content.encode('utf-8'), overwrite=True)
    else:
         raise RuntimeError(f"Cannot save {file_path}: Azure Client not initialized.")

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

    # nasdaqdatalink.ApiConfig.verify_ssl = False # SSL Verification Enabled
    api_key = os.environ.get('NASDAQ_API_KEY')
    if api_key:
        nasdaqdatalink.ApiConfig.api_key = api_key
    else:
         print("Warning: NASDAQ_API_KEY environment variable is missing. Active tickers fetch may fail or be limited.")
            
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


def _parse_alpha_vantage_listing_status_csv(csv_text: str) -> pd.DataFrame:
    """
    Parse Alpha Vantage LISTING_STATUS CSV into a normalized symbol DataFrame.

    Alpha Vantage columns:
      symbol,name,exchange,assetType,ipoDate,delistingDate,status

    Returns
    -------
    pandas.DataFrame
        Columns: Symbol, Name, Exchange, AssetType, IpoDate, DelistingDate, Status
    """
    raw = (csv_text or "").strip()
    if not raw:
        return pd.DataFrame(columns=["Symbol"])

    df = pd.read_csv(StringIO(raw), dtype=str, keep_default_na=False)
    if df.empty:
        return pd.DataFrame(columns=["Symbol"])

    rename_map = {
        "symbol": "Symbol",
        "name": "Name",
        "exchange": "Exchange",
        "assetType": "AssetType",
        "ipoDate": "IpoDate",
        "delistingDate": "DelistingDate",
        "status": "Status",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "Symbol" not in df.columns:
        return pd.DataFrame(columns=["Symbol"])

    for col in ["Symbol", "Name", "Exchange", "AssetType", "IpoDate", "DelistingDate", "Status"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Alpha Vantage uses "null" for some empty cells (e.g., delistingDate).
    for col in ["IpoDate", "DelistingDate"]:
        if col in df.columns:
            df[col] = df[col].replace({"null": "", "None": "", "nan": ""})

    df = df[df["Symbol"].astype(str).str.strip().ne("")]

    # Filter to active equities by default; callers can merge other sets if desired.
    if "Status" in df.columns:
        df = df[df["Status"].str.upper() == "ACTIVE"]
    if "AssetType" in df.columns:
        df = df[df["AssetType"].str.upper() == "STOCK"]

    keep = [c for c in ["Symbol", "Name", "Exchange", "AssetType", "IpoDate", "DelistingDate", "Status"] if c in df.columns]
    df = df[keep].drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    return df


def get_active_tickers_alpha_vantage() -> pd.DataFrame:
    """
    Fetch active tickers from Alpha Vantage via LISTING_STATUS.

    Preferred path for ETL jobs is to call the API-hosted Alpha Vantage gateway.

    Fallback (local/dev only): call Alpha Vantage directly when ALPHA_VANTAGE_API_KEY is present.
    """
    # Prefer the API gateway if configured.
    if os.environ.get("ASSET_ALLOCATION_API_BASE_URL") or os.environ.get("ASSET_ALLOCATION_API_URL"):
        try:
            from core.alpha_vantage_gateway_client import AlphaVantageGatewayClient

            with AlphaVantageGatewayClient.from_env() as av_gateway:
                csv_text = av_gateway.get_listing_status_csv(state="active")
            return _parse_alpha_vantage_listing_status_csv(str(csv_text))
        except Exception as exc:
            write_error(f"Failed to get active tickers via API gateway: {exc}")
            return pd.DataFrame(columns=["Symbol"])

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key or not api_key.strip():
        write_warning(
            "Alpha Vantage symbol fetch disabled (no ASSET_ALLOCATION_API_BASE_URL and no ALPHA_VANTAGE_API_KEY)."
        )
        return pd.DataFrame(columns=["Symbol"])

    # Fallback: direct Alpha Vantage call (used for local/dev only).
    try:
        from alpha_vantage import AlphaVantageClient, AlphaVantageConfig

        rate_limit = int(os.environ.get("ALPHA_VANTAGE_RATE_LIMIT_PER_MIN", "300") or 300)
        timeout = float(os.environ.get("ALPHA_VANTAGE_TIMEOUT_SECONDS", "15") or 15.0)

        av_cfg = AlphaVantageConfig(
            api_key=str(api_key).strip(),
            rate_limit_per_min=rate_limit,
            timeout=timeout,
            max_workers=1,
            max_retries=3,
            backoff_base_seconds=0.5,
        )

        with AlphaVantageClient(av_cfg) as av:
            csv_text = av.get_listing_status(state="active")
        return _parse_alpha_vantage_listing_status_csv(str(csv_text))
    except Exception as exc:
        write_error(f"Failed to get active tickers from Alpha Vantage: {exc}")
        return pd.DataFrame(columns=["Symbol"])


def get_active_tickers_massive() -> pd.DataFrame:
    """
    Fetch active tickers from Massive Provider.
    """
    api_key = cfg.MASSIVE_API_KEY
    if not api_key:
        write_warning("Massive symbol fetch disabled (MASSIVE_API_KEY missing).")
        return pd.DataFrame(columns=["Symbol"])

    try:
        df = get_complete_ticker_list(
            api_key=api_key,
            base_url=cfg.MASSIVE_BASE_URL,
            timeout_seconds=cfg.MASSIVE_TIMEOUT_SECONDS,
            page_limit=cfg.MASSIVE_TICKERS_PAGE_LIMIT,
            active=True
        )
        return df
    except Exception as exc:
        write_error(f"Failed to get active tickers from Massive: {exc}")
        return pd.DataFrame(columns=["Symbol"])


def merge_symbol_sources(df_nasdaq: pd.DataFrame, df_alpha_vantage: pd.DataFrame, df_massive: pd.DataFrame) -> pd.DataFrame:
    """
    Merge NASDAQ + Alpha Vantage + Massive symbol universes into a single DataFrame.

    Precedence:
      - Prefer NASDAQ Name/sector/industry metadata when present.
      - Use Massive for additional coverage and metadata.
      - Use Alpha Vantage as fallback.
    """
    df_n = df_nasdaq.copy() if df_nasdaq is not None else pd.DataFrame()
    df_a = df_alpha_vantage.copy() if df_alpha_vantage is not None else pd.DataFrame()
    df_m = df_massive.copy() if df_massive is not None else pd.DataFrame()

    if "Symbol" not in df_n.columns:
        df_n["Symbol"] = pd.Series(dtype="object")
    if "Symbol" not in df_a.columns:
        df_a["Symbol"] = pd.Series(dtype="object")
    if "Symbol" not in df_m.columns:
        df_m["Symbol"] = pd.Series(dtype="object")

    df_n["Symbol"] = df_n["Symbol"].astype(str).str.strip()
    df_a["Symbol"] = df_a["Symbol"].astype(str).str.strip()
    df_m["Symbol"] = df_m["Symbol"].astype(str).str.strip()
    
    df_n = df_n[df_n["Symbol"].ne("")]
    df_a = df_a[df_a["Symbol"].ne("")]
    df_m = df_m[df_m["Symbol"].ne("")]

    nasdaq_cols = [c for c in ["Symbol", "Name", "Description", "Sector", "Industry", "Industry_2", "Optionable", "Country"] if c in df_n.columns]
    alpha_cols = [c for c in ["Symbol", "Name", "Exchange", "AssetType", "IpoDate", "DelistingDate", "Status"] if c in df_a.columns]
    massive_cols = [c for c in ["Symbol", "Name", "Exchange", "AssetType", "Locale", "Market", "CurrencyName", "Active"] if c in df_m.columns]

    left = df_n[nasdaq_cols].drop_duplicates(subset=["Symbol"]) if nasdaq_cols else df_n[["Symbol"]].drop_duplicates(subset=["Symbol"])
    right_a = df_a[alpha_cols].drop_duplicates(subset=["Symbol"]) if alpha_cols else df_a[["Symbol"]].drop_duplicates(subset=["Symbol"])
    right_m = df_m[massive_cols].drop_duplicates(subset=["Symbol"]) if massive_cols else df_m[["Symbol"]].drop_duplicates(subset=["Symbol"])

    # Add explicit source markers
    left = left.copy()
    right_a = right_a.copy()
    right_m = right_m.copy()
    left["source_nasdaq"] = True
    right_a["source_alpha_vantage"] = True
    right_m["source_massive"] = True

    # Merge: NASDAQ outer join Massive outer join Alpha Vantage
    merged = left.merge(right_m, on="Symbol", how="outer", suffixes=("_nasdaq", "_massive"))
    merged = merged.merge(right_a, on="Symbol", how="outer", suffixes=("", "_av"))
    # Note: suffixes handling for triple merge might need adjustment if collisions occur.
    # The first merge creates _nasdaq and _massive for colliding columns (like Name).
    # The second merge interacts with the result.

    def pick_str(*values) -> Any:
        for v in values:
            if v is not None and not pd.isna(v):
                s = str(v).strip()
                if s:
                    return s
        return None

    out = pd.DataFrame()
    out["Symbol"] = merged["Symbol"].astype(str).str.strip()

    # Name precedence: NASDAQ -> Massive -> Alpha Vantage    
    # Safe column extraction helper
    def get_col(df, col_base, suffix):
        if f"{col_base}{suffix}" in df.columns:
            return df[f"{col_base}{suffix}"]
        if col_base in df.columns and suffix == "": # Fallback/original
             return df[col_base]
        return None

    out["Name"] = merged.apply(
        lambda row: pick_str(
            row.get("Name_nasdaq"), 
            row.get("Name_massive"), 
            row.get("Name_av") if "Name_av" in row else row.get("Name")
        ), axis=1
    )

    # NASDAQ metadata (best effort).
    for col in ["Description", "Sector", "Industry", "Industry_2", "Optionable", "Country"]:
        if col in merged.columns:
            out[col] = merged[col]

    # Alpha Vantage metadata (best effort).
    for col in ["IpoDate", "DelistingDate", "Status"]:
         if col in merged.columns:
            out[col] = merged[col]

    # Massive/Shared metadata
    # Exchange, AssetType might exist in both Massive and AV.
    out["Exchange"] = merged.apply(lambda row: pick_str(row.get("Exchange_massive"), row.get("Exchange_av") if "Exchange_av" in row else row.get("Exchange")), axis=1)
    out["AssetType"] = merged.apply(lambda row: pick_str(row.get("AssetType_massive"), row.get("AssetType_av") if "AssetType_av" in row else row.get("AssetType")), axis=1)

    # Massive unique
    for col in ["Locale", "Market", "CurrencyName"]:
        if col in merged.columns:
             out[col] = merged[col]

    # Source tracking
    if "source_nasdaq" in merged.columns:
        out["source_nasdaq"] = merged["source_nasdaq"].fillna(False).astype(bool)
    else:
        out["source_nasdaq"] = False
        
    if "source_massive" in merged.columns:
         out["source_massive"] = merged["source_massive"].fillna(False).astype(bool)
    else:
        out["source_massive"] = False

    if "source_alpha_vantage" in merged.columns:
        out["source_alpha_vantage"] = merged["source_alpha_vantage"].fillna(False).astype(bool)
    else:
        out["source_alpha_vantage"] = False

    # Generate summary source column
    def _make_source_str(row):
        parts = []
        if row.get("source_nasdaq"): parts.append("nasdaq")
        if row.get("source_massive"): parts.append("massive")
        if row.get("source_alpha_vantage"): parts.append("alpha_vantage")
        return ",".join(parts)
    
    out["source"] = out.apply(_make_source_str, axis=1)

    out = out[out["Symbol"].ne("")].drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    return out


def _get_symbols_refresh_interval_hours() -> float:
    raw = os.environ.get("SYMBOLS_REFRESH_INTERVAL_HOURS", "24")
    try:
        value = float(str(raw).strip() or "24")
    except Exception:
        return 24.0
    if value < 0:
        return 24.0
    return value


def _ensure_symbols_tables(cur) -> None:
    """
    Ensure the symbols and sync metadata tables exist (best-effort).

    This keeps `get_symbols()` self-healing for fresh environments.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.symbols (
          symbol TEXT PRIMARY KEY
        );
        """
    )

    # Ensure expected columns exist even if the table pre-dates Alpha Vantage integration.
    columns: list[tuple[str, str]] = [
        ("name", "TEXT"),
        ("description", "TEXT"),
        ("sector", "TEXT"),
        ("industry", "TEXT"),
        ("industry_2", "TEXT"),
        ("optionable", "TEXT"),
        ("country", "TEXT"),
        ("exchange", "TEXT"),
        ("asset_type", "TEXT"),
        ("ipo_date", "TEXT"),
        ("delisting_date", "TEXT"),
        ("status", "TEXT"),
        ("source_nasdaq", "BOOLEAN"),
        ("source_alpha_vantage", "BOOLEAN"),
        ("source", "TEXT"),
        ("updated_at", "TIMESTAMPTZ NOT NULL DEFAULT now()"),
    ]
    for name, col_type in columns:
        cur.execute(f"ALTER TABLE public.symbols ADD COLUMN IF NOT EXISTS {name} {col_type};")

    # Ensure a unique index exists for upserts even if the table was created without a PK.
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS symbols_symbol_uidx ON public.symbols(symbol);")
    except Exception as exc:
        write_warning(f"Unable to ensure unique index for symbols.symbol. ({exc})")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS public.symbol_sync_state (
          id SMALLINT PRIMARY KEY,
          last_refreshed_at TIMESTAMPTZ,
          last_refreshed_sources JSONB,
          last_refresh_error TEXT
        );
        """
    )
    cur.execute("INSERT INTO public.symbol_sync_state(id) VALUES (1) ON CONFLICT DO NOTHING;")


def _symbols_refresh_due(cur, interval_hours: float) -> bool:
    # Always return True to allow refreshes whenever get_symbols() is called or job is triggered.
    return True


def _try_advisory_lock_symbols_refresh(cur) -> bool:
    try:
        cur.execute("SELECT pg_try_advisory_lock(%s, %s);", (11873, 42001))
        row = cur.fetchone()
        return bool(row and row[0])
    except Exception:
        return True  # best-effort: if locks unavailable, proceed.


def _unlock_symbols_refresh(cur) -> None:
    try:
        cur.execute("SELECT pg_advisory_unlock(%s, %s);", (11873, 42001))
    except Exception:
        pass


def upsert_symbols_to_db(
    df_symbols: pd.DataFrame, *, sources: Optional[dict[str, Any]] = None, cur: Any = None
) -> None:
    """
    Upsert symbol universe into Postgres, preserving existing values when new values are empty.
    """
    if df_symbols is None or df_symbols.empty:
        return

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn and cur is None:
        return

    # Map DF columns (TitleCase) to DB columns (lowercase)
    col_map = {
        "Symbol": "symbol",
        "Name": "name",
        "Description": "description",
        "Sector": "sector",
        "Industry": "industry",
        "Industry_2": "industry_2",
        "Optionable": "optionable",
        "Country": "country",
        "Exchange": "exchange",
        "AssetType": "asset_type",
        "IpoDate": "ipo_date",
        "DelistingDate": "delisting_date",
        "Status": "status",
        "source_nasdaq": "source_nasdaq",
        "source_alpha_vantage": "source_alpha_vantage",
        "source": "source",
    }

    df_to_upload = df_symbols.copy()
    if "Symbol" not in df_to_upload.columns:
        return

    # Normalize and keep only mapped columns.
    existing_cols = [c for c in col_map.keys() if c in df_to_upload.columns]
    df_to_upload = df_to_upload[existing_cols].copy()
    df_to_upload["Symbol"] = df_to_upload["Symbol"].astype(str).str.strip()
    df_to_upload = df_to_upload[df_to_upload["Symbol"].ne("")]
    df_to_upload = df_to_upload.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
    if df_to_upload.empty:
        return

    # Convert empty strings/NaNs to NULL to avoid wiping out existing values on upsert.
    for col in df_to_upload.columns:
        if col in {"source_nasdaq", "source_alpha_vantage", "source"}:
            continue
        df_to_upload[col] = df_to_upload[col].apply(lambda v: None if v is None or pd.isna(v) or str(v).strip() == "" else v)

    df_to_upload.rename(columns=col_map, inplace=True)
    db_cols = list(df_to_upload.columns)

    # Build DO UPDATE clause preserving existing values when EXCLUDED values are NULL/empty.
    update_cols = [c for c in db_cols if c != "symbol"]
    set_parts = []
    for col in update_cols:
        if col in {"source_nasdaq", "source_alpha_vantage", "source"}:
            set_parts.append(f"{col} = COALESCE(EXCLUDED.{col}, s.{col})")
        else:
            set_parts.append(f"{col} = COALESCE(EXCLUDED.{col}, s.{col})")
    set_parts.append("updated_at = now()")
    set_clause = ", ".join(set_parts)

    def _execute_upsert(target_cur) -> None:
        cols_sql = ", ".join(db_cols)
        placeholders = ", ".join(["%s"] * len(db_cols))
        insert_sql = (
            f"""
            INSERT INTO public.symbols AS s ({cols_sql})
            VALUES ({placeholders})
            ON CONFLICT (symbol) DO UPDATE SET {set_clause};
            """
        )
        target_cur.executemany(insert_sql, list(df_to_upload.itertuples(index=False, name=None)))

        if sources is not None:
            target_cur.execute(
                """
                INSERT INTO public.symbol_sync_state(id, last_refreshed_at, last_refreshed_sources, last_refresh_error)
                VALUES (1, now(), %s, NULL)
                ON CONFLICT (id) DO UPDATE
                SET last_refreshed_at = EXCLUDED.last_refreshed_at,
                    last_refreshed_sources = EXCLUDED.last_refreshed_sources,
                    last_refresh_error = NULL;
                """,
                (json.dumps(sources),),
            )

    if cur is not None:
        _execute_upsert(cur)
        return

    with connect(dsn) as conn:
        with conn.cursor() as target_cur:
            _execute_upsert(target_cur)


def refresh_symbols_to_db_if_due() -> None:
    """
    Periodically refresh the Postgres symbols table from NASDAQ + Alpha Vantage.

    This is invoked opportunistically by get_symbols() and uses an advisory lock
    to avoid redundant refreshes across concurrent jobs.
    """
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        return

    interval_hours = _get_symbols_refresh_interval_hours()
    if interval_hours <= 0:
        return

    with connect(dsn) as conn:
        with conn.cursor() as cur:
            if not _try_advisory_lock_symbols_refresh(cur):
                write_line("Symbols refresh already in progress; skipping.")
                return

            try:
                if not _symbols_refresh_due(cur, interval_hours):
                    return

                write_line("Refreshing symbols from NASDAQ + Alpha Vantage + Massive...")
                df_nasdaq = get_active_tickers()
                df_av = get_active_tickers_alpha_vantage()
                df_massive = get_active_tickers_massive()

                now_ts = datetime.now().isoformat()
                sources = {
                    "nasdaq": {
                        "rows": int(len(df_nasdaq)) if df_nasdaq is not None else 0,
                        "timestamp": now_ts
                    },
                    "alpha_vantage": {
                        "rows": int(len(df_av)) if df_av is not None else 0,
                        "timestamp": now_ts
                    },
                    "massive": {
                        "rows": int(len(df_massive)) if df_massive is not None else 0,
                        "timestamp": now_ts
                    },
                }

                df_merged = merge_symbol_sources(df_nasdaq, df_av, df_massive)

                # Mix in manual additions before persisting.
                tickers_to_add = cfg.TICKERS_TO_ADD
                for ticker_to_add in tickers_to_add:
                    symbol = str(ticker_to_add.get("Symbol") or "").strip()
                    if not symbol:
                        continue
                    if df_merged["Symbol"].eq(symbol).any():
                        continue
                    df_merged = pd.concat([df_merged, pd.DataFrame.from_dict([ticker_to_add])], ignore_index=True)

                df_merged = df_merged.drop_duplicates(subset=["Symbol"]).reset_index(drop=True)
                sources["merged"] = {"rows": int(len(df_merged))}

                if df_merged.empty:
                    write_warning("Symbols refresh produced empty symbol universe; skipping DB update.")
                    return

                upsert_symbols_to_db(df_merged, sources=sources, cur=cur)
                write_line(f"Symbols refresh complete. merged={len(df_merged)}")
            except Exception as exc:
                try:
                    cur.execute(
                        """
                        INSERT INTO public.symbol_sync_state(id, last_refresh_error)
                        VALUES (1, %s)
                        ON CONFLICT (id) DO UPDATE
                        SET last_refresh_error = EXCLUDED.last_refresh_error;
                        """,
                        (str(exc),),
                    )
                except Exception:
                    pass
                write_warning(f"Symbols refresh failed: {exc}")
            finally:
                _unlock_symbols_refresh(cur)

def get_symbols_from_db():
    try:
        dsn = os.environ.get("POSTGRES_DSN")
        if not dsn:
            logger.warning("POSTGRES_DSN not set. Skipping DB fetch.")
            return None
            
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM public.symbols")
                if cur.description is None:
                     return pd.DataFrame()
                columns = [desc[0] for desc in cur.description]
                data = cur.fetchall()
                
            if not data:
                return pd.DataFrame(columns=columns)
                
            df = pd.DataFrame(data, columns=columns)
            
            # Rename lowercase DB columns to TitleCase for app compatibility
            rename_map = {
                'symbol': 'Symbol',
                'name': 'Name',
                'description': 'Description',
                'sector': 'Sector',
                'industry': 'Industry', 
                'industry_2': 'Industry_2',
                'optionable': 'Optionable',
                'country': 'Country',
                'exchange': 'Exchange',
                'asset_type': 'AssetType',
                'ipo_date': 'IpoDate',
                'delisting_date': 'DelistingDate',
                'status': 'Status',
                'source_nasdaq': 'source_nasdaq',
                'source_alpha_vantage': 'source_alpha_vantage',
                'source': 'source',
                'updated_at': 'UpdatedAt',
            }
            df.rename(columns=rename_map, inplace=True)
            return df
            
    except Exception as e:
        logger.error(f"Error reading symbols from DB: {e}")
        return None


def sync_symbols_to_db(df_symbols: pd.DataFrame):
    """
    Writes symbols ensuring they are in the Postgres database.
    Filters out existing symbols before inserting.
    """
    if df_symbols is None or df_symbols.empty:
        return

    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        write_warning("POSTGRES_DSN not set. Cannot sync symbols to DB.")
        return

    # Map DF columns (TitleCase) to DB columns (lowercase)
    col_map = {
        'Symbol': 'symbol',
        'Name': 'name',
        'Description': 'description',
        'Sector': 'sector',
        'Industry': 'industry',
        'Industry_2': 'industry_2',
        'Optionable': 'optionable',
        'Country': 'country'
    }
    
    # Filter only relevant columns
    df_to_upload = df_symbols.copy()
    existing_cols = [c for c in col_map.keys() if c in df_to_upload.columns]
    df_to_upload = df_to_upload[existing_cols]
    df_to_upload.rename(columns=col_map, inplace=True)
    
    db_cols = list(df_to_upload.columns)
    
    try:
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                # 1. Fetch existing symbols to avoid duplicates
                cur.execute("SELECT symbol FROM public.symbols")
                existing_symbols = set(row[0] for row in cur.fetchall())
                
                # 2. Filter out existing
                df_new = df_to_upload[~df_to_upload['symbol'].isin(existing_symbols)]
                
                # drop duplicates in new set too
                df_new = df_new.drop_duplicates(subset=['symbol'])

                if df_new.empty:
                    write_line("All symbols already exist in DB. No new inserts.")
                    return

                write_line(f"Syncing {len(df_new)} new symbols to Postgres...")
                
                # 3. Insert using copy_rows
                copy_rows(
                    cur,
                    table="public.symbols",
                    columns=db_cols,
                    rows=df_new.itertuples(index=False, name=None)
                )
                
    except Exception as e:
        write_error(f"Error syncing symbols to DB: {e}")

def get_symbols():
    # Opportunistic periodic refresh from external sources.
    refresh_symbols_to_db_if_due()

    df_symbols = get_symbols_from_db()

    # Fallback/Supplemental Logic
    if df_symbols is None or df_symbols.empty:
        write_line("DB symbols missing or empty. Fetching from NASDAQ + Alpha Vantage + Massive...")
        df_nasdaq = get_active_tickers()
        df_av = get_active_tickers_alpha_vantage()
        df_massive = get_active_tickers_massive()
        df_symbols = merge_symbol_sources(df_nasdaq, df_av, df_massive)

        # Best effort: persist immediately if Postgres is configured.
        try:
            upsert_symbols_to_db(df_symbols, sources={"mode": "bootstrap"})
        except Exception:
            pass
    else:
        write_line(f"Loaded {len(df_symbols)} symbols from Postgres.")
        
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
            
    df_symbols.drop_duplicates(subset=['Symbol'], inplace=True)
    
    df_symbols.drop_duplicates(subset=['Symbol'], inplace=True)
    
    # Sync new symbols to DB (instead of CSV)
    sync_symbols_to_db(df_symbols)
    
    return df_symbols

def is_weekend(date):
    return date.weekday() >= 5

# ------------------------------------------------------------------------------
# Concurrency / Locking (Distributed Lock via Azure Blob Lease)
# ------------------------------------------------------------------------------

class JobLock:
    """
    Context manager for distributed locking using Azure Blob Storage Leases.

    By default, if the lock is already held, the job exits successfully (0) to avoid concurrent execution.
    Optionally, callers can wait for the lock by setting wait_timeout_seconds to a positive number or None
    (wait forever).
    """

    def __init__(
        self,
        job_name: str,
        lease_duration: int = 60,
        *,
        wait_timeout_seconds: Optional[float] = 0,
        poll_interval_seconds: float = 5.0,
    ):
        self.job_name = job_name
        self.lease_duration = lease_duration
        self.wait_timeout_seconds = wait_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.lock_blob_name = f"locks/{job_name}.lock"
        self.lease_client = None
        self.blob_client = None
        self._renew_stop = threading.Event()
        self._renew_thread: Optional[threading.Thread] = None

    def _renew_loop(self) -> None:
        interval = max(1, int(self.lease_duration * 0.5))
        while not self._renew_stop.wait(timeout=interval):
            if not self.lease_client:
                continue
            try:
                self.lease_client.renew()
                write_line(f"Lock renewed for {self.job_name}. Lease ID: {self.lease_client.id}")
            except Exception as exc:
                write_error(f"Lock renewal failed for {self.job_name}: {exc}")
                # Fail fast: if we can't renew, we may lose exclusivity and corrupt shared state.
                os._exit(1)

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

            # 3. Acquire Lease (optionally wait)
            start_wait: Optional[float] = None
            attempt = 0
            while True:
                attempt += 1
                try:
                    self.lease_client.acquire(lease_duration=self.lease_duration)
                    write_line(f"Lock acquired for {self.job_name}. Lease ID: {self.lease_client.id}")

                    # 4. Keep lease alive for long-running jobs
                    self._renew_stop.clear()
                    self._renew_thread = threading.Thread(
                        target=self._renew_loop,
                        name=f"job-lock-renew:{self.job_name}",
                        daemon=True,
                    )
                    self._renew_thread.start()
                    return self

                except (ResourceExistsError, HttpResponseError) as exc:
                    status_code = getattr(exc, "status_code", None) or getattr(
                        getattr(exc, "response", None), "status_code", None
                    )
                    if status_code != 409:
                        write_error(f"Failed to acquire lock for {self.job_name}: {exc}")
                        raise

                    # Locked
                    if self.wait_timeout_seconds == 0:
                        write_warning(f"Lock already held for {self.job_name}. Skipping execution.")
                        raise SystemExit(0)

                    now = time.monotonic()
                    if start_wait is None:
                        start_wait = now
                        write_line(f"Lock already held for {self.job_name}. Waiting for release...")
                    else:
                        elapsed = now - start_wait
                        if self.wait_timeout_seconds is not None and elapsed >= self.wait_timeout_seconds:
                            write_error(
                                f"Timed out waiting for lock {self.job_name} after {elapsed:.1f}s; exiting."
                            )
                            raise SystemExit(1)
                        # Emit a periodic heartbeat while waiting (roughly every minute).
                        if attempt % max(1, int(60 / max(0.1, self.poll_interval_seconds))) == 0:
                            write_line(f"Still waiting for lock {self.job_name} (elapsed={elapsed:.0f}s)...")

                    sleep_seconds = max(0.5, float(self.poll_interval_seconds))
                    # Add a small jitter to reduce herd effects if many jobs contend.
                    sleep_seconds += random.uniform(0, min(1.0, sleep_seconds * 0.2))
                    time.sleep(sleep_seconds)

        except Exception as e:
            write_error(f"Failed to acquire lock for {self.job_name}: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._renew_stop.set()
        if self._renew_thread and self._renew_thread.is_alive():
            self._renew_thread.join(timeout=2)
        if self.lease_client:
            try:
                write_line(f"Releasing lock for {self.job_name}...")
                self.lease_client.release()
            except Exception as e:
                write_error(f"Error releasing lock: {e}")

def read_raw_bytes(file_path: Union[str, Path], client: Optional[BlobStorageClient] = None) -> bytes:
    """
    Retrieves raw bytes from Azure Blob Storage.
    file_path: Remote path.
    client: Specific client to use.
    """
    if client:
        blob_name = get_remote_path(file_path)
        content_bytes = client.download_data(blob_name)
        if content_bytes:
            return content_bytes
    
    logger.warning(f"Failed to load bytes from {file_path} (client={client is not None}).")
    return b""

def store_raw_bytes(
    data: bytes, 
    file_path: Union[str, Path], 
    client: Optional[BlobStorageClient] = None,
    overwrite: bool = True
) -> str:
    """
    Stores raw bytes to Azure Blob Storage.
    file_path: Remote path.
    client: Specific client to use.
    """
    remote_path = get_remote_path(file_path)

    if client is None:
        raise RuntimeError("Azure Storage Client not provided. Cannot store raw bytes.")

    client.upload_data(remote_path, data, overwrite=overwrite)
    return remote_path


def get_symbol_sync_state(dsn: str) -> Optional[dict]:
    """Retrieves the current symbol synchronization state from the database."""
    with connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, last_refreshed_at, last_refreshed_sources, last_refresh_error FROM public.symbol_sync_state WHERE id=1;")
            row = cur.fetchone()
            if row:
                return {
                    "id": row[0],
                    "last_refreshed_at": row[1],
                    "last_refreshed_sources": row[2],
                    "last_refresh_error": row[3],
                }
    return None
