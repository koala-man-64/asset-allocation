import os
import logging
from typing import Optional, Dict, Any, Union
from pathlib import Path
import pandas as pd
from deltalake import DeltaTable, write_deltalake

# Configure logger
logger = logging.getLogger(__name__)

def _parse_connection_string(conn_str: str) -> Dict[str, str]:
    """Parses Azure Storage Connection String into a dictionary."""
    return dict(item.split('=', 1) for item in conn_str.split(';') if '=' in item)

def get_delta_storage_options() -> Dict[str, str]:
    """
    Constructs the storage_options dictionary required by deltalake (delta-rs)
    for Azure Blob Storage authentication.
    
    Prioritizes Account Key if available, otherwise attempts to configure for
    Managed Identity (via different provider configs if supported) or SAS.
    
    Note: delta-rs support for Azure Managed Identity can be complex.
    For now, we support:
    1. Account Key (AZURE_STORAGE_ACCOUNT_KEY or parsed from Connection String)
    2. Connection String (not directly supported by simple options, usually parsed)
    3. SAS Token (AZURE_STORAGE_SAS_TOKEN)
    4. Azure CLI/Identity fallback (azure_use_azure_cli='true')
    """
    options = {}
    
    # 0. Helper: Parse Connection String if present
    cs_map = {}
    conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if conn_str:
        cs_map = _parse_connection_string(conn_str)
    
    # Account Name is mandatory
    account_name = os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
    if not account_name:
        account_name = cs_map.get('AccountName')
        
    if account_name:
        options['account_name'] = account_name

    # 1. Account Key
    account_key = os.environ.get('AZURE_STORAGE_ACCOUNT_KEY') or os.environ.get('AZURE_STORAGE_ACCESS_KEY')
    if not account_key:
        account_key = cs_map.get('AccountKey')

    if account_key:
        options['account_key'] = account_key
        return options

    # 2. SAS Token
    sas_token = os.environ.get('AZURE_STORAGE_SAS_TOKEN')
    if sas_token:
        options['sas_token'] = sas_token
        return options

    # 3. Client Secret (Service Principal)
    client_id = os.environ.get('AZURE_CLIENT_ID')
    client_secret = os.environ.get('AZURE_CLIENT_SECRET')
    tenant_id = os.environ.get('AZURE_TENANT_ID')
    
    if client_id and client_secret and tenant_id:
        options['client_id'] = client_id
        options['client_secret'] = client_secret
        options['tenant_id'] = tenant_id
        return options

    # 4. Managed Identity / Azure CLI (Fallback)
    # If we are in an Azure environment (Container Apps, App Service, VM), IDENTITY_ENDPOINT is usually set.
    # In that case, we should NOT force use_azure_cli, as the underlying library (object_store/azure-identity)
    # should automatically detect Managed Identity.
    # We only default to Azure CLI if we are NOT in a known MSI environment.
    if os.environ.get('IDENTITY_ENDPOINT') or os.environ.get('MSI_ENDPOINT'):
        logger.info("Detected Managed Identity environment (IDENTITY_ENDPOINT/MSI_ENDPOINT). Skipping 'use_azure_cli'.")
        # Do not set 'use_azure_cli' to true, relying on default chain/MSI.
    else:
        # Local development fallback: try Azure CLI
        options['use_azure_cli'] = 'true'
    
    return options

def get_delta_table_uri(container: str, path: str, account_name: Optional[str] = None) -> str:
    """
    Returns the full abfss:// URI for a Delta table.
    """
    acc = account_name or os.environ.get('AZURE_STORAGE_ACCOUNT_NAME')
    if not acc:
         # Try logic from parsing connection string if environment variable is missing
         # Re-use parsing logic (inefficient to do twice but safe)
         conn_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
         if conn_str:
             cs_map = dict(item.split('=', 1) for item in conn_str.split(';') if '=' in item)
             acc = cs_map.get('AccountName')

    if not acc:
         raise ValueError("AZURE_STORAGE_ACCOUNT_NAME must be set (or parseable from AZURE_STORAGE_CONNECTION_STRING) to construct Delta URI.")
         
    # Clean path
    path = path.strip('/')
    
    # Format: abfss://<container>@<account>.dfs.core.windows.net/<path>
    # Note: simple w/o dfs sometimes works for blob, but abfss is standard for Data Lake / Delta
    # using object_store.
    return f"abfss://{container}@{acc}.dfs.core.windows.net/{path}"

def store_delta(
    df: pd.DataFrame, 
    container: str, 
    path: str, 
    mode: str = 'overwrite', 
    partition_by: list = None,
    merge_schema: bool = False
) -> None:
    """
    Writes a pandas DataFrame to a Delta table in Azure.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options()
        
        write_deltalake(
            uri,
            df,
            mode=mode,
            partition_by=partition_by,
            schema_mode="merge" if merge_schema else None,
            storage_options=opts
        )
        logger.info(f"Successfully wrote Delta table to {path}")
    except Exception as e:
        logger.error(f"Failed to write Delta table {path}: {e}")
        raise

def load_delta(container: str, path: str, version: int = None) -> Optional[pd.DataFrame]:
    """
    Reads a Delta table from Azure into a pandas DataFrame.
    Returns None if table does not exist or access fails.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options()
        
        dt = DeltaTable(uri, version=version, storage_options=opts)
        return dt.to_pandas()
    except Exception as e:
        # Check for specific "Not a Delta table" or "Not found" errors if possible
        # For now, log warning and return None to mimic load_parquet behavior
        logger.warning(f"Failed to load Delta table {path}: {e}")
        return None

def get_delta_last_commit(container: str, path: str) -> Optional[float]:
    """
    Returns the timestamp of the last commit to the Delta table.
    Useful for freshness checks.
    """
    try:
        uri = get_delta_table_uri(container, path)
        opts = get_delta_storage_options()
        
        dt = DeltaTable(uri, storage_options=opts)
        # history() returns a list of dictionaries. 0-th index is usually latest? 
        # Actually dt.history() in python returns a list of dicts, most recent first usually?
        # Let's check metadata directly or history.
        # dt.metadata().created_time is creation.
        # let's use history(1)
        hist = dt.history(1)
        if hist:
            # timestamp is usually in milliseconds or microseconds?
            # dictionary keys: timestamp, operation, etc.
            # timestamp is int (ms since epoch)
            ts = hist[0].get('timestamp')
            if ts:
                return ts / 1000.0 # Convert to seconds for standard unix time
        return None
    except Exception as e:
        logger.warning(f"Failed to get Delta history for {path}: {e}")
        return None
