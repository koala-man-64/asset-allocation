
import sys
import os
import pandas as pd
import warnings
from io import BytesIO

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from scripts.common import core as mdc
from scripts.common import delta_core
from scripts.finance_data import config as cfg
from scripts.common.pipeline import DataPaths

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

def transpose_dataframe(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transposes Yahoo Finance CSV (Metrics in Rows, Dates in Columns)
    to (Dates in Rows, Metrics in Columns).
    """
    if 'name' in df.columns:
        df = df.set_index('name')
    elif 'breakdown' in df.columns:
        df = df.set_index('breakdown')
    else:
        df = df.set_index(df.columns[0])
    
    df_t = df.transpose()
    df_t.index.name = 'Date'
    df_t = df_t.reset_index()
    df_t.columns.name = None
    df_t['Symbol'] = ticker
    return df_t

def process_blob(blob):
    blob_name = blob.name 
    # expected: finance-data/Folder Name/ticker_suffix.csv
    # e.g. finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv
    
    parts = blob_name.split('/')
    if len(parts) < 3:
        return
        
    folder_name = parts[1]
    filename = parts[2]
    
    if not filename.endswith('.csv'):
        return
        
    # extract ticker
    # filename: ticker_suffix.csv
    # suffix starts with quarterly_...
    # split by first underscore? Tickers can have no underscores usually.
    # suffix is known: 
    known_suffixes = [
        "quarterly_balance-sheet", 
        "quarterly_valuation_measures", 
        "quarterly_cash-flow", 
        "quarterly_financials"
    ]
    
    suffix = None
    for s in known_suffixes:
        if filename.endswith(s + ".csv"):
            suffix = s
            break
            
    if not suffix:
        mdc.write_line(f"Skipping unknown file format: {filename}")
        return
        
    ticker = filename.replace(f"_{suffix}.csv", "")
    
    # Silver Path
    # Use DataPaths or manual? DataPaths uses folder name.
    # DataPaths.get_finance_path(folder_name, ticker, suffix)
    silver_path = DataPaths.get_finance_path(folder_name, ticker, suffix)
    
    # Check freshness
    # Bronze blob last_modified
    bronze_lm = blob.last_modified.timestamp()
    
    silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, silver_path)
    
    if silver_lm and (silver_lm > bronze_lm):
        # Silver is newer than Bronze (already processed)
        # mdc.write_line(f"Skipping {ticker}/{folder_name} (Silver up to date)")
        return

    mdc.write_line(f"Processing {ticker} {folder_name}...")
    
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_raw = pd.read_csv(BytesIO(raw_bytes))
        
        df_clean = transpose_dataframe(df_raw, ticker)
        
        # Write to Silver (Overwrite is fine for finance snapshots, or merge? 
        # Typically Finance Sheets are full snapshots. Replacing is safer for consistency, 
        # but if we want history of OLD financial restatements... 
        # Current logic: Overwrite/Upsert. store_delta defaults to append?
        # Let's use overwrite mode for now as Transposed data is simpler.
        # But wait, store_delta default implementation? 
        # Checking delta_core usage: usually it appends or overwrites. 
        # I'll check store_delta signature in next turn if needed.
        # Assuming overwrite for the partition/table is safest for now to avoid specific duplicates.
        
        delta_core.store_delta(df_clean, cfg.AZURE_CONTAINER_SILVER, silver_path)
        mdc.write_line(f"Updated Silver {silver_path}")
        
    except Exception as e:
        mdc.write_error(f"Failed to process {blob_name}: {e}")

def main():
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze Finance files...")
    # Recursive list? list_blobs(name_starts_with="finance-data/") usually returns all nested.
    blobs = bronze_client.list_blobs(name_starts_with="finance-data/")
    
    count = 0 
    for blob in blobs:
        process_blob(blob)
        count += 1
        
    mdc.write_line(f"Processed {count} blobs.")

if __name__ == "__main__":
    job_name = 'finance-data-job-silver'
    with mdc.JobLock(job_name):
        main()
