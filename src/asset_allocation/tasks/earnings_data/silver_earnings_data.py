
import os
import asyncio
import warnings
import pandas as pd
import numpy as np
from datetime import datetime

from asset_allocation.core import core as mdc
from asset_allocation.tasks.earnings_data import config as cfg
from asset_allocation.core import delta_core
from asset_allocation.core.pipeline import DataPaths

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

def process_file(blob_name):
    # Expecting earnings-data/{symbol}.json
    ticker = blob_name.replace('earnings-data/', '').replace('.json', '')
    mdc.write_line(f"Processing {ticker} from {blob_name}...")
    
    # 1. Read Raw from Bronze
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        from io import BytesIO
        # JSON
        df_new = pd.read_json(BytesIO(raw_bytes), orient='records')
    except Exception as e:
        mdc.write_error(f"Failed to read/parse {blob_name}: {e}")
        return False

    # 2. Clean/Normalize
    df_new = df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in col], errors='ignore')
    if 'Date' in df_new.columns:
        df_new['Date'] = pd.to_datetime(df_new['Date'], errors='coerce')
    
    df_new['Symbol'] = ticker
    
    # 3. Load Existing Silver (History)
    cloud_path = DataPaths.get_earnings_path(ticker)
    df_history = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, cloud_path)
    
    # 4. Merge
    # Earnings data is tricky. We often want to UPDATE fields (Surprise) for past dates.
    # Simple strategy: Concat -> Drop Duplicates on [Symbol, Date] -> Keep Last (Latest Snapshot version of that date).
    
    if df_history is None or df_history.empty:
        df_merged = df_new
    else:
        if 'Date' in df_history.columns:
             df_history['Date'] = pd.to_datetime(df_history['Date'], errors='coerce')
        
        df_merged = pd.concat([df_history, df_new], ignore_index=True)
    
    # Sort
    df_merged = df_merged.sort_values(by=['Date'], ascending=True)
    
    # Dedup: Keep LAST (Newest information for that Earnings Date)
    df_merged = df_merged.drop_duplicates(subset=['Date', 'Symbol'], keep='last')
    
    # 5. Write to Silver
    try:
        delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, cloud_path)
    except Exception as e:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {e}")
        return False

    mdc.write_line(f"Updated Silver Delta for {ticker} (Total rows: {len(df_merged)})")
    return True

def main():
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze files...")
    blobs = bronze_client.list_files(name_starts_with="earnings-data/")
    blob_list = [b for b in blobs if b.endswith('.json')]
    
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Filtering for {cfg.DEBUG_SYMBOLS}")
        blob_list = [b for b in blob_list if any(s in b for s in cfg.DEBUG_SYMBOLS)]

    mdc.write_line(f"Found {len(blob_list)} files to process.")
    
    processed = 0
    failed = 0
    for blob_name in blob_list:
        if process_file(blob_name):
            processed += 1
        else:
            failed += 1

    mdc.write_line(f"Silver earnings job complete: processed={processed} failed={failed}")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from asset_allocation.core.by_date_pipeline import run_partner_then_by_date
    from asset_allocation.tasks.earnings_data.materialize_silver_earnings_by_date import main as by_date_main

    job_name = "silver-earnings-job"
    raise SystemExit(
        run_partner_then_by_date(
            job_name=job_name,
            partner_main=main,
            by_date_main=by_date_main,
        )
    )
