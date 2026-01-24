
import os
import asyncio
import pandas as pd
from datetime import datetime
import warnings

from asset_allocation.tasks.market_data import config as cfg
from asset_allocation.core import core as mdc
from asset_allocation.core import delta_core
from asset_allocation.core.pipeline import DataPaths

# Suppress warnings
warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

def process_file(blob_name):
    ticker = blob_name.replace('market-data/', '').replace('.csv', '')
    mdc.write_line(f"Processing {ticker} from {blob_name}...")
    
    # 1. Read Raw from Bronze
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        from io import BytesIO
        df_new = pd.read_csv(BytesIO(raw_bytes))
    except Exception as e:
        mdc.write_error(f"Failed to read/parse {blob_name}: {e}")
        return False

    # 2. Clean/Normalize
    if "Adj Close" in df_new.columns:
        df_new = df_new.drop('Adj Close', axis=1)
    
    if 'Date' in df_new.columns:
        df_new['Date'] = pd.to_datetime(df_new['Date'])
    
    df_new['Symbol'] = ticker
    
    # 3. Load Existing Silver (History)
    ticker_file_path = DataPaths.get_market_data_path(ticker.replace('.', '-'))
    df_history = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, ticker_file_path)
    
    # 4. Merge
    if df_history is None or df_history.empty:
        df_merged = df_new
    else:
        # Ensure types match before concat
        if 'Date' in df_history.columns:
             df_history['Date'] = pd.to_datetime(df_history['Date'])
        df_merged = pd.concat([df_history, df_new], ignore_index=True)
    
    # 5. Deduplicate and Sort
    df_merged = df_merged.sort_values(by=['Date', 'Symbol', 'Volume'], ascending=[True, True, False])
    df_merged = df_merged.drop_duplicates(subset=['Date', 'Symbol'], keep='first')
    
    # 6. Type Casting & Formatting
    df_merged = df_merged.astype({
        'Open': float, 'High': float, 'Low': float, 'Close': float, 'Volume': float
    })
    
    cols_to_round = ['Open', 'High', 'Low', 'Close']
    for col in cols_to_round:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].round(2)

    cols_to_drop = ['index', 'Beta (5Y Monthly)', 'PE Ratio (TTM)', '1y Target Est', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Market Cap']
    df_merged = df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns])

    # 7. Write to Silver
    try:
        delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, ticker_file_path)
    except Exception as e:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {e}")
        return False

    mdc.write_line(f"Updated Silver Delta for {ticker} (Total rows: {len(df_merged)})")
    return True

def main():
    mdc.log_environment_diagnostics()
    
    # List all files in Bronze market-data folder
    # Assuming mdc has a list_blobs or similar, otherwise use client directly
    # mdc.list_blobs is not explicitly shown in context, using client.
    # azure.storage.blob.ContainerClient.list_blobs
    
    mdc.write_line("Listing Bronze files...")
    blobs = bronze_client.list_files(name_starts_with="market-data/")
    
    # Convert to list to enable progress tracking/filtering
    blob_list = [b for b in blobs if b.endswith('.csv')]
    
    # Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Filtering for {cfg.DEBUG_SYMBOLS}")
        blob_list = [b for b in blob_list if any(s in b for s in cfg.DEBUG_SYMBOLS)]

    mdc.write_line(f"Found {len(blob_list)} files to process.")

    processed = 0
    failed = 0
    for blob_name in blob_list:
        if process_file(blob_name):
            processed += 1
        else:
            failed += 1

    mdc.write_line(f"Silver market job complete: processed={processed} failed={failed}")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from asset_allocation.core.by_date_pipeline import run_partner_then_by_date
    from asset_allocation.tasks.market_data.materialize_silver_market_by_date import main as by_date_main

    job_name = "silver-market-job"
    raise SystemExit(
        run_partner_then_by_date(
            job_name=job_name,
            partner_main=main,
            by_date_main=by_date_main,
        )
    )
