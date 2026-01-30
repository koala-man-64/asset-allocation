
import os
import warnings
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from io import BytesIO

from core import core as mdc
from core import delta_core
from tasks.price_target_data import config as cfg
from core.pipeline import DataPaths

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

def process_blob(blob):
    blob_name = blob['name'] # price-target-data/{symbol}.parquet
    if not blob_name.endswith('.parquet'):
        return True
        
    ticker = blob_name.replace('price-target-data/', '').replace('.parquet', '')
    
    # Silver Path
    silver_path = DataPaths.get_price_target_path(ticker)
    
    # Freshness Check
    bronze_lm = blob['last_modified'].timestamp()
    silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, silver_path)
    
    if silver_lm and (silver_lm > bronze_lm):
        return True

    mdc.write_line(f"Processing {ticker}...")
    
    try:
        # Read Bronze
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_parquet(BytesIO(raw_bytes))
        
        column_names = [
            "symbol", "obs_date", "tp_mean_est", "tp_std_dev_est", 
            "tp_high_est", "tp_low_est", "tp_cnt_est", 
            "tp_cnt_est_rev_up", "tp_cnt_est_rev_down"
        ]
        
        # Transform
        if df_new.empty:
            return

        df_new['obs_date'] = pd.to_datetime(df_new['obs_date'])
        df_new = df_new.sort_values(by='obs_date')
        
        # Carry Forward / Upsample
        today = pd.to_datetime("today").normalize()
        if not df_new.empty:
             latest_obs = df_new['obs_date'].max()
             if latest_obs < today:
                 # Extend date range
                 all_dates = pd.date_range(start=df_new['obs_date'].min(), end=today)
                 df_dates = pd.DataFrame({'obs_date': all_dates})
                 df_new = df_dates.merge(df_new, on='obs_date', how='left')
                 df_new = df_new.ffill()

        df_new['symbol'] = ticker
        
        for col in column_names:
            if col not in df_new.columns:
                df_new[col] = np.nan
        df_new = df_new[column_names]

        # Resample Daily (Full Range)
        df_new = df_new.set_index('obs_date')
        df_new = df_new[~df_new.index.duplicated(keep='last')]
        
        full_range = pd.date_range(start=df_new.index.min(), end=df_new.index.max(), freq='D')
        df_new = df_new.reindex(full_range)
        df_new.ffill(inplace=True)
        df_new = df_new.reset_index().rename(columns={'index': 'obs_date'})
        df_new['symbol'] = ticker

        # Load Existing Silver
        df_history = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path)
        
        # Merge
        if df_history is None or df_history.empty:
            df_merged = df_new
        else:
             if 'obs_date' in df_history.columns:
                 df_history['obs_date'] = pd.to_datetime(df_history['obs_date'])
             df_merged = pd.concat([df_history, df_new], ignore_index=True)
             
        df_merged = df_merged.drop_duplicates(subset=['obs_date', 'symbol'], keep='last')
        df_merged = df_merged.sort_values(by=['obs_date', 'symbol'])
        
        # Write
        delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, silver_path)
        mdc.write_line(f"Updated Silver {ticker}")
        return True
    except Exception as e:
        mdc.write_error(f"Failed to process {ticker}: {e}")
        return False

def main():
    mdc.log_environment_diagnostics()
    mdc.write_line("Listing Bronze Price Target files...")
    blobs = bronze_client.list_blob_infos(name_starts_with="price-target-data/")
    
    blob_list = list(blobs)
    mdc.write_line(f"Found {len(blob_list)} blobs. Processing...")
    
    ok_or_skipped = 0
    failed = 0
    for blob in blob_list:
        if process_blob(blob):
            ok_or_skipped += 1
        else:
            failed += 1

    mdc.write_line(f"Silver price target job complete: ok_or_skipped={ok_or_skipped} failed={failed}")
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.price_target_data.materialize_silver_price_target_by_date import main as by_date_main
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "silver-price-target-job"
    exit_code = run_partner_then_by_date(
        job_name=job_name,
        partner_main=main,
        by_date_main=by_date_main,
    )
    if exit_code == 0:
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
