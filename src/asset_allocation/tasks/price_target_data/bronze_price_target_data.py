
import os
import asyncio
import warnings
import pandas as pd
import nasdaqdatalink
from datetime import datetime, date, timezone
from pathlib import Path
from typing import List

from asset_allocation.core import core as mdc
from asset_allocation.tasks.price_target_data import config as cfg
from asset_allocation.core.pipeline import ListManager

warnings.filterwarnings('ignore')

# Initialize Client
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
list_manager = ListManager(bronze_client, "price-target-data")

BATCH_SIZE = 50

def _validate_environment() -> None:
    required = ["AZURE_CONTAINER_BRONZE", "NASDAQ_API_KEY"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Missing env vars: " + " ".join(missing))
    
    nasdaqdatalink.ApiConfig.api_key = os.environ.get('NASDAQ_API_KEY')

async def process_batch_bronze(symbols: List[str], semaphore: asyncio.Semaphore):
    async with semaphore:
        # Check freshness of Bronze Blobs?
        # For simplicity, we can fetch all or strictly stale.
        # Let's check which symbols *need* update based on Bronze Blob age.
        
        stale_symbols = []
        for sym in symbols:
            blob_path = f"price-target-data/{sym}.parquet"
            # simple check: if blob exists and < 7 days old, skip?
            # Price targets update daily/weekly. 
            # API call is cheap (Nasdaq Data Link is specific).
            # Let's enforce 24h freshness for Bronze.
            try:
                blob = bronze_client.get_blob_client(blob_path)
                if blob.exists():
                     props = blob.get_blob_properties()
                     age = datetime.now(timezone.utc) - props.last_modified
                     if age.total_seconds() < 24 * 3600:
                         continue
            except Exception:
                pass
            stale_symbols.append(sym)
            
        if not stale_symbols:
            return

        min_date = date(2020, 1, 1)        
        
        loop = asyncio.get_event_loop()
        def fetch_api():
            try:
                tickers_str = ",".join(stale_symbols)
                return nasdaqdatalink.get_table(
                    'ZACKS/TP',
                    ticker=tickers_str,
                    obs_date={'gte': min_date.strftime('%Y-%m-%d')}
                )
            except Exception as e:
                mdc.write_error(f"API Batch Error: {e}")
                return pd.DataFrame()

        mdc.write_line(f"Fetching {len(stale_symbols)} symbols from Nasdaq...")
        batch_df = await loop.run_in_executor(None, fetch_api)
        
        if not batch_df.empty:
            for symbol, group_df in batch_df.groupby('ticker'):
                symbol = str(symbol)
                try:
                    raw_parquet = group_df.to_parquet(index=False)
                    mdc.store_raw_bytes(raw_parquet, f"price-target-data/{symbol}.parquet", client=bronze_client)
                    mdc.write_line(f"Saved Bronze {symbol}")
                    list_manager.add_to_whitelist(symbol)
                except Exception as e:
                    mdc.write_error(f"Failed to save {symbol}: {e}")
                    
            # Check for missing
            found_tickers = set(str(t) for t in batch_df['ticker'].unique())
            for sym in stale_symbols:
                if sym not in found_tickers:
                     # Likely no data exists or invalid
                     mdc.write_line(f"No data for {sym}, blacklisting.")
                     list_manager.add_to_blacklist(sym)

async def main_async():
    mdc.log_environment_diagnostics()
    _validate_environment()
    
    list_manager.load()
    
    df_symbols = mdc.get_symbols()
    symbols = [
        row['Symbol']
        for _, row in df_symbols.iterrows()
        if not list_manager.is_blacklisted(row['Symbol'])
    ]

    if cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Restricting to {len(cfg.DEBUG_SYMBOLS)} symbols")
        symbols = [s for s in symbols if s in cfg.DEBUG_SYMBOLS]
        
    chunked_symbols = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    semaphore = asyncio.Semaphore(3)
    
    mdc.write_line(f"Starting Bronze Price Target Ingestion for {len(symbols)} symbols...")
    tasks = [process_batch_bronze(chunk, semaphore) for chunk in chunked_symbols]
    await asyncio.gather(*tasks)
    mdc.write_line("Bronze Ingestion Complete.")

if __name__ == "__main__":
    job_name = 'bronze-price-target-job'
    with mdc.JobLock(job_name):
        asyncio.run(main_async())
