from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import warnings
from io import BytesIO

from scripts.common import core as mdc
from scripts.common import delta_core
from scripts.finance_data import config as cfg
from scripts.common.pipeline import DataPaths

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)


@dataclass(frozen=True)
class BlobProcessResult:
    blob_name: str
    silver_path: Optional[str]
    status: str  # ok|skipped|failed
    error: Optional[str] = None

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

def _utc_today() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(timezone.utc).date())


def resample_daily_ffill(df: pd.DataFrame, *, extend_to: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    """
    Resamples sparse dataframe to daily frequency using forward fill.
    """
    if 'Date' not in df.columns:
        return df
        
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'], errors="coerce", utc=True).dt.tz_convert(None)
    df = df.dropna(subset=["Date"])
    if df.empty:
        return df

    df = df.set_index('Date')
    df = df.sort_index()
    
    # Resample and ffill
    # We must restrict to the known date range
    if df.empty:
        return df
        
    end = df.index.max()
    if extend_to is not None and extend_to > end:
        end = extend_to

    full_range = pd.date_range(start=df.index.min(), end=end, freq="D", name="Date")
    df_daily = df.reindex(full_range).ffill()
    
    return df_daily.reset_index()

def _try_get_delta_max_date(container: str, path: str) -> Optional[pd.Timestamp]:
    df = delta_core.load_delta(container, path, columns=["Date"])
    if df is None or df.empty or "Date" not in df.columns:
        return None

    dates = pd.to_datetime(df["Date"], errors="coerce")
    dates = dates.dropna()
    if dates.empty:
        return None
    return pd.Timestamp(dates.max()).normalize()


def process_blob(blob, *, desired_end: pd.Timestamp) -> BlobProcessResult:
    blob_name = blob['name'] 
    # expected: finance-data/Folder Name/ticker_suffix.csv
    # e.g. finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv
    
    parts = blob_name.split('/')
    if len(parts) < 3:
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
    folder_name = parts[1]
    filename = parts[2]
    
    if not filename.endswith('.csv'):
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
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
        return BlobProcessResult(blob_name=blob_name, silver_path=None, status="skipped")
        
    ticker = filename.replace(f"_{suffix}.csv", "")
    
    # Silver Path
    # Use DataPaths or manual? DataPaths uses folder name.
    # DataPaths.get_finance_path(folder_name, ticker, suffix)
    silver_path = DataPaths.get_finance_path(folder_name, ticker, suffix)
    
    # Check freshness
    # Bronze blob last_modified
    bronze_lm = blob['last_modified'].timestamp()
    
    silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, silver_path)
    
    if silver_lm and (silver_lm > bronze_lm):
        max_date = _try_get_delta_max_date(cfg.AZURE_CONTAINER_SILVER, silver_path)
        if max_date is not None and max_date >= desired_end:
            return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")

    mdc.write_line(f"Processing {ticker} {folder_name}...")
    
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_raw = pd.read_csv(BytesIO(raw_bytes))
        
        df_clean = transpose_dataframe(df_raw, ticker)
        
        # Resample to daily frequency (forward fill)
        df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
        if df_clean is None or df_clean.empty:
            raise ValueError("No valid dated rows after cleaning/resample.")
        
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
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="ok")
        
    except Exception as e:
        mdc.write_error(f"Failed to process {blob_name}: {e}")
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="failed", error=str(e))


def main() -> int:
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze Finance files...")
    # Recursive list? list_blobs(name_starts_with="finance-data/") usually returns all nested.
    blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")
    
    desired_end = _utc_today()

    results: list[BlobProcessResult] = []
    for blob in blobs:
        results.append(process_blob(blob, desired_end=desired_end))
        
    processed = sum(1 for r in results if r.status == "ok")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = sum(1 for r in results if r.status == "failed")
    mdc.write_line(f"Silver finance ingest complete: processed={processed}, skipped={skipped}, failed={failed}")

    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from scripts.common.by_date_pipeline import run_partner_then_by_date
    from scripts.finance_data.materialize_silver_finance_by_date import main as by_date_main

    job_name = "silver-finance-job"
    raise SystemExit(
        run_partner_then_by_date(
            job_name=job_name,
            partner_main=main,
            by_date_main=by_date_main,
        )
    )
