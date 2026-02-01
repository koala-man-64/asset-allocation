from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import warnings
from io import BytesIO
import re

from core import core as mdc
from core import delta_core
from tasks.finance_data import config as cfg
from core.pipeline import DataPaths
from tasks.common.watermarks import check_blob_unchanged, load_watermarks, save_watermarks

warnings.filterwarnings('ignore')

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)


@dataclass(frozen=True)
class BlobProcessResult:
    blob_name: str
    silver_path: Optional[str]
    status: str  # ok|skipped|failed
    error: Optional[str] = None


def _read_finance_csv(raw_bytes: bytes) -> pd.DataFrame:
    """
    Read finance CSVs defensively.

    These Yahoo-derived CSVs commonly include thousands separators and sparse cells.
    Reading everything as strings (and disabling default NA parsing) avoids mixed
    object columns like ["1,234", NaN] that later break Arrow/Delta writes.
    """
    return pd.read_csv(BytesIO(raw_bytes), dtype=str, keep_default_na=False)

def transpose_dataframe(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Transposes Yahoo Finance CSV (Metrics in Rows, Dates in Columns)
    to (Dates in Rows, Metrics in Columns).
    """
    if 'name' in df.columns:
        df['name'] = df['name'].astype(str).str.strip()
    elif 'breakdown' in df.columns:
        df['breakdown'] = df['breakdown'].astype(str).str.strip()

    df = _rename_ttm_columns(df)

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


def _infer_date_format(columns: list[str]) -> str:
    for raw in columns:
        value = str(raw).strip()
        if not value:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return "%Y-%m-%d"
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", value):
            return "%m/%d/%Y"
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{2}", value):
            return "%m/%d/%y"
        if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", value):
            return "%Y/%m/%d"
    return "%m/%d/%Y"


def _rename_ttm_columns(df: pd.DataFrame) -> pd.DataFrame:
    ttm_cols = [col for col in df.columns if str(col).strip().lower() == "ttm"]
    if not ttm_cols:
        return df

    date_candidates = [
        col
        for col in df.columns
        if col not in ttm_cols and str(col).strip().lower() not in {"name", "breakdown"}
    ]
    date_format = _infer_date_format([str(c) for c in date_candidates])
    today_str = datetime.now(timezone.utc).date().strftime(date_format)

    out = df.copy()
    for col in ttm_cols:
        if today_str in out.columns:
            out = out.drop(columns=[col])
        else:
            out = out.rename(columns={col: today_str})
    return out

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


def _align_to_existing_schema(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
    existing_cols = delta_core.get_delta_schema_columns(container, path)
    if not existing_cols:
        return df.reset_index(drop=True)

    out = df.copy()
    for col in existing_cols:
        if col not in out.columns:
            out[col] = pd.NA

    ordered_cols = list(existing_cols) + [col for col in out.columns if col not in existing_cols]
    out = out[ordered_cols]
    return out.reset_index(drop=True)


def process_blob(blob, *, desired_end: pd.Timestamp, watermarks: dict | None = None) -> BlobProcessResult:
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

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS and ticker not in cfg.DEBUG_SYMBOLS:
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")
    
    if watermarks is not None:
        unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
        if unchanged:
            return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")
    else:
        signature = {}
        bronze_lm = blob.get("last_modified")
        if bronze_lm is not None:
            bronze_ts = bronze_lm.timestamp()
            silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, silver_path)
            if silver_lm and (silver_lm > bronze_ts):
                max_date = _try_get_delta_max_date(cfg.AZURE_CONTAINER_SILVER, silver_path)
                if max_date is not None and max_date >= desired_end:
                    return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")

    mdc.write_line(f"Processing {ticker} {folder_name}...")
    
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_raw = _read_finance_csv(raw_bytes)

        # Header-only or otherwise empty inputs occasionally appear in Bronze.
        if df_raw is None or df_raw.empty or len(df_raw.columns) <= 1:
            mdc.write_warning(f"Skipping empty finance CSV: {blob_name}")
            return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="skipped")
        
        df_clean = transpose_dataframe(df_raw, ticker)
        
        # Resample to daily frequency (forward fill)
        df_clean = resample_daily_ffill(df_clean, extend_to=desired_end)
        if df_clean is None or df_clean.empty:
            mdc.write_warning(f"No valid dated rows after cleaning/resample for {blob_name}; skipping.")
            return BlobProcessResult(
                blob_name=blob_name,
                silver_path=silver_path,
                status="skipped",
                error="No valid dated rows after cleaning/resample.",
            )
        
        # Write to Silver (Overwrite is fine for finance snapshots, or merge? 
        # Typically Finance Sheets are full snapshots. Replacing is safer for consistency, 
        # but if we want history of OLD financial restatements... 
        # Current logic: Overwrite/Upsert. store_delta defaults to append?
        # Let's use overwrite mode for now as Transposed data is simpler.
        # But wait, store_delta default implementation? 
        # Checking delta_core usage: usually it appends or overwrites. 
        # I'll check store_delta signature in next turn if needed.
        # Assuming overwrite for the partition/table is safest for now to avoid specific duplicates.
        
        df_clean = _align_to_existing_schema(df_clean, cfg.AZURE_CONTAINER_SILVER, silver_path)
        delta_core.store_delta(
            df_clean,
            cfg.AZURE_CONTAINER_SILVER,
            silver_path,
            mode="overwrite",
            schema_mode="merge",
        )
        mdc.write_line(f"Updated Silver {silver_path}")
        if watermarks is not None and signature:
            signature["updated_at"] = datetime.utcnow().isoformat()
            watermarks[blob_name] = signature
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="ok")
        
    except Exception as e:
        mdc.write_error(f"Failed to process {blob_name}: {e}")
        return BlobProcessResult(blob_name=blob_name, silver_path=silver_path, status="failed", error=str(e))


def main() -> int:
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze Finance files...")
    # Recursive list? list_blobs(name_starts_with="finance-data/") usually returns all nested.
    blobs = bronze_client.list_blob_infos(name_starts_with="finance-data/")

    watermarks = load_watermarks("bronze_finance_data")
    watermarks_dirty = False
    
    desired_end = _utc_today()

    results: list[BlobProcessResult] = []
    for blob in blobs:
        result = process_blob(blob, desired_end=desired_end, watermarks=watermarks)
        if result.status == "ok" and watermarks is not None:
            watermarks_dirty = True
        results.append(result)
        
    processed = sum(1 for r in results if r.status == "ok")
    skipped = sum(1 for r in results if r.status == "skipped")
    failed = sum(1 for r in results if r.status == "failed")
    mdc.write_line(f"Silver finance ingest complete: processed={processed}, skipped={skipped}, failed={failed}")
    if watermarks is not None and watermarks_dirty:
        save_watermarks("bronze_finance_data", watermarks)
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.finance_data.materialize_silver_finance_by_date import main as by_date_main
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "silver-finance-job"
    exit_code = run_partner_then_by_date(
        job_name=job_name,
        partner_main=main,
        by_date_main=by_date_main,
    )
    if exit_code == 0:
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
