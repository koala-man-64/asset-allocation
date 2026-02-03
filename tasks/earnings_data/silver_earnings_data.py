
import pandas as pd
from datetime import datetime

from core import core as mdc
from core import config as cfg
from core import delta_core
from core.pipeline import DataPaths
from tasks.common.backfill import filter_by_date, get_backfill_range, get_latest_only_flag
from tasks.common.watermarks import check_blob_unchanged, load_watermarks, save_watermarks

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

def process_file(blob_name: str) -> bool:
    """
    Backwards-compatible wrapper (tests/local tooling) that processes a blob by name.

    Production uses `process_blob()` with `last_modified` metadata for freshness checks.
    """
    return process_blob({"name": blob_name}) != "failed"

def process_blob(blob: dict, *, watermarks: dict | None = None) -> str:
    blob_name = blob["name"]  # earnings-data/{symbol}.json
    if not blob_name.endswith(".json"):
        return "skipped_non_json"

    # Expecting earnings-data/{symbol}.json
    prefix_len = len(cfg.EARNINGS_DATA_PREFIX) + 1 # +1 for slash
    ticker = blob_name[prefix_len:].replace('.json', '')
    mdc.write_line(f"Processing {ticker} from {blob_name}...")

    cloud_path = DataPaths.get_earnings_path(ticker)
    if watermarks is not None:
        unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
        if unchanged:
            return "skipped_unchanged"
    else:
        signature = {}
        bronze_lm = blob.get("last_modified")
        if bronze_lm is not None:
            try:
                bronze_ts = bronze_lm.timestamp()
            except Exception:
                bronze_ts = None
            else:
                silver_lm = delta_core.get_delta_last_commit(cfg.AZURE_CONTAINER_SILVER, cloud_path)
                if silver_lm and (silver_lm > bronze_ts):
                    return "skipped_fresh"
    
    # 1. Read Raw from Bronze
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        from io import BytesIO
        # JSON
        df_new = pd.read_json(BytesIO(raw_bytes), orient='records')
    except Exception as e:
        mdc.write_error(f"Failed to read/parse {blob_name}: {e}")
        return "failed"

    # 2. Clean/Normalize
    df_new = df_new.drop(columns=[col for col in df_new.columns if "Unnamed" in col], errors='ignore')
    if 'Date' in df_new.columns:
        df_new['Date'] = pd.to_datetime(df_new['Date'], errors='coerce')
        df_new = df_new.dropna(subset=["Date"])
    
    df_new['Symbol'] = ticker

    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        df_new = filter_by_date(df_new, "Date", backfill_start, backfill_end)
        latest_only = False
    else:
        latest_only = get_latest_only_flag("EARNINGS", default=True)

    # Only process the most recent earnings date unless backfill or latest_only disabled.
    if latest_only and "Date" in df_new.columns and not df_new.empty:
        latest_date = df_new["Date"].max()
        df_new = df_new[df_new["Date"] == latest_date].copy()
    
    # 3. Load Existing Silver (History)
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
    df_merged = df_merged.reset_index(drop=True)
    
    # 5. Write to Silver
    try:
        delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, cloud_path)
    except Exception as e:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {e}")
        return "failed"

    mdc.write_line(f"Updated Silver Delta for {ticker} (Total rows: {len(df_merged)})")
    if watermarks is not None and signature:
        signature["updated_at"] = datetime.utcnow().isoformat()
        watermarks[blob_name] = signature
    return "ok"

def main():
    mdc.log_environment_diagnostics()
    
    mdc.write_line("Listing Bronze files...")
    blobs = bronze_client.list_blob_infos(name_starts_with="earnings-data/")
    watermarks = load_watermarks("bronze_earnings_data")
    watermarks_dirty = False
    blob_list = [b for b in blobs if b["name"].endswith(".json")]
    
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG: Filtering for {cfg.DEBUG_SYMBOLS}")
        blob_list = [b for b in blob_list if any(s in b["name"] for s in cfg.DEBUG_SYMBOLS)]

    mdc.write_line(f"Found {len(blob_list)} files to process.")
    
    processed = 0
    failed = 0
    skipped_unchanged = 0
    skipped_other = 0
    for blob in blob_list:
        status = process_blob(blob, watermarks=watermarks)
        if status == "ok":
            processed += 1
            if watermarks is not None:
                watermarks_dirty = True
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
        elif status.startswith("skipped"):
            skipped_other += 1
        else:
            failed += 1

    mdc.write_line(
        "Silver earnings job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} skipped_other={skipped_other} failed={failed}"
    )
    if watermarks is not None and watermarks_dirty:
        save_watermarks("bronze_earnings_data", watermarks)
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.earnings_data.materialize_silver_earnings_by_date import (
        discover_year_months_from_data,
        main as by_date_main,
    )
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "silver-earnings-job"
    exit_code = run_partner_then_by_date(
        job_name=job_name,
        partner_main=main,
        by_date_main=by_date_main,
        year_months_provider=discover_year_months_from_data,
    )
    if exit_code == 0:
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
