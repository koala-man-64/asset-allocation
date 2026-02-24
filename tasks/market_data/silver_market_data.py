
import pandas as pd
from datetime import datetime

from tasks.market_data import config as cfg
from core import core as mdc
from core import delta_core
from core.pipeline import DataPaths
from tasks.common.backfill import (
    apply_backfill_start_cutoff,
    filter_by_date,
    get_backfill_range,
    get_latest_only_flag,
)
from tasks.common.watermarks import (
    check_blob_unchanged,
    load_last_success,
    load_watermarks,
    save_last_success,
    save_watermarks,
    should_process_blob_since_last_success,
)
from tasks.common.silver_contracts import normalize_columns_to_snake_case

# Suppress warnings

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

_SUPPLEMENTAL_MARKET_COLUMNS = ("ShortInterest", "ShortVolume", "FloatShares")


def _normalize_col_name(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _rename_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Normalize common OHLCV casing for defensive parsing.
    canonical_map = {
        "date": "Date",
        "timestamp": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
    }
    rename_map = {src: dest for src, dest in canonical_map.items() if src in out.columns and dest not in out.columns}
    if rename_map:
        out = out.rename(columns=rename_map)

    # Normalize supplemental metric aliases from Bronze market payloads.
    supplemental_aliases = {
        "shortinterest": "ShortInterest",
        "shortinterestshares": "ShortInterest",
        "sharesshort": "ShortInterest",
        "shortvolume": "ShortVolume",
        "shortvolumeshares": "ShortVolume",
        "volumeshort": "ShortVolume",
        "floatshares": "FloatShares",
        "sharesfloat": "FloatShares",
        "freefloat": "FloatShares",
        "float": "FloatShares",
    }
    normalized_cols = {_normalize_col_name(col): col for col in out.columns}
    alias_renames: dict[str, str] = {}
    for alias_key, canonical in supplemental_aliases.items():
        source_col = normalized_cols.get(alias_key)
        if source_col and source_col != canonical and canonical not in out.columns:
            alias_renames[source_col] = canonical
    if alias_renames:
        out = out.rename(columns=alias_renames)

    return out


def _ensure_numeric_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in ("Open", "High", "Low", "Close"):
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    if "Volume" not in out.columns:
        out["Volume"] = 0.0
    out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce")

    for col in _SUPPLEMENTAL_MARKET_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


def process_file(blob_name: str) -> bool:
    """
    Backwards-compatible wrapper (tests/local tooling) that processes a blob by name.

    Production uses `process_blob()` with `last_modified` metadata for freshness checks.
    """
    return process_blob({"name": blob_name}, watermarks={}) != "failed"

def process_blob(blob: dict, *, watermarks: dict) -> str:
    blob_name = blob["name"]  # market-data/{ticker}.csv
    if not blob_name.endswith(".csv"):
        return "skipped_non_csv"

    if blob_name.endswith("whitelist.csv") or blob_name.endswith("blacklist.csv"):
        return "skipped_list"

    ticker = blob_name.replace("market-data/", "").replace(".csv", "")
    mdc.write_line(f"Processing {ticker} from {blob_name}...")

    silver_path = DataPaths.get_market_data_path(ticker.replace(".", "-"))

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        return "skipped_unchanged"
    
    # 1. Read Raw from Bronze
    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        from io import BytesIO
        df_new = pd.read_csv(BytesIO(raw_bytes))
    except Exception as e:
        mdc.write_error(f"Failed to read/parse {blob_name}: {e}")
        return "failed"

    # 2. Clean/Normalize
    if "Adj Close" in df_new.columns:
        df_new = df_new.drop('Adj Close', axis=1)

    df_new = _rename_market_columns(df_new)
    
    if 'Date' in df_new.columns:
        df_new['Date'] = pd.to_datetime(df_new['Date'], errors="coerce")
        df_new = df_new.dropna(subset=["Date"])
    else:
        mdc.write_error(f"Missing Date column in {blob_name}; skipping.")
        return "failed"

    required_cols = ["Open", "High", "Low", "Close"]
    missing_cols = [col for col in required_cols if col not in df_new.columns]
    if missing_cols:
        mdc.write_error(f"Missing required columns in {blob_name}: {missing_cols}")
        return "failed"

    df_new = _ensure_numeric_market_columns(df_new)

    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        df_new = filter_by_date(df_new, "Date", backfill_start, backfill_end)
        latest_only = False
    else:
        latest_only = get_latest_only_flag("MARKET", default=True)

    # Only process the most recent date unless backfill or latest_only disabled.
    if latest_only and "Date" in df_new.columns and not df_new.empty:
        latest_date = df_new["Date"].max()
        df_new = df_new[df_new["Date"] == latest_date].copy()
    
    df_new['Symbol'] = ticker
    
    # 3. Load Existing Silver (History)
    df_history = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path)
    
    # 4. Merge
    if df_history is None or df_history.empty:
        df_merged = df_new
    else:
        df_history = _rename_market_columns(df_history)
        df_history = _ensure_numeric_market_columns(df_history)
        # Ensure types match before concat
        if 'Date' in df_history.columns:
             df_history['Date'] = pd.to_datetime(df_history['Date'])
        df_merged = pd.concat([df_history, df_new], ignore_index=True)
    
    # 5. Deduplicate and Sort
    df_merged = df_merged.sort_values(by=['Date', 'Symbol', 'Volume'], ascending=[True, True, False])
    df_merged = df_merged.drop_duplicates(subset=['Date', 'Symbol'], keep='last')
    df_merged = df_merged.reset_index(drop=True)

    # Enforce backfill cutoff on the final Silver payload so historical rows are purged from ADLS.
    df_merged, _ = apply_backfill_start_cutoff(
        df_merged,
        date_col="Date",
        backfill_start=backfill_start,
        context=f"silver market {ticker}",
    )
    
    # 6. Type Casting & Formatting
    df_merged = _ensure_numeric_market_columns(df_merged)
    
    cols_to_round = ['Open', 'High', 'Low', 'Close']
    for col in cols_to_round:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].round(2)

    cols_to_drop = ['index', 'Beta (5Y Monthly)', 'PE Ratio (TTM)', '1y Target Est', 'EPS (TTM)', 'Earnings Date', 'Forward Dividend & Yield', 'Market Cap']
    df_merged = df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns])

    if backfill_start is not None and df_merged.empty:
        if silver_client is not None:
            deleted = silver_client.delete_prefix(silver_path)
            mdc.write_line(
                f"Silver market backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                f"deleted {deleted} blob(s) under {silver_path}."
            )
            if signature:
                signature["updated_at"] = datetime.utcnow().isoformat()
                watermarks[blob_name] = signature
            return "ok"
        mdc.write_warning(
            f"Silver market backfill purge for {ticker} could not delete {silver_path}: storage client unavailable."
        )
        return "failed"

    # 7. Write to Silver
    try:
        df_merged = normalize_columns_to_snake_case(df_merged)
        delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, silver_path, mode="overwrite")
        if backfill_start is not None:
            delta_core.vacuum_delta_table(
                cfg.AZURE_CONTAINER_SILVER,
                silver_path,
                retention_hours=0,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
    except Exception as e:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {e}")
        return "failed"

    mdc.write_line(f"Updated Silver Delta for {ticker} (Total rows: {len(df_merged)})")
    if signature:
        signature["updated_at"] = datetime.utcnow().isoformat()
        watermarks[blob_name] = signature
    return "ok"

def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying BACKFILL_START_DATE cutoff to silver market data: {backfill_start.date().isoformat()}")
    
    # List all files in Bronze market-data folder
    # Assuming mdc has a list_blobs or similar, otherwise use client directly
    # mdc.list_blobs is not explicitly shown in context, using client.
    # azure.storage.blob.ContainerClient.list_blobs
    
    mdc.write_line("Listing Bronze files...")
    blobs = bronze_client.list_blob_infos(name_starts_with="market-data/")
    watermarks = load_watermarks("bronze_market_data")
    last_success = load_last_success("silver_market_data")
    watermarks_dirty = False

    # Convert to list to enable progress tracking/filtering
    blob_list = [b for b in blobs if b["name"].endswith(".csv")]
    
    # Debug Filter
    if hasattr(cfg, 'DEBUG_SYMBOLS') and cfg.DEBUG_SYMBOLS:
        mdc.write_line(f"DEBUG MODE: Filtering for {cfg.DEBUG_SYMBOLS}")
        blob_list = [b for b in blob_list if any(s in b["name"] for s in cfg.DEBUG_SYMBOLS)]

    checkpoint_skipped = 0
    candidate_blobs: list[dict] = []
    for blob in blob_list:
        prior = watermarks.get(blob["name"])
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver market checkpoint filter: "
            f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} skipped_checkpoint={checkpoint_skipped}"
        )
    mdc.write_line(f"Found {len(blob_list)} files total; {len(candidate_blobs)} candidate files to process.")

    processed = 0
    failed = 0
    skipped_unchanged = 0
    skipped_other = 0
    for blob in candidate_blobs:
        status = process_blob(blob, watermarks=watermarks)
        if status == "ok":
            processed += 1
            watermarks_dirty = True
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
        elif status.startswith("skipped"):
            skipped_other += 1
        else:
            failed += 1

    mdc.write_line(
        "Silver market job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_other={skipped_other} skipped_checkpoint={checkpoint_skipped} failed={failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_market_data", watermarks)
    if failed == 0:
        save_last_success(
            "silver_market_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "processed": processed,
                "skipped_checkpoint": checkpoint_skipped,
                "skipped_unchanged": skipped_unchanged,
                "skipped_other": skipped_other,
            },
        )
        return 0
    return 1

if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "silver-market-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="silver", domain="market", job_name=job_name)
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
