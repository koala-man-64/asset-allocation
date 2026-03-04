
import pandas as pd
from datetime import datetime
from io import BytesIO
from typing import Optional

from tasks.market_data import config as cfg
from core import core as mdc
from core import delta_core
from core.pipeline import DataPaths
from tasks.common import bronze_bucketing
from tasks.common import layer_bucketing
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
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_market_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_market_tables,
)

# Suppress warnings

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)

_SUPPLEMENTAL_MARKET_COLUMNS = ("ShortInterest", "ShortVolume")
_REMOVED_MARKET_COLUMNS = ("FloatShares", "float_shares", "shares_float", "free_float", "float")
_INDEX_ARTIFACT_COLUMN_NAMES = {
    "index",
    "level_0",
    "index_level_0",
}
_MARKET_PRICE_COLUMNS = {"open", "high", "low", "close"}
_ALPHA26_MARKET_MIN_COLUMNS = [
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "short_interest",
    "short_volume",
]
_ALPHA26_MARKET_NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "short_interest",
    "short_volume",
]


def _empty_alpha26_market_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "symbol": pd.Series(dtype="string"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
            "short_interest": pd.Series(dtype="float64"),
            "short_volume": pd.Series(dtype="float64"),
        }
    )


def _coerce_alpha26_market_bucket_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in _ALPHA26_MARKET_MIN_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["symbol"] = out["symbol"].astype("string").str.upper()
    for col in _ALPHA26_MARKET_NUMERIC_COLUMNS:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["symbol", "date"]).copy()
    if out.empty:
        return _empty_alpha26_market_frame()

    out = out.sort_values(["symbol", "date"]).drop_duplicates(subset=["symbol", "date"], keep="last")
    return out[_ALPHA26_MARKET_MIN_COLUMNS].reset_index(drop=True)


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
        "symbol": "Symbol",
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


def _drop_removed_market_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    to_drop = [col for col in _REMOVED_MARKET_COLUMNS if col in out.columns]
    if to_drop:
        out = out.drop(columns=to_drop)
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


def _repair_symbol_column_aliases(df: pd.DataFrame, *, ticker: str) -> pd.DataFrame:
    out = df.copy()
    legacy_symbol_cols = [
        col
        for col in out.columns
        if isinstance(col, str) and col.startswith("symbol_") and col[7:].isdigit()
    ]
    if not legacy_symbol_cols:
        return out

    if "symbol" not in out.columns:
        first_legacy = legacy_symbol_cols[0]
        out = out.rename(columns={first_legacy: "symbol"})
        legacy_symbol_cols = legacy_symbol_cols[1:]
        mdc.write_warning(
            f"Silver market {ticker}: renamed legacy column {first_legacy} -> symbol."
        )

    for col in legacy_symbol_cols:
        if col not in out.columns:
            continue
        primary = out["symbol"].astype("string")
        fallback = out[col].astype("string")
        conflicts = int((primary.notna() & fallback.notna() & (primary != fallback)).sum())
        if conflicts > 0:
            mdc.write_warning(
                f"Silver market {ticker}: symbol repair conflict in {col}; "
                f"conflicting_rows={conflicts}; keeping existing symbol when both populated."
            )
        out["symbol"] = out["symbol"].combine_first(out[col])
        out = out.drop(columns=[col])
        mdc.write_warning(
            f"Silver market {ticker}: collapsed legacy column {col} into symbol."
        )

    return out


def _drop_index_artifact_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    to_drop: list[str] = []
    for col in out.columns:
        normalized = str(col).strip().lower()
        if normalized in _INDEX_ARTIFACT_COLUMN_NAMES:
            to_drop.append(col)
            continue
        if normalized.startswith("unnamed_"):
            suffix = normalized[len("unnamed_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue
        if normalized.startswith("index_level_"):
            suffix = normalized[len("index_level_") :]
            if suffix.replace("_", "").isdigit():
                to_drop.append(col)
                continue
    if to_drop:
        out = out.drop(columns=to_drop)
    return out


def process_file(blob_name: str) -> bool:
    """
    Backwards-compatible wrapper (tests/local tooling) that processes a blob by name.

    Production uses `process_blob()` with `last_modified` metadata for freshness checks.
    """
    return process_blob({"name": blob_name}, watermarks={}) != "failed"


def _process_symbol_frame(
    *,
    ticker: str,
    df_new: pd.DataFrame,
    source_name: str,
    include_history: bool = True,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    silver_path = DataPaths.get_market_data_path(ticker.replace(".", "-"))
    out = df_new.copy()
    out = out.drop(columns=["source_hash", "ingested_at"], errors="ignore")

    if "Adj Close" in out.columns:
        out = out.drop("Adj Close", axis=1)

    out = _rename_market_columns(out)
    if "Date" not in out.columns:
        mdc.write_error(f"Missing Date column in {source_name}; skipping {ticker}.")
        return "failed"

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date"])
    if out.empty:
        return "skipped_empty"

    required_cols = ["Open", "High", "Low", "Close"]
    missing_cols = [col for col in required_cols if col not in out.columns]
    if missing_cols:
        mdc.write_error(f"Missing required columns in {source_name} for {ticker}: {missing_cols}")
        return "failed"

    out = _drop_removed_market_columns(out)
    out = _ensure_numeric_market_columns(out)

    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        out = filter_by_date(out, "Date", backfill_start, backfill_end)
        latest_only = False
    else:
        latest_only = get_latest_only_flag("MARKET", default=True)

    if not include_history:
        # Alpha26 rebuild mode writes full per-symbol slices into bucket tables.
        latest_only = False

    if latest_only and "Date" in out.columns and not out.empty:
        latest_date = out["Date"].max()
        out = out[out["Date"] == latest_date].copy()

    out["Symbol"] = ticker
    df_history = (
        delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path)
        if include_history
        else None
    )

    if df_history is None or df_history.empty:
        df_merged = out
    else:
        df_history = _rename_market_columns(df_history)
        df_history = _drop_removed_market_columns(df_history)
        df_history = _ensure_numeric_market_columns(df_history)
        if "Date" in df_history.columns:
            df_history["Date"] = pd.to_datetime(df_history["Date"])
        df_merged = pd.concat([df_history, out], ignore_index=True)

    df_merged = df_merged.sort_values(by=["Date", "Symbol", "Volume"], ascending=[True, True, False])
    df_merged = df_merged.drop_duplicates(subset=["Date", "Symbol"], keep="last")
    df_merged = df_merged.reset_index(drop=True)

    df_merged, _ = apply_backfill_start_cutoff(
        df_merged,
        date_col="Date",
        backfill_start=backfill_start,
        context=f"silver market {ticker}",
    )

    df_merged = _drop_removed_market_columns(df_merged)
    df_merged = _ensure_numeric_market_columns(df_merged)
    cols_to_drop = [
        "index",
        "Beta (5Y Monthly)",
        "PE Ratio (TTM)",
        "1y Target Est",
        "EPS (TTM)",
        "Earnings Date",
        "Forward Dividend & Yield",
        "Market Cap",
    ]
    df_merged = df_merged.drop(columns=[c for c in cols_to_drop if c in df_merged.columns])

    if backfill_start is not None and df_merged.empty:
        if not persist:
            return "ok"
        if silver_client is not None:
            deleted = silver_client.delete_prefix(silver_path)
            mdc.write_line(
                f"Silver market backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                f"deleted {deleted} blob(s) under {silver_path}."
            )
            return "ok"
        mdc.write_warning(
            f"Silver market backfill purge for {ticker} could not delete {silver_path}: storage client unavailable."
        )
        return "failed"

    try:
        df_merged = normalize_columns_to_snake_case(df_merged)
        df_merged = _repair_symbol_column_aliases(df_merged, ticker=ticker)
        df_merged = _drop_index_artifact_columns(df_merged)
        df_merged = apply_precision_policy(
            df_merged,
            price_columns=_MARKET_PRICE_COLUMNS,
            calculated_columns=set(),
            price_scale=2,
            calculated_scale=4,
        )
        if not persist:
            if alpha26_bucket_frames is None:
                raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
            bucket = layer_bucketing.bucket_letter(ticker)
            alpha26_bucket_frames.setdefault(bucket, []).append(df_merged.copy())
        else:
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
        applied_price_cols = sorted(col for col in _MARKET_PRICE_COLUMNS if col in df_merged.columns)
        price_cols_str = ",".join(applied_price_cols) if applied_price_cols else "none"
        mdc.write_line(
            "precision_policy_applied domain=market "
            f"ticker={ticker} price_cols={price_cols_str} calc_cols=none rows={len(df_merged)}"
        )
    except Exception as exc:
        mdc.write_error(f"Failed to write Silver Delta for {ticker}: {exc}")
        return "failed"

    if persist:
        mdc.write_line(f"Updated Silver Delta for {ticker} (Total rows: {len(df_merged)})")
    return "ok"


def process_blob(
    blob: dict,
    *,
    watermarks: dict,
    include_history: bool = True,
    persist: bool = True,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = blob["name"]  # market-data/{ticker}.csv
    if not blob_name.endswith(".csv"):
        return "skipped_non_csv"

    if blob_name.endswith("whitelist.csv") or blob_name.endswith("blacklist.csv"):
        return "skipped_list"

    ticker = blob_name.replace("market-data/", "").replace(".csv", "")
    mdc.write_line(f"Processing {ticker} from {blob_name}...")
    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_csv(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_error(f"Failed to read/parse {blob_name}: {exc}")
        return "failed"

    status = _process_symbol_frame(
        ticker=ticker,
        df_new=df_new,
        source_name=blob_name,
        include_history=include_history,
        persist=persist,
        alpha26_bucket_frames=alpha26_bucket_frames,
    )
    if status == "ok" and signature:
        signature["updated_at"] = datetime.utcnow().isoformat()
        watermarks[blob_name] = signature
    return status


def process_alpha26_bucket_blob(
    blob: dict,
    *,
    watermarks: dict,
    include_history: bool = False,
    persist: bool = False,
    force_reprocess: bool = False,
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = str(blob.get("name", ""))
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged and not force_reprocess:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_error(f"Failed to read market alpha26 bucket {blob_name}: {exc}")
        return "failed"

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = datetime.utcnow().isoformat()
            watermarks[blob_name] = signature
        return "ok"

    symbol_col = "symbol" if "symbol" in df_bucket.columns else ("Symbol" if "Symbol" in df_bucket.columns else None)
    if symbol_col is None:
        mdc.write_error(f"Missing symbol column in market alpha26 bucket {blob_name}.")
        return "failed"

    debug_symbols = set(getattr(cfg, "DEBUG_SYMBOLS", []) or [])
    has_failed = False
    for symbol, group in df_bucket.groupby(symbol_col):
        ticker = str(symbol or "").strip().upper()
        if not ticker:
            continue
        if debug_symbols and ticker not in debug_symbols:
            continue
        status = _process_symbol_frame(
            ticker=ticker,
            df_new=group.copy(),
            source_name=blob_name,
            include_history=include_history,
            persist=persist,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        if status == "failed":
            has_failed = True

    if not has_failed and signature:
        signature["updated_at"] = datetime.utcnow().isoformat()
        watermarks[blob_name] = signature
    return "failed" if has_failed else "ok"


def _write_alpha26_market_buckets(bucket_frames: dict[str, list[pd.DataFrame]]) -> tuple[int, Optional[str]]:
    symbol_to_bucket: dict[str, str] = {}
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        parts = bucket_frames.get(bucket, [])
        if parts:
            df_bucket = pd.concat(parts, ignore_index=True)
        else:
            df_bucket = _empty_alpha26_market_frame()

        df_bucket = _coerce_alpha26_market_bucket_frame(df_bucket)
        for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
            if symbol:
                symbol_to_bucket[symbol] = bucket
        delta_core.store_delta(
            df_bucket.reset_index(drop=True),
            cfg.AZURE_CONTAINER_SILVER,
            DataPaths.get_silver_market_bucket_path(bucket),
            mode="overwrite",
        )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="market",
        symbol_to_bucket=symbol_to_bucket,
    )
    return len(symbol_to_bucket), index_path


def _count_staged_bucket_rows(bucket_frames: dict[str, list[pd.DataFrame]]) -> int:
    total_rows = 0
    for parts in bucket_frames.values():
        for frame in parts:
            if frame is not None:
                total_rows += int(len(frame))
    return total_rows


def _detect_missing_alpha26_market_buckets() -> tuple[bool, set[str]]:
    if silver_client is None:
        return False, set()
    try:
        blob_infos = silver_client.list_blob_infos(name_starts_with="market-data/buckets/")
    except Exception as exc:
        mdc.write_warning(f"Silver market alpha26 bootstrap probe failed: {exc}")
        return False, set()

    present_buckets: set[str] = set()
    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    for blob in blob_infos:
        name = str(blob.get("name", "")).strip("/")
        parts = name.split("/")
        if len(parts) < 3:
            continue
        bucket = parts[2].strip().upper()
        if bucket in valid_buckets:
            present_buckets.add(bucket)

    missing = valid_buckets.difference(present_buckets)
    return bool(missing), missing


def _run_market_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver market reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_market_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="market-data")
    orphan_symbols, deleted_blobs = purge_orphan_market_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        downstream_path_builder=DataPaths.get_market_data_path,
        delete_prefix=silver_client.delete_prefix,
    )
    if orphan_symbols:
        mdc.write_line(
            "Silver market reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Silver market reconciliation: no orphan symbols detected.")

    backfill_start, _ = get_backfill_range()
    cutoff_symbols = silver_symbols.difference(set(orphan_symbols))
    cutoff_stats = enforce_backfill_cutoff_on_tables(
        symbols=cutoff_symbols,
        table_paths_for_symbol=lambda symbol: [DataPaths.get_market_data_path(symbol)],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver market reconciliation cutoff",
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            cfg.AZURE_CONTAINER_SILVER,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Silver market reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver market reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver market data: {backfill_start.date().isoformat()}")
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()
    
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
    blob_list = [
        b
        for b in blobs
        if str(b.get("name", "")).startswith("market-data/buckets/") and str(b.get("name", "")).endswith(".parquet")
    ]

    checkpoint_skipped = 0
    candidate_blobs: list[dict] = []
    bootstrap_missing, missing_buckets = _detect_missing_alpha26_market_buckets()
    force_checkpoint_rebuild = bool(force_rebuild or bootstrap_missing)
    if bootstrap_missing:
        missing_list = ",".join(sorted(missing_buckets))
        mdc.write_warning(
            "Silver market alpha26 bootstrap required: "
            f"missing_bucket_tables={missing_list}; forcing bronze replay."
        )
    for blob in blob_list:
        prior = watermarks.get(blob["name"])
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_checkpoint_rebuild,
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
    alpha26_bucket_frames: dict[str, list[pd.DataFrame]] = {}
    for blob in candidate_blobs:
        status = process_alpha26_bucket_blob(
            blob,
            watermarks=watermarks,
            include_history=False,
            persist=False,
            force_reprocess=bootstrap_missing,
            alpha26_bucket_frames=alpha26_bucket_frames,
        )
        if status == "ok":
            processed += 1
            watermarks_dirty = True
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
        elif status.startswith("skipped"):
            skipped_other += 1
        else:
            failed += 1

    alpha26_written_symbols = 0
    alpha26_index_path: Optional[str] = None
    alpha26_staged_rows = _count_staged_bucket_rows(alpha26_bucket_frames)
    if failed == 0:
        if alpha26_staged_rows == 0:
            mdc.write_line("Silver market alpha26 bucket write skipped: no staged rows.")
        else:
            try:
                alpha26_written_symbols, alpha26_index_path = _write_alpha26_market_buckets(alpha26_bucket_frames)
                mdc.write_line(
                    "Silver market alpha26 buckets written: "
                    f"symbols={alpha26_written_symbols} index_path={alpha26_index_path or 'unavailable'}"
                )
            except Exception as exc:
                failed += 1
                mdc.write_error(f"Silver market alpha26 bucket write failed: {exc}")

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver market job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_other={skipped_other} skipped_checkpoint={checkpoint_skipped} "
        f"alpha26_staged_rows={alpha26_staged_rows} "
        f"alpha26_symbols={alpha26_written_symbols} "
        f"reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} "
        f"failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_market_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_market_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "processed": processed,
                "skipped_checkpoint": checkpoint_skipped,
                "skipped_unchanged": skipped_unchanged,
                "skipped_other": skipped_other,
                "alpha26_staged_rows": alpha26_staged_rows,
                "alpha26_symbols": alpha26_written_symbols,
                "alpha26_index_path": alpha26_index_path,
                "reconciled_orphans": reconciliation_orphans,
                "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
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
