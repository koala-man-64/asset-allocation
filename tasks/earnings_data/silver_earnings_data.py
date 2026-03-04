import pandas as pd
from datetime import datetime
from io import BytesIO
from typing import Optional

from core import core as mdc
from core import config as cfg
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
from tasks.common.silver_contracts import (
    ContractViolation,
    align_to_existing_schema,
    assert_no_unexpected_mixed_empty,
    log_contract_violation,
    normalize_date_column,
    normalize_columns_to_snake_case,
)
from tasks.common.silver_precision import apply_precision_policy
from tasks.common.market_reconciliation import (
    collect_bronze_earnings_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)
_ALPHA26_EARNINGS_MIN_COLUMNS = [
    "date",
    "symbol",
    "reported_eps",
    "eps_estimate",
    "surprise",
]


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
    cloud_path = DataPaths.get_earnings_path(ticker)
    out = df_new.copy()
    out = out.drop(columns=["source_hash", "ingested_at", "symbol"], errors="ignore")
    out = out.drop(columns=[col for col in out.columns if "Unnamed" in col], errors="ignore")
    out = out.rename(
        columns={
            "date": "Date",
            "reported_eps": "Reported EPS",
            "eps_estimate": "EPS Estimate",
            "surprise": "Surprise",
        }
    )

    try:
        out = normalize_date_column(
            out,
            context=f"earnings date parse {source_name}",
            aliases=("Date", "date"),
            canonical="Date",
        )
        out = assert_no_unexpected_mixed_empty(out, context=f"earnings date filter {source_name}", alias="Date")
    except ContractViolation as exc:
        log_contract_violation(f"earnings preflight failed for {source_name}", exc, severity="ERROR")
        return "failed"

    out["Symbol"] = ticker

    backfill_start, backfill_end = get_backfill_range()
    if backfill_start or backfill_end:
        out = filter_by_date(out, "Date", backfill_start, backfill_end)
        latest_only = False
    else:
        latest_only = get_latest_only_flag("EARNINGS", default=True)
    if not include_history:
        latest_only = False

    if latest_only and "Date" in out.columns and not out.empty:
        latest_date = out["Date"].max()
        out = out[out["Date"] == latest_date].copy()

    df_history = (
        delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, cloud_path)
        if include_history
        else None
    )
    if df_history is None or df_history.empty:
        df_merged = out
    else:
        df_history = align_to_existing_schema(df_history, cfg.AZURE_CONTAINER_SILVER, cloud_path)
        if "Date" in df_history.columns:
            df_history["Date"] = pd.to_datetime(df_history["Date"], errors="coerce")
        elif "date" in df_history.columns:
            df_history = df_history.rename(columns={"date": "Date"})
            df_history["Date"] = pd.to_datetime(df_history["Date"], errors="coerce")
        df_merged = pd.concat([df_history, out], ignore_index=True)

    df_merged = df_merged.sort_values(by=["Date"], ascending=True)
    df_merged = df_merged.drop_duplicates(subset=["Date", "Symbol"], keep="last")
    df_merged = df_merged.reset_index(drop=True)

    df_merged, _ = apply_backfill_start_cutoff(
        df_merged,
        date_col="Date",
        backfill_start=backfill_start,
        context=f"silver earnings {ticker}",
    )
    if backfill_start is not None and df_merged.empty:
        if not persist:
            return "ok"
        if silver_client is not None:
            deleted = silver_client.delete_prefix(cloud_path)
            mdc.write_line(
                f"Silver earnings backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                f"deleted {deleted} blob(s) under {cloud_path}."
            )
            return "ok"
        mdc.write_warning(
            f"Silver earnings backfill purge for {ticker} could not delete {cloud_path}: storage client unavailable."
        )
        return "failed"

    df_merged = normalize_columns_to_snake_case(df_merged)
    df_merged = apply_precision_policy(
        df_merged,
        price_columns=set(),
        calculated_columns=set(),
        price_scale=2,
        calculated_scale=4,
    )
    try:
        if not persist:
            if alpha26_bucket_frames is None:
                raise ValueError("alpha26_bucket_frames must be provided when persist=False.")
            bucket = layer_bucketing.bucket_letter(ticker)
            alpha26_bucket_frames.setdefault(bucket, []).append(df_merged.copy())
        else:
            delta_core.store_delta(df_merged, cfg.AZURE_CONTAINER_SILVER, cloud_path)
            if backfill_start is not None:
                delta_core.vacuum_delta_table(
                    cfg.AZURE_CONTAINER_SILVER,
                    cloud_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
        mdc.write_line(
            "precision_policy_applied domain=earnings "
            f"ticker={ticker} price_cols=none calc_cols=none rows={len(df_merged)}"
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
    blob_name = blob["name"]  # earnings-data/{symbol}.json
    if not blob_name.endswith(".json"):
        return "skipped_non_json"

    prefix_len = len(cfg.EARNINGS_DATA_PREFIX) + 1
    ticker = blob_name[prefix_len:].replace(".json", "")
    mdc.write_line(f"Processing {ticker} from {blob_name}...")
    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_json(BytesIO(raw_bytes), orient="records")
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
    alpha26_bucket_frames: Optional[dict[str, list[pd.DataFrame]]] = None,
) -> str:
    blob_name = str(blob.get("name", ""))
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        return "skipped_unchanged"

    try:
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_bucket = pd.read_parquet(BytesIO(raw_bytes))
    except Exception as exc:
        mdc.write_error(f"Failed to read earnings alpha26 bucket {blob_name}: {exc}")
        return "failed"

    if df_bucket is None or df_bucket.empty:
        if signature:
            signature["updated_at"] = datetime.utcnow().isoformat()
            watermarks[blob_name] = signature
        return "ok"

    symbol_col = "symbol" if "symbol" in df_bucket.columns else ("Symbol" if "Symbol" in df_bucket.columns else None)
    if symbol_col is None:
        mdc.write_error(f"Missing symbol column in earnings alpha26 bucket {blob_name}.")
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


def _write_alpha26_earnings_buckets(bucket_frames: dict[str, list[pd.DataFrame]]) -> tuple[int, Optional[str]]:
    symbol_to_bucket: dict[str, str] = {}
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        bucket_path = DataPaths.get_silver_earnings_bucket_path(bucket)
        existing_cols = delta_core.get_delta_schema_columns(cfg.AZURE_CONTAINER_SILVER, bucket_path)
        parts = bucket_frames.get(bucket, [])
        if parts:
            df_bucket = pd.concat(parts, ignore_index=True)
            if "symbol" in df_bucket.columns and "date" in df_bucket.columns:
                df_bucket["symbol"] = df_bucket["symbol"].astype(str).str.upper()
                df_bucket["date"] = pd.to_datetime(df_bucket["date"], errors="coerce")
                df_bucket = df_bucket.dropna(subset=["symbol", "date"]).copy()
                df_bucket = df_bucket.sort_values(["symbol", "date"]).drop_duplicates(
                    subset=["symbol", "date"], keep="last"
                )
                for symbol in df_bucket["symbol"].dropna().astype(str).tolist():
                    if symbol:
                        symbol_to_bucket[symbol] = bucket
            else:
                df_bucket = pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)
        else:
            df_bucket = pd.DataFrame(columns=_ALPHA26_EARNINGS_MIN_COLUMNS)
        if df_bucket.empty and not existing_cols:
            mdc.write_line(
                f"Skipping Silver earnings empty bucket write for {bucket_path}: no existing Delta schema."
            )
            continue
        delta_core.store_delta(
            df_bucket.reset_index(drop=True),
            cfg.AZURE_CONTAINER_SILVER,
            bucket_path,
            mode="overwrite",
        )
    index_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="earnings",
        symbol_to_bucket=symbol_to_bucket,
    )
    return len(symbol_to_bucket), index_path


def _run_earnings_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver earnings reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_earnings_symbols_from_blob_infos(bronze_blob_list)
    earnings_prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix=earnings_prefix)
    orphan_symbols, deleted_blobs = purge_orphan_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        downstream_path_builder=DataPaths.get_earnings_path,
        delete_prefix=silver_client.delete_prefix,
    )
    if orphan_symbols:
        mdc.write_line(
            "Silver earnings reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Silver earnings reconciliation: no orphan symbols detected.")

    backfill_start, _ = get_backfill_range()
    cutoff_symbols = silver_symbols.difference(set(orphan_symbols))
    cutoff_stats = enforce_backfill_cutoff_on_tables(
        symbols=cutoff_symbols,
        table_paths_for_symbol=lambda symbol: [DataPaths.get_earnings_path(symbol)],
        load_table=lambda path: delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, path),
        store_table=lambda df, path: delta_core.store_delta(df, cfg.AZURE_CONTAINER_SILVER, path, mode="overwrite"),
        delete_prefix=silver_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="silver earnings reconciliation cutoff",
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
            "Silver earnings reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Silver earnings reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver earnings data: {backfill_start.date().isoformat()}")
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
    force_rebuild = layer_bucketing.silver_alpha26_force_rebuild()

    mdc.write_line("Listing Bronze files...")
    earnings_prefix = str(getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data")).strip("/")
    blobs = bronze_client.list_blob_infos(name_starts_with=f"{earnings_prefix}/")
    watermarks = load_watermarks("bronze_earnings_data")
    last_success = load_last_success("silver_earnings_data")
    watermarks_dirty = False
    blob_list = [
        b
        for b in blobs
        if str(b.get("name", "")).startswith(f"{earnings_prefix}/buckets/")
        and str(b.get("name", "")).endswith(".parquet")
    ]

    checkpoint_skipped = 0
    candidate_blobs: list[dict] = []
    for blob in blob_list:
        prior = watermarks.get(blob["name"])
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_rebuild,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver earnings checkpoint filter: "
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
    if failed == 0:
        try:
            alpha26_written_symbols, alpha26_index_path = _write_alpha26_earnings_buckets(alpha26_bucket_frames)
            mdc.write_line(
                "Silver earnings alpha26 buckets written: "
                f"symbols={alpha26_written_symbols} index_path={alpha26_index_path or 'unavailable'}"
            )
        except Exception as exc:
            failed += 1
            mdc.write_error(f"Silver earnings alpha26 bucket write failed: {exc}")

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver earnings job complete: "
        f"processed={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_other={skipped_other} skipped_checkpoint={checkpoint_skipped} "
        f"alpha26_symbols={alpha26_written_symbols} "
        f"reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} "
        f"failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_earnings_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_earnings_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "processed": processed,
                "skipped_checkpoint": checkpoint_skipped,
                "skipped_unchanged": skipped_unchanged,
                "skipped_other": skipped_other,
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

    job_name = "silver-earnings-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="silver", domain="earnings", job_name=job_name)
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
