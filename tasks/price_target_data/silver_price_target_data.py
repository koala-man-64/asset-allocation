import pandas as pd
import numpy as np
from datetime import datetime
from io import BytesIO

from core import core as mdc
from core import delta_core
from tasks.price_target_data import config as cfg
from core.pipeline import DataPaths
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
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
from tasks.common.market_reconciliation import (
    collect_bronze_price_target_symbols_from_blob_infos,
    collect_delta_market_symbols,
    purge_orphan_tables,
)

# Initialize Clients
bronze_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
silver_client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)


def _needs_obs_date_migration(silver_path: str) -> bool:
    """
    Determine whether an existing Silver table still uses legacy date naming.

    We migrate legacy `Date`/`date` schemas to canonical `obs_date` even when
    the source blob watermark is unchanged.
    """
    schema = delta_core.get_delta_schema_columns(cfg.AZURE_CONTAINER_SILVER, silver_path)
    if not schema:
        return False

    lowered = {str(col).strip().lower() for col in schema}
    return "obs_date" not in lowered and "date" in lowered


def _extract_ticker(blob_name: str) -> str:
    return blob_name.replace("price-target-data/", "").replace(".parquet", "")


def process_blob(blob, *, watermarks: dict) -> str:
    blob_name = blob["name"]  # price-target-data/{symbol}.parquet
    if not blob_name.endswith(".parquet"):
        return "skipped_non_parquet"

    ticker = blob_name.replace("price-target-data/", "").replace(".parquet", "")
    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS and ticker not in cfg.DEBUG_SYMBOLS:
        return "skipped_debug_symbols"

    # Silver Path
    silver_path = DataPaths.get_price_target_path(ticker)
    backfill_start, _ = get_backfill_range()

    unchanged, signature = check_blob_unchanged(blob, watermarks.get(blob_name))
    if unchanged:
        if not _needs_obs_date_migration(silver_path):
            return "skipped_unchanged"
        mdc.write_line(f"Schema migration required for {ticker}; processing despite unchanged source blob.")

    mdc.write_line(f"Processing {ticker}...")

    try:
        # Read Bronze
        raw_bytes = mdc.read_raw_bytes(blob_name, client=bronze_client)
        df_new = pd.read_parquet(BytesIO(raw_bytes))

        column_names = [
            "symbol",
            "obs_date",
            "tp_mean_est",
            "tp_std_dev_est",
            "tp_high_est",
            "tp_low_est",
            "tp_cnt_est",
            "tp_cnt_est_rev_up",
            "tp_cnt_est_rev_down",
        ]

        # Transform
        if df_new.empty:
            return "skipped_empty"

        try:
            df_new = normalize_date_column(
                df_new,
                context=f"price target date parse {blob_name}",
                aliases=("obs_date", "Date", "date"),
                canonical="obs_date",
            )
            df_new = assert_no_unexpected_mixed_empty(
                df_new, context=f"price target date filter {blob_name}", alias="obs_date"
            )
            df_new["obs_date"] = df_new["obs_date"].dt.normalize()
        except ContractViolation as exc:
            log_contract_violation(
                f"price-target preflight failed for {ticker} in {blob_name}",
                exc,
                severity="ERROR",
            )
            return "failed"

        df_new, _ = apply_backfill_start_cutoff(
            df_new,
            date_col="obs_date",
            backfill_start=backfill_start,
            context=f"silver price-target {ticker}",
        )
        if backfill_start is not None and df_new.empty:
            if silver_client is not None:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver price-target backfill purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
                if signature:
                    signature["updated_at"] = datetime.utcnow().isoformat()
                    watermarks[blob_name] = signature
                return "ok"
            mdc.write_warning(
                f"Silver price-target backfill purge for {ticker} could not delete {silver_path}: storage client unavailable."
            )
            return "failed"

        df_new = df_new.sort_values(by="obs_date")

        # Carry Forward / Upsample
        today = pd.to_datetime("today").normalize()
        if not df_new.empty:
            latest_obs = df_new["obs_date"].max()
            if latest_obs < today:
                # Extend date range
                all_dates = pd.date_range(start=df_new["obs_date"].min(), end=today)
                df_dates = pd.DataFrame({"obs_date": all_dates})
                df_new = df_dates.merge(df_new, on="obs_date", how="left")
                df_new = df_new.ffill()

        df_new["symbol"] = ticker

        for col in column_names:
            if col not in df_new.columns:
                df_new[col] = np.nan
        df_new = df_new[column_names]

        # Resample Daily (Full Range)
        df_new = df_new.set_index("obs_date")
        df_new = df_new[~df_new.index.duplicated(keep="last")]

        full_range = pd.date_range(start=df_new.index.min(), end=df_new.index.max(), freq="D")
        df_new = df_new.reindex(full_range)
        df_new.ffill(inplace=True)
        df_new = df_new.reset_index().rename(columns={"index": "obs_date"})
        df_new["symbol"] = ticker

        # Load Existing Silver
        df_history = delta_core.load_delta(cfg.AZURE_CONTAINER_SILVER, silver_path)

        # Merge
        if df_history is None or df_history.empty:
            df_merged = df_new
        else:
            df_history = align_to_existing_schema(df_history, container=cfg.AZURE_CONTAINER_SILVER, path=silver_path)
            if "Date" in df_history.columns and "obs_date" not in df_history.columns:
                df_history = df_history.rename(columns={"Date": "obs_date"})
            if "date" in df_history.columns and "obs_date" not in df_history.columns:
                df_history = df_history.rename(columns={"date": "obs_date"})
            if "obs_date" in df_history.columns:
                df_history["obs_date"] = pd.to_datetime(df_history["obs_date"], errors="coerce")
            df_merged = pd.concat([df_history, df_new], ignore_index=True)

        for legacy_col in ("Date", "date"):
            if legacy_col in df_merged.columns:
                df_merged = df_merged.drop(columns=[legacy_col])

        df_merged = df_merged.drop_duplicates(subset=["obs_date", "symbol"], keep="last")
        df_merged = df_merged.sort_values(by=["obs_date", "symbol"])
        df_merged = df_merged.reset_index(drop=True)

        df_merged, _ = apply_backfill_start_cutoff(
            df_merged,
            date_col="obs_date",
            backfill_start=backfill_start,
            context=f"silver price-target merged {ticker}",
        )
        if backfill_start is not None and df_merged.empty:
            if silver_client is not None:
                deleted = silver_client.delete_prefix(silver_path)
                mdc.write_line(
                    f"Silver price-target merged purge for {ticker}: no rows >= {backfill_start.date().isoformat()}, "
                    f"deleted {deleted} blob(s) under {silver_path}."
                )
                if signature:
                    signature["updated_at"] = datetime.utcnow().isoformat()
                    watermarks[blob_name] = signature
                return "ok"
            mdc.write_warning(
                f"Silver price-target merged purge for {ticker} could not delete {silver_path}: storage client unavailable."
            )
            return "failed"

        df_merged = normalize_columns_to_snake_case(df_merged)

        # Write
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
        mdc.write_line(f"Updated Silver {ticker}")
        if signature:
            signature["updated_at"] = datetime.utcnow().isoformat()
            watermarks[blob_name] = signature
        return "ok"
    except Exception as e:
        mdc.write_error(f"Failed to process {ticker}: {e}")
        return "failed"


def _run_price_target_reconciliation(*, bronze_blob_list: list[dict]) -> tuple[int, int]:
    if silver_client is None:
        raise RuntimeError("Silver price-target reconciliation requires silver storage client.")

    bronze_symbols = collect_bronze_price_target_symbols_from_blob_infos(bronze_blob_list)
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="price-target-data")
    orphan_symbols, deleted_blobs = purge_orphan_tables(
        upstream_symbols=bronze_symbols,
        downstream_symbols=silver_symbols,
        downstream_path_builder=DataPaths.get_price_target_path,
        delete_prefix=silver_client.delete_prefix,
    )
    if orphan_symbols:
        mdc.write_line(
            "Silver price-target reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Silver price-target reconciliation: no orphan symbols detected.")
    return len(orphan_symbols), deleted_blobs


def main():
    mdc.log_environment_diagnostics()
    backfill_start, _ = get_backfill_range()
    if backfill_start is not None:
        mdc.write_line(f"Applying historical cutoff to silver price-target data: {backfill_start.date().isoformat()}")
    mdc.write_line("Listing Bronze Price Target files...")
    blobs = bronze_client.list_blob_infos(name_starts_with="price-target-data/")
    watermarks = load_watermarks("bronze_price_target_data")
    last_success = load_last_success("silver_price_target_data")
    watermarks_dirty = False

    blob_list = list(blobs)
    checkpoint_skipped = 0
    forced_schema_migration = 0
    candidate_blobs: list[dict] = []
    for blob in blob_list:
        blob_name = str(blob.get("name", ""))
        force_reprocess = False
        if blob_name.endswith(".parquet"):
            ticker = _extract_ticker(blob_name)
            silver_path = DataPaths.get_price_target_path(ticker)
            force_reprocess = _needs_obs_date_migration(silver_path)
            if force_reprocess:
                forced_schema_migration += 1
        prior = watermarks.get(blob_name)
        should_process = should_process_blob_since_last_success(
            blob,
            prior_signature=prior,
            last_success_at=last_success,
            force_reprocess=force_reprocess,
        )
        if should_process:
            candidate_blobs.append(blob)
        else:
            checkpoint_skipped += 1

    if last_success is not None:
        mdc.write_line(
            "Silver price target checkpoint filter: "
            f"last_success={last_success.isoformat()} candidates={len(candidate_blobs)} "
            f"skipped_checkpoint={checkpoint_skipped} forced_schema_migration={forced_schema_migration}"
        )
    mdc.write_line(f"Found {len(blob_list)} blobs total; {len(candidate_blobs)} candidate blobs. Processing...")

    ok_or_skipped = 0
    failed = 0
    skipped_unchanged = 0
    skipped_other = 0
    for blob in candidate_blobs:
        status = process_blob(blob, watermarks=watermarks)
        if status == "ok":
            ok_or_skipped += 1
            watermarks_dirty = True
        elif status == "skipped_unchanged":
            skipped_unchanged += 1
            ok_or_skipped += 1
        elif status.startswith("skipped"):
            skipped_other += 1
            ok_or_skipped += 1
        else:
            failed += 1

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    try:
        reconciliation_orphans, reconciliation_deleted_blobs = _run_price_target_reconciliation(
            bronze_blob_list=blob_list
        )
    except Exception as exc:
        reconciliation_failed = 1
        mdc.write_error(f"Silver price-target reconciliation failed: {exc}")

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Silver price target job complete: "
        f"ok_or_skipped={ok_or_skipped} skipped_unchanged={skipped_unchanged} skipped_other={skipped_other} "
        f"skipped_checkpoint={checkpoint_skipped} reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} failed={total_failed}"
    )
    if watermarks_dirty:
        save_watermarks("bronze_price_target_data", watermarks)
    if total_failed == 0:
        save_last_success(
            "silver_price_target_data",
            metadata={
                "total_blobs": len(blob_list),
                "candidates": len(candidate_blobs),
                "ok_or_skipped": ok_or_skipped,
                "skipped_checkpoint": checkpoint_skipped,
                "forced_schema_migration": forced_schema_migration,
                "reconciled_orphans": reconciliation_orphans,
                "reconciliation_deleted_blobs": reconciliation_deleted_blobs,
            },
        )
        return 0
    return 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env, trigger_next_job_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "silver-price-target-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="silver", domain="price-target", job_name=job_name)
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
