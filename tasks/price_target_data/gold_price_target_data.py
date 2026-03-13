import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.delta_write_policy import prepare_delta_write_frame
from tasks.common.gold_output_contracts import project_gold_output_frame
from tasks.common import domain_artifacts
from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common import layer_bucketing
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)
from tasks.common.postgres_gold_sync import (
    bucket_sync_is_current,
    load_domain_sync_state,
    resolve_postgres_dsn,
    sync_gold_bucket,
    sync_state_cache_entry,
)


@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str
    max_workers: int
    tickers: Sequence[str]


@dataclass(frozen=True)
class BucketExecutionResult:
    bucket: str
    status: str
    symbols_written: int
    watermark_updated: bool


def _load_existing_gold_price_target_symbol_to_bucket_map() -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="price-target")
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out

    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    for _, row in existing.iterrows():
        symbol = str(row.get("symbol") or "").strip().upper()
        bucket = str(row.get("bucket") or "").strip().upper()
        if not symbol or bucket not in valid_buckets:
            continue
        out[symbol] = bucket
    return out


def _merge_symbol_to_bucket_map(
    existing: dict[str, str],
    *,
    touched_bucket: str,
    touched_symbol_to_bucket: dict[str, str],
) -> dict[str, str]:
    out = {symbol: bucket for symbol, bucket in existing.items() if bucket != touched_bucket}
    out.update(touched_symbol_to_bucket)
    return out


def _coerce_datetime(series: pd.Series) -> pd.Series:
    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


_SNAKE_CASE_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake_case(value: Any) -> str:
    text = str(value).strip()
    if not text:
        return "col"

    text = _SNAKE_CASE_CAMEL_1.sub(r"\1_\2", text)
    text = _SNAKE_CASE_CAMEL_2.sub(r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return text or "col"


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    names = [_to_snake_case(col) for col in out.columns]

    seen: Dict[str, int] = {}
    unique: List[str] = []
    for name in names:
        count = seen.get(name, 0) + 1
        seen[name] = count
        unique.append(name if count == 1 else f"{name}_{count}")

    out.columns = unique
    return out


def _resample_daily_ffill(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if date_col not in df.columns:
        return df

    out = df.copy()
    out[date_col] = _coerce_datetime(out[date_col])
    out = out.dropna(subset=[date_col]).copy()
    if out.empty:
        return out

    out = out.sort_values(date_col).copy()
    out = out.drop_duplicates(subset=[date_col], keep="last").copy()

    out = out.set_index(date_col)
    full_range = pd.date_range(start=out.index.min(), end=out.index.max(), freq="D")
    out = out.reindex(full_range)
    out = out.ffill()
    out = out.reset_index().rename(columns={"index": date_col})
    return out


def _rolling_slope_fixed_window(values: pd.Series, window: int) -> pd.Series:
    y = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    if y.size < window:
        return pd.Series(np.nan, index=values.index)

    x = np.arange(window, dtype=float)
    sum_x = float(x.sum())
    sum_x2 = float((x * x).sum())
    denom = window * sum_x2 - sum_x * sum_x
    if denom == 0:
        return pd.Series(np.nan, index=values.index)

    finite = np.isfinite(y)
    y_zero = np.where(finite, y, 0.0)
    ones = np.ones(window, dtype=float)

    sum_y = np.correlate(y_zero, ones, mode="valid")
    sum_xy = np.correlate(y_zero, x, mode="valid")
    count = np.correlate(finite.astype(float), ones, mode="valid")

    slope = (window * sum_xy - sum_x * sum_y) / denom
    slope = np.where(count == window, slope, np.nan)

    out = np.full(y.shape, np.nan, dtype=float)
    out[window - 1 :] = slope
    return pd.Series(out, index=values.index)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = _snake_case_columns(df)

    required = {
        "symbol",
        "obs_date",
        "tp_mean_est",
        "tp_std_dev_est",
        "tp_high_est",
        "tp_low_est",
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
    }
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["obs_date"] = _coerce_datetime(out["obs_date"])
    out["symbol"] = out["symbol"].astype(str)

    numeric_cols = [
        "tp_mean_est",
        "tp_std_dev_est",
        "tp_high_est",
        "tp_low_est",
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["obs_date"]).sort_values(["symbol", "obs_date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "obs_date"], keep="last").reset_index(drop=True)

    out = _resample_daily_ffill(out, "obs_date")
    out = out.dropna(subset=["obs_date"]).sort_values(["symbol", "obs_date"]).reset_index(drop=True)

    tp_mean = out["tp_mean_est"]
    tp_std = out["tp_std_dev_est"]
    tp_high = out["tp_high_est"]
    tp_low = out["tp_low_est"]

    disp_abs = tp_high - tp_low
    out["disp_abs"] = disp_abs
    out["disp_norm"] = _safe_div(disp_abs, tp_mean)
    out["disp_std_norm"] = _safe_div(tp_std, tp_mean)

    rev_up = out["tp_cnt_est_rev_up"]
    rev_down = out["tp_cnt_est_rev_down"]
    rev_net = rev_up - rev_down
    out["rev_net"] = rev_net
    out["rev_ratio"] = _safe_div(rev_up + 1.0, rev_down + 1.0)
    out["rev_intensity"] = _safe_div(rev_net, out["tp_cnt_est"])

    out["disp_norm_change_30d"] = out["disp_norm"] - out["disp_norm"].shift(30)
    out["tp_mean_change_30d"] = out["tp_mean_est"] - out["tp_mean_est"].shift(30)

    # Dispersion Z-Score (252d)
    disp_norm_mean_252 = out["disp_norm"].rolling(window=252, min_periods=252).mean()
    disp_norm_std_252 = out["disp_norm"].rolling(window=252, min_periods=252).std()
    out["disp_z"] = _safe_div(out["disp_norm"] - disp_norm_mean_252, disp_norm_std_252)

    out["tp_mean_slope_90d"] = _rolling_slope_fixed_window(out["tp_mean_est"], window=90)

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _process_ticker(task: Tuple[str, str, str, str, str, Optional[str]]) -> Dict[str, Any]:
    from core import core as mdc
    from core import delta_core

    ticker, raw_path, gold_path, silver_container, gold_container, backfill_start_iso = task

    df_raw = delta_core.load_delta(silver_container, raw_path)
    if df_raw is None or df_raw.empty:
        return {"ticker": ticker, "status": "skipped_no_data", "raw_path": raw_path}

    try:
        df_features = compute_features(df_raw)
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_compute", "raw_path": raw_path, "error": str(exc)}

    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    df_features, _ = apply_backfill_start_cutoff(
        df_features,
        date_col="obs_date",
        backfill_start=backfill_start,
        context=f"gold price-target {ticker}",
    )

    if backfill_start is not None and df_features.empty:
        gold_client = mdc.get_storage_client(gold_container)
        if gold_client is None:
            return {
                "ticker": ticker,
                "status": "failed_write",
                "gold_path": gold_path,
                "error": f"Storage client unavailable for cutoff purge {gold_path}.",
            }
        deleted = gold_client.delete_prefix(gold_path)
        return {
            "ticker": ticker,
            "status": "ok",
            "rows": 0,
            "gold_path": gold_path,
            "purged_blobs": deleted,
        }

    df_features = project_gold_output_frame(df_features, domain="price-target")

    try:
        delta_core.store_delta(df_features, gold_container, gold_path, mode="overwrite")
        if backfill_start is not None:
            delta_core.vacuum_delta_table(
                gold_container,
                gold_path,
                retention_hours=0,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_write", "gold_path": gold_path, "error": str(exc)}

    return {"ticker": ticker, "status": "ok", "rows": len(df_features), "gold_path": gold_path}


def _get_max_workers() -> int:
    try:
        available_cpus = len(os.sched_getaffinity(0))  # type: ignore[attr-defined]
    except Exception:
        available_cpus = os.cpu_count() or 1

    default_workers = available_cpus if available_cpus <= 2 else available_cpus - 1

    configured = os.environ.get("FEATURE_ENGINEERING_MAX_WORKERS")
    if configured:
        try:
            parsed = int(configured)
            if parsed > 0:
                return min(parsed, available_cpus)
        except ValueError:
            pass
    return max(1, default_workers)


def _build_job_config() -> FeatureJobConfig:
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER") or "silver"
    gold_container = os.environ.get("AZURE_CONTAINER_GOLD") or "gold"

    from core import core as mdc
    from core import config as common_cfg

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    if hasattr(common_cfg, "DEBUG_SYMBOLS") and common_cfg.DEBUG_SYMBOLS:
        mdc.write_line(
            f"DEBUG MODE: Restricting execution to {len(common_cfg.DEBUG_SYMBOLS)} symbols: {common_cfg.DEBUG_SYMBOLS}"
        )
        df_symbols = df_symbols[df_symbols["Symbol"].isin(common_cfg.DEBUG_SYMBOLS)]

    tickers = [str(symbol).strip() for symbol in df_symbols["Symbol"].tolist() if str(symbol).strip()]
    tickers = list(dict.fromkeys(tickers))

    max_workers = _get_max_workers()
    mdc.write_line(f"Price target feature engineering configured for {len(tickers)} tickers (max_workers={max_workers})")

    return FeatureJobConfig(
        silver_container=silver_container,
        gold_container=gold_container,
        max_workers=max_workers,
        tickers=tickers,
    )


def _run_price_target_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold price-target reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold price-target reconciliation requires gold storage client.")

    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="price-target-data")
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="targets")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_price_targets_bucket_path(layer_bucketing.bucket_letter(symbol))
        ],
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            gold_container,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    deleted_blobs = purge_stats.deleted_blobs
    if orphan_symbols:
        mdc.write_line(
            "Gold price-target reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold price-target reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold price-target orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_gold_bucket_paths(domain="price-target"),
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("obs_date", "date", "Date"),
        backfill_start=backfill_start,
        context="gold price-target reconciliation cutoff",
        vacuum_table=lambda path: delta_core.vacuum_delta_table(
            gold_container,
            path,
            retention_hours=0,
            dry_run=False,
            enforce_retention_duration=False,
            full=True,
        ),
    )
    if cutoff_stats.rows_dropped > 0 or cutoff_stats.tables_rewritten > 0 or cutoff_stats.deleted_blobs > 0:
        mdc.write_line(
            "Gold price-target reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(
            f"Gold price-target reconciliation cutoff sweep encountered errors={cutoff_stats.errors}."
        )
    return len(orphan_symbols), deleted_blobs


def _run_alpha26_price_target_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str]]:
    from core import core as mdc
    from core.pipeline import DataPaths
    from core import delta_core

    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    failed = 0
    watermarks_dirty = False
    symbol_to_bucket = _load_existing_gold_price_target_symbol_to_bucket_map()
    postgres_dsn = resolve_postgres_dsn()
    sync_state = load_domain_sync_state(postgres_dsn, domain="price-target") if postgres_dsn else {}
    pending_watermark_updates: dict[str, dict[str, Any]] = {}
    bucket_results: list[BucketExecutionResult] = []

    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_price_target_bucket_path(bucket)
        gold_path = DataPaths.get_gold_price_targets_bucket_path(bucket)
        watermark_key = f"bucket::{bucket}"
        silver_commit = delta_core.get_delta_last_commit(silver_container, silver_path)
        gold_commit = delta_core.get_delta_last_commit(gold_container, gold_path)
        prior = watermarks.get(watermark_key, {})
        postgres_sync_current = (
            bucket_sync_is_current(sync_state, bucket=bucket, source_commit=silver_commit)
            if postgres_dsn
            else True
        )
        if (
            silver_commit is not None
            and prior.get("silver_last_commit") is not None
            and prior.get("silver_last_commit") >= silver_commit
            and gold_commit is not None
            and postgres_sync_current
        ):
            skipped_unchanged += 1
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="skipped_unchanged",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            continue

        prior_bucket_symbols = sorted(
            symbol for symbol, current_bucket in symbol_to_bucket.items() if current_bucket == bucket
        )
        bucket_symbol_to_bucket: dict[str, str] = {}
        bucket_symbol_failures = 0
        if silver_commit is None:
            skipped_missing_source += 1
            df_gold_bucket = project_gold_output_frame(
                pd.DataFrame(columns=["obs_date", "symbol"]),
                domain="price-target",
            )
        else:
            df_silver_bucket = delta_core.load_delta(silver_container, silver_path)
            symbol_frames: list[pd.DataFrame] = []
            if df_silver_bucket is not None and not df_silver_bucket.empty and "symbol" in df_silver_bucket.columns:
                for symbol, group in df_silver_bucket.groupby("symbol"):
                    ticker = str(symbol or "").strip().upper()
                    if not ticker:
                        continue
                    try:
                        df_features = compute_features(group.copy())
                        df_features, _ = apply_backfill_start_cutoff(
                            df_features,
                            date_col="obs_date",
                            backfill_start=backfill_start,
                            context=f"gold price-target alpha26 {ticker}",
                        )
                        if df_features is None or df_features.empty:
                            continue
                        symbol_frames.append(df_features)
                        bucket_symbol_to_bucket[ticker] = bucket
                    except Exception as exc:
                        failed += 1
                        bucket_symbol_failures += 1
                        mdc.write_warning(f"Gold price-target alpha26 compute failed for {ticker}: {exc}")
            if symbol_frames:
                df_gold_bucket = project_gold_output_frame(
                    pd.concat(symbol_frames, ignore_index=True),
                    domain="price-target",
                )
            else:
                df_gold_bucket = project_gold_output_frame(
                    pd.DataFrame(columns=["obs_date", "symbol"]),
                    domain="price-target",
                )

        write_decision = prepare_delta_write_frame(
            df_gold_bucket.reset_index(drop=True),
            container=gold_container,
            path=gold_path,
        )
        mdc.write_line(
            "delta_write_decision layer=gold domain=price-target "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={gold_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(
                f"Skipping Gold price-target empty bucket write for {gold_path}: no existing Delta schema."
            )
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=price-target bucket={bucket} "
                "status=blocked reason=empty_bucket_no_existing_schema"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="skipped_empty_no_schema",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )
            continue
        try:
            delta_core.store_delta(write_decision.frame, gold_container, gold_path, mode="overwrite")
            if backfill_start is not None:
                delta_core.vacuum_delta_table(
                    gold_container,
                    gold_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
            try:
                domain_artifacts.write_bucket_artifact(
                    layer="gold",
                    domain="price-target",
                    bucket=bucket,
                    df=write_decision.frame,
                    date_column="obs_date",
                    job_name="gold-price-target-job",
                )
            except Exception as exc:
                mdc.write_warning(f"Gold price-target metadata bucket artifact write failed bucket={bucket}: {exc}")
            if postgres_dsn:
                sync_result = sync_gold_bucket(
                    domain="price-target",
                    bucket=bucket,
                    frame=write_decision.frame,
                    scope_symbols=sorted(set(prior_bucket_symbols).union(bucket_symbol_to_bucket.keys())),
                    source_commit=silver_commit,
                    dsn=postgres_dsn,
                )
                sync_state[bucket] = sync_state_cache_entry(sync_result)
                mdc.write_line(
                    "postgres_gold_sync_status "
                    f"domain=price-target bucket={bucket} status={sync_result.status} "
                    f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                    f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                )
            processed += 1
            symbol_to_bucket = _merge_symbol_to_bucket_map(
                symbol_to_bucket,
                touched_bucket=bucket,
                touched_symbol_to_bucket=bucket_symbol_to_bucket,
            )
            watermark_updated = False
            if silver_commit is not None:
                pending_watermark_updates[watermark_key] = {
                    "silver_last_commit": silver_commit,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                watermark_updated = True
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=price-target bucket={bucket} status=updated reason=success"
                )
            else:
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=price-target bucket={bucket} "
                    "status=blocked reason=missing_source_commit"
                )
            symbols_written = len(bucket_symbol_to_bucket)
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=ok bucket={bucket} "
                f"symbols_in={symbols_written + bucket_symbol_failures} "
                f"symbols_out={symbols_written} failures={bucket_symbol_failures}"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="ok" if bucket_symbol_failures == 0 else "ok_with_failures",
                    symbols_written=symbols_written,
                    watermark_updated=watermark_updated,
                )
            )
        except Exception as exc:
            failed += 1
            mdc.write_error(f"Gold price-target alpha26 write failed bucket={bucket}: {exc}")
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                f"reason=write_failure symbols_in={len(bucket_symbol_to_bucket)} symbols_out=0 "
                f"failures={bucket_symbol_failures + 1}"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=price-target bucket={bucket} "
                "status=blocked reason=write_failure"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="failed_write",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )

    status_counts: dict[str, int] = {}
    for result in bucket_results:
        status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
    mdc.write_line(
        "layer_handoff_status transition=silver_to_gold status=complete "
        f"bucket_statuses={status_counts} failed={failed}"
    )

    index_path: Optional[str] = None
    publication_reason: Optional[str] = None
    if failed > 0:
        publication_reason = "failed_buckets"
    else:
        try:
            index_path = layer_bucketing.write_layer_symbol_index(
                layer="gold",
                domain="price-target",
                symbol_to_bucket=symbol_to_bucket,
            )
        except Exception as exc:
            failed += 1
            publication_reason = "index_write_failed"
            mdc.write_error(f"Gold price-target symbol index write failed: {exc}")

        if index_path:
            try:
                domain_artifacts.write_domain_artifact(
                    layer="gold",
                    domain="price-target",
                    date_column="obs_date",
                    symbol_count_override=len(symbol_to_bucket),
                    symbol_index_path=index_path,
                    job_name="gold-price-target-job",
                )
            except Exception as exc:
                mdc.write_warning(f"Gold price-target metadata artifact write failed: {exc}")
            if pending_watermark_updates:
                watermarks.update(pending_watermark_updates)
                watermarks_dirty = True
        elif publication_reason is None:
            publication_reason = "index_unavailable"

    if publication_reason is None:
        mdc.write_line(
            "artifact_publication_status "
            f"layer=gold domain=price-target status=published buckets_ok={processed}"
        )
    else:
        mdc.write_line(
            "artifact_publication_status "
            f"layer=gold domain=price-target status=blocked reason={publication_reason} "
            f"failed={failed} processed={processed}"
        )
    return processed, skipped_unchanged, skipped_missing_source, failed, watermarks_dirty, len(symbol_to_bucket), index_path


def main() -> int:
    from core import core as mdc
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold price-target features: {backfill_start_iso}")
    layer_bucketing.gold_layout_mode()

    watermarks = load_watermarks("gold_price_target_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = _run_alpha26_price_target_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    if watermarks_dirty:
        save_watermarks("gold_price_target_features", watermarks)
    total_failed = failed
    mdc.write_line(
        "Gold price-target alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.job_entrypoint import run_logged_job
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-price-target-job"
    ensure_api_awake_from_env(required=True)
    raise SystemExit(
        run_logged_job(
            job_name=job_name,
            run=main,
            on_success=(lambda: write_system_health_marker(layer="gold", domain="price-target", job_name=job_name),),
        )
    )
