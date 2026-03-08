"""Gold market feature engineering job.

This module reads silver-layer market bars, computes gold-layer technical
features, and writes bucketed Delta tables for downstream consumers.

Execution flow:
1. `main()` loads diagnostics, runtime config, and backfill settings.
2. `_run_alpha26_market_gold()` iterates alphabet buckets and symbols.
3. `compute_features()` derives technical indicators from OHLCV bars.
4. Bucket tables are written to gold storage and watermarks are updated.
5. Health marker updates run at exit.
"""

import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common import domain_artifacts
from tasks.common import layer_bucketing
from tasks.technical_analysis.technical_indicators import (
    add_candlestick_patterns,
    add_heikin_ashi_and_ichimoku,
)
from tasks.common.silver_contracts import normalize_columns_to_snake_case
from tasks.common.delta_write_policy import prepare_delta_write_frame
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
    """Runtime configuration needed to execute the gold market job."""

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


_SILVER_TO_GOLD_REQUIRED_COLUMNS = {
    "date",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
}


def _coerce_datetime(series: pd.Series) -> pd.Series:
    """Parse a series to datetimes and normalize timezone-aware values to naive."""

    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Divide two series while preserving NaN when denominator values are zero."""

    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


_SNAKE_CASE_CAMEL_1 = re.compile(r"(.)([A-Z][a-z]+)")
_SNAKE_CASE_CAMEL_2 = re.compile(r"([a-z0-9])([A-Z])")


def _to_snake_case(value: Any) -> str:
    """Normalize a column-like value into a stable snake_case identifier."""

    text = str(value).strip()
    if not text:
        return "col"

    text = _SNAKE_CASE_CAMEL_1.sub(r"\1_\2", text)
    text = _SNAKE_CASE_CAMEL_2.sub(r"\1_\2", text)
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_").lower()
    return text or "col"


def _snake_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with snake_case and de-duplicated column names."""

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


def _percentile_rank_last(window: np.ndarray) -> float:
    """Return percentile rank of the window's last value within valid samples."""

    if window.size == 0:
        return np.nan
    last = window[-1]
    if np.isnan(last):
        return np.nan
    valid = window[~np.isnan(window)]
    if valid.size == 0:
        return np.nan
    return float((valid <= last).sum() / valid.size)


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute gold-layer technical features from OHLCV market rows.

    Required columns after normalization:
    - date, open, high, low, close, volume, symbol

    Output includes return, volatility, drawdown, ATR/gap, moving-average trend,
    range/compression, volume context, and candlestick/Ichimoku features.
    """

    # Normalize schema once so the rest of the function can use fixed names.
    out = _snake_case_columns(df)

    required = {"date", "open", "high", "low", "close", "volume", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")
    symbols = out["symbol"].astype("string").str.strip().str.upper().replace("", pd.NA).dropna().unique().tolist()
    if len(symbols) > 1:
        raise ValueError(f"compute_features expects single-symbol input; received symbols={sorted(symbols)}")

    # Coerce input types early. Invalid values become NaN and are handled later.
    out["date"] = _coerce_datetime(out["date"])

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Keep series math deterministic by sorting and removing duplicate bars.
    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]

    # Returns over multiple lookback windows.
    for window in (1, 5, 20, 60):
        out[f"return_{window}d"] = close.pct_change(periods=window)

    daily_return = out["return_1d"]

    # Volatility measured as rolling standard deviation of daily return.
    for window in (20, 60):
        out[f"vol_{window}d"] = daily_return.rolling(window=window, min_periods=window).std()

    # Drawdown relative to rolling 1-year high.
    out["rolling_max_252d"] = close.rolling(window=252, min_periods=1).max()
    out["drawdown_1y"] = _safe_div(close, out["rolling_max_252d"]) - 1.0

    # ATR (14-day simple average true range) and normalized opening gap.
    prev_close = close.shift(1)
    true_range_components = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    )
    out["true_range"] = true_range_components.max(axis=1)
    out["atr_14d"] = out["true_range"].rolling(window=14, min_periods=14).mean()
    out["gap_atr"] = _safe_div((out["open"] - prev_close).abs(), out["atr_14d"])

    # Moving-average trend state and crossover event flags.
    for window in (20, 50, 200):
        out[f"sma_{window}d"] = close.rolling(window=window, min_periods=window).mean()

    out["sma_20_gt_sma_50"] = (out["sma_20d"] > out["sma_50d"]).astype(int)
    out["sma_50_gt_sma_200"] = (out["sma_50d"] > out["sma_200d"]).astype(int)
    out["trend_50_200"] = _safe_div(out["sma_50d"], out["sma_200d"]) - 1.0
    out["above_sma_50"] = (close > out["sma_50d"]).astype(int)

    out["sma_20_crosses_above_sma_50"] = (out["sma_20_gt_sma_50"].diff() == 1).astype(int)
    out["sma_20_crosses_below_sma_50"] = (out["sma_20_gt_sma_50"].diff() == -1).astype(int)
    out["sma_50_crosses_above_sma_200"] = (out["sma_50_gt_sma_200"].diff() == 1).astype(int)
    out["sma_50_crosses_below_sma_200"] = (out["sma_50_gt_sma_200"].diff() == -1).astype(int)

    # Compression context from Bollinger-band width and intraday range.
    close_std_20 = close.rolling(window=20, min_periods=20).std()
    bb_mid_20 = out["sma_20d"]
    bb_upper_20 = bb_mid_20 + 2 * close_std_20
    bb_lower_20 = bb_mid_20 - 2 * close_std_20
    out["bb_width_20d"] = _safe_div((bb_upper_20 - bb_lower_20), bb_mid_20)
    out["range_close"] = _safe_div((high - low), close)

    # Additional range-compression score as 1-year percentile rank.
    high_20 = high.rolling(window=20, min_periods=20).max()
    low_20 = low.rolling(window=20, min_periods=20).min()
    out["range_20"] = _safe_div((high_20 - low_20), close)
    out["compression_score"] = out["range_20"].rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    # Volume context from short-window z-score and long-window percentile rank.
    vol_mean_20 = volume.rolling(window=20, min_periods=20).mean()
    vol_std_20 = volume.rolling(window=20, min_periods=20).std()
    out["volume_z_20d"] = _safe_div((volume - vol_mean_20), vol_std_20)
    out["volume_pct_rank_252d"] = volume.rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    # Shared TA enrichments are centralized in `tasks.technical_analysis`.
    out = add_candlestick_patterns(out)
    out = add_heikin_ashi_and_ichimoku(out)

    # Internal helper columns (prefixed with "_") are implementation detail only.
    helper_cols = [col for col in out.columns if str(col).startswith("_")]
    if helper_cols:
        out = out.drop(columns=helper_cols, errors="ignore")

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _process_ticker(task: Tuple[str, str, str, str, str, Optional[str]]) -> Dict[str, Any]:
    """Compute and write features for a single ticker table path.

    This helper supports per-ticker processing. The current `main()` path uses
    `_run_alpha26_market_gold()` for bucket-based execution.
    """

    from core import core as mdc
    from core import delta_core

    ticker, raw_path, gold_path, silver_container, gold_container, backfill_start_iso = task

    # Read source rows from silver.
    df_raw = delta_core.load_delta(silver_container, raw_path)
    if df_raw is None or df_raw.empty:
        return {"ticker": ticker, "status": "skipped_no_data", "raw_path": raw_path}

    # Isolate compute failures to the current ticker.
    try:
        df_features = compute_features(df_raw)
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_compute", "raw_path": raw_path, "error": str(exc)}

    # Optionally drop rows before the configured backfill cutoff date.
    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    df_features, _ = apply_backfill_start_cutoff(
        df_features,
        date_col="date",
        backfill_start=backfill_start,
        context=f"gold market {ticker}",
    )

    # If cutoff removes all rows, purge stale output to prevent ghost data.
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

    # Persist canonical snake_case names in the gold layer.
    df_features = normalize_columns_to_snake_case(df_features)

    # Overwrite to keep output fully derived from current source inputs.
    try:
        delta_core.store_delta(
            df_features,
            gold_container,
            gold_path,
            mode="overwrite",
        )
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
    """Pick worker count from CPU affinity/capacity with optional env override."""

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
    """Build runtime configuration and the active ticker universe."""

    silver_container = os.environ.get("AZURE_CONTAINER_SILVER")
    gold_container = os.environ.get("AZURE_CONTAINER_GOLD")

    from core import core as mdc
    from core import config as common_cfg

    # Load the symbol universe from shared core metadata.
    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    # Optional debug mode limits execution to an explicit symbol allow-list.
    if hasattr(common_cfg, "DEBUG_SYMBOLS") and common_cfg.DEBUG_SYMBOLS:
        mdc.write_line(
            f"DEBUG MODE: Restricting execution to {len(common_cfg.DEBUG_SYMBOLS)} symbols: {common_cfg.DEBUG_SYMBOLS}"
        )
        df_symbols = df_symbols[df_symbols["Symbol"].isin(common_cfg.DEBUG_SYMBOLS)]

    # Normalize symbols for storage/path safety and skip unsupported dotted symbols.
    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        clean = symbol.replace(".", "-")
        tickers.append(clean)

    # De-duplicate while preserving order.
    tickers = list(dict.fromkeys(tickers))

    max_workers = _get_max_workers()
    mdc.write_line(f"Feature engineering configured for {len(tickers)} tickers (max_workers={max_workers})")

    return FeatureJobConfig(
        silver_container=silver_container,
        gold_container=gold_container,
        max_workers=max_workers,
        tickers=tickers,
    )


def _run_market_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    """Reconcile gold market tables with silver source symbols and backfill policy.

    Returns:
    - orphan symbol count
    - number of blobs deleted while purging orphans
    """

    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold market reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold market reconciliation requires gold storage client.")

    # Discover symbol sets directly from Delta table prefixes.
    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="market-data")
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="market")

    # Remove gold tables that no longer have an upstream silver source.
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_market_bucket_path(layer_bucketing.bucket_letter(symbol))
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
            "Gold market reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold market reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold market orphan purge encountered errors={purge_stats.errors}.")

    # Apply the same backfill cutoff policy used by active processing.
    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=layer_bucketing.all_gold_bucket_paths(domain="market"),
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="gold market reconciliation cutoff",
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
            "Gold market reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Gold market reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    status = "failed" if cutoff_stats.errors > 0 else "ok"
    mdc.write_line(
        "reconciliation_result layer=gold domain=market "
        f"status={status} orphan_count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
        f"cutoff_rows_dropped={cutoff_stats.rows_dropped} cutoff_tables_rewritten={cutoff_stats.tables_rewritten} "
        f"cutoff_errors={cutoff_stats.errors}"
    )
    return len(orphan_symbols), deleted_blobs


def _load_existing_gold_symbol_to_bucket_map() -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="market")
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


def _validate_silver_to_gold_market_bucket_contract(
    df_silver_bucket: pd.DataFrame,
    *,
    bucket: str,
) -> pd.DataFrame:
    if df_silver_bucket is None:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: source frame is None.")

    normalized = normalize_columns_to_snake_case(df_silver_bucket.copy())
    missing = sorted(_SILVER_TO_GOLD_REQUIRED_COLUMNS.difference(set(normalized.columns)))
    if missing:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: missing required columns={missing}")

    if normalized.empty:
        return normalized

    parsed_dates = pd.to_datetime(normalized["date"], errors="coerce").dropna()
    if parsed_dates.empty:
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: no parseable date values.")

    symbols = normalized["symbol"].astype("string").str.strip().str.upper()
    if symbols.empty or symbols.eq("").all():
        raise ValueError(f"silver_to_gold contract violation for bucket={bucket}: no non-empty symbols.")

    return normalized


def _run_alpha26_market_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str], list[BucketExecutionResult]]:
    """Build and write bucketed gold market tables from silver alpha26 inputs.

    Processing model:
    - Iterate alphabetical buckets from `layer_bucketing.ALPHABET_BUCKETS`.
    - Skip unchanged buckets via commit watermarks unless force-rebuild is enabled.
    - Compute features for each symbol, then write one consolidated Delta table per bucket.

    Returns:
    - processed bucket count
    - skipped unchanged bucket count
    - skipped missing-source bucket count
    - failure count
    - watermark dirty flag
    - indexed symbol count
    - symbol index path (if available)
    """

    from core import core as mdc
    from core.pipeline import DataPaths
    from core import delta_core

    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None

    # Track per-run outcomes for caller status and logging.
    failed = 0
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    symbol_to_bucket = _load_existing_gold_symbol_to_bucket_map()
    postgres_dsn = resolve_postgres_dsn()
    sync_state = load_domain_sync_state(postgres_dsn, domain="market") if postgres_dsn else {}
    pending_watermark_updates: dict[str, dict[str, Any]] = {}
    watermarks_dirty = False
    bucket_results: list[BucketExecutionResult] = []

    # Each bucket maps to one silver source table and one gold destination table.
    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_market_bucket_path(bucket)
        gold_path = DataPaths.get_gold_market_bucket_path(bucket)
        watermark_key = f"bucket::{bucket}"
        silver_commit = delta_core.get_delta_last_commit(silver_container, silver_path)
        gold_commit = delta_core.get_delta_last_commit(gold_container, gold_path)
        prior = watermarks.get(watermark_key, {})
        postgres_sync_current = (
            bucket_sync_is_current(sync_state, bucket=bucket, source_commit=silver_commit) if postgres_dsn else True
        )

        # Skip stable buckets to reduce compute/write overhead on no-change runs.
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
        bucket_compute_failed = False

        # Missing source still writes an empty table to keep state deterministic.
        if silver_commit is None:
            skipped_missing_source += 1
            df_gold_bucket = pd.DataFrame(columns=["date", "symbol"])
        else:
            df_silver_bucket = delta_core.load_delta(silver_container, silver_path)
            symbol_frames: list[pd.DataFrame] = []

            # Compute features independently for each symbol in the bucket.
            if df_silver_bucket is not None and not df_silver_bucket.empty:
                try:
                    df_silver_bucket = _validate_silver_to_gold_market_bucket_contract(
                        df_silver_bucket,
                        bucket=bucket,
                    )
                except Exception as exc:
                    failed += 1
                    bucket_compute_failed = True
                    mdc.write_error(str(exc))
                    mdc.write_line(
                        f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                        "reason=contract_validation symbols_in=0 symbols_out=0 failures=1"
                    )
                    mdc.write_line(
                        f"watermark_update_status layer=gold domain=market bucket={bucket} "
                        "status=blocked reason=contract_validation"
                    )
                    bucket_results.append(
                        BucketExecutionResult(
                            bucket=bucket,
                            status="failed_contract",
                            symbols_written=0,
                            watermark_updated=False,
                        )
                    )
                    continue
                for symbol, group in df_silver_bucket.groupby("symbol"):
                    ticker = str(symbol or "").strip().upper()
                    if not ticker:
                        continue
                    try:
                        df_features = compute_features(group.copy())
                        df_features, _ = apply_backfill_start_cutoff(
                            df_features,
                            date_col="date",
                            backfill_start=backfill_start,
                            context=f"gold market alpha26 {ticker}",
                        )
                        if df_features is None or df_features.empty:
                            continue
                        symbol_frames.append(df_features)

                        # Persist symbol->bucket mapping for downstream index generation.
                        bucket_symbol_to_bucket[ticker] = bucket
                    except Exception as exc:
                        bucket_compute_failed = True
                        mdc.write_warning(f"Gold market alpha26 compute failed for {ticker}: {exc}")
            if bucket_compute_failed:
                failed += 1
                mdc.write_line(
                    f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                    f"reason=compute_failure symbols_in={len(bucket_symbol_to_bucket)} symbols_out=0 failures=1"
                )
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} "
                    "status=blocked reason=compute_failure"
                )
                bucket_results.append(
                    BucketExecutionResult(
                        bucket=bucket,
                        status="failed_compute",
                        symbols_written=0,
                        watermark_updated=False,
                    )
                )
                continue

            # Consolidate symbol outputs into one bucket table.
            if symbol_frames:
                df_gold_bucket = pd.concat(symbol_frames, ignore_index=True)
                df_gold_bucket = normalize_columns_to_snake_case(df_gold_bucket)
            else:
                df_gold_bucket = pd.DataFrame(columns=["date", "symbol"])

        write_decision = prepare_delta_write_frame(
            df_gold_bucket.reset_index(drop=True),
            container=gold_container,
            path=gold_path,
        )
        mdc.write_line(
            "delta_write_decision layer=gold domain=market "
            f"bucket={bucket} action={'skip' if write_decision.action == 'skip_empty_no_schema' else 'write'} "
            f"reason={write_decision.reason} path={gold_path}"
        )
        if write_decision.action == "skip_empty_no_schema":
            mdc.write_line(f"Skipping Gold market empty bucket write for {gold_path}: no existing Delta schema.")
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=skipped bucket={bucket} "
                "reason=empty_bucket_no_existing_schema symbols_in=0 symbols_out=0 failures=0"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=market bucket={bucket} "
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

        # Persist bucket output and watermark after successful write.
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
                    domain="market",
                    bucket=bucket,
                    df=write_decision.frame,
                    date_column="date",
                    job_name="gold-market-job",
                )
            except Exception as exc:
                mdc.write_warning(f"Gold market metadata bucket artifact write failed bucket={bucket}: {exc}")
            if postgres_dsn:
                sync_result = sync_gold_bucket(
                    domain="market",
                    bucket=bucket,
                    frame=write_decision.frame,
                    scope_symbols=sorted(set(prior_bucket_symbols).union(bucket_symbol_to_bucket.keys())),
                    source_commit=silver_commit,
                    dsn=postgres_dsn,
                )
                sync_state[bucket] = sync_state_cache_entry(sync_result)
                mdc.write_line(
                    "postgres_gold_sync_status "
                    f"domain=market bucket={bucket} status={sync_result.status} "
                    f"rows_out={sync_result.row_count} symbols_out={sync_result.symbol_count} "
                    f"scope_symbols={sync_result.scope_symbol_count} source_commit={silver_commit}"
                )
            processed += 1
            symbol_to_bucket = _merge_symbol_to_bucket_map(
                symbol_to_bucket,
                touched_bucket=bucket,
                touched_symbol_to_bucket=bucket_symbol_to_bucket,
            )
            symbols_written = len(bucket_symbol_to_bucket)
            watermark_updated = False
            if silver_commit is not None:
                pending_watermark_updates[watermark_key] = {
                    "silver_last_commit": silver_commit,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                watermark_updated = True
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} status=updated reason=success"
                )
            else:
                mdc.write_line(
                    f"watermark_update_status layer=gold domain=market bucket={bucket} "
                    "status=blocked reason=missing_source_commit"
                )
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=ok bucket={bucket} "
                f"symbols_in={len(bucket_symbol_to_bucket)} symbols_out={symbols_written} failures=0"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="ok",
                    symbols_written=symbols_written,
                    watermark_updated=watermark_updated,
                )
            )
        except Exception as exc:
            failed += 1
            mdc.write_error(f"Gold market alpha26 write failed bucket={bucket}: {exc}")
            mdc.write_line(
                f"layer_handoff_status transition=silver_to_gold status=failed bucket={bucket} "
                "reason=write_failure symbols_in=0 symbols_out=0 failures=1"
            )
            mdc.write_line(
                f"watermark_update_status layer=gold domain=market bucket={bucket} status=blocked reason=write_failure"
            )
            bucket_results.append(
                BucketExecutionResult(
                    bucket=bucket,
                    status="failed_write",
                    symbols_written=0,
                    watermark_updated=False,
                )
            )

    # Write symbol index so consumers can resolve symbols to bucket locations.
    index_path: Optional[str] = None
    try:
        index_path = layer_bucketing.write_layer_symbol_index(
            layer="gold",
            domain="market",
            symbol_to_bucket=symbol_to_bucket,
        )
    except Exception as exc:
        failed += 1
        mdc.write_error(f"Gold market symbol index write failed: {exc}")

    if index_path is not None:
        try:
            domain_artifacts.write_domain_artifact(
                layer="gold",
                domain="market",
                date_column="date",
                symbol_count_override=len(symbol_to_bucket),
                symbol_index_path=index_path,
                job_name="gold-market-job",
            )
        except Exception as exc:
            mdc.write_warning(f"Gold market metadata artifact write failed: {exc}")
        if pending_watermark_updates:
            watermarks.update(pending_watermark_updates)
            watermarks_dirty = True
    else:
        if pending_watermark_updates:
            mdc.write_warning(
                "Gold market symbol index unavailable; skipping watermark updates to keep index/watermark state consistent."
            )

    return (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        len(symbol_to_bucket),
        index_path,
        bucket_results,
    )


def main() -> int:
    """Run the gold market feature engineering pipeline and return process exit code."""

    from core import core as mdc

    # Emit environment diagnostics to simplify operations troubleshooting.
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold market features: {backfill_start_iso}")

    # Ensure layout mode is resolved before writing outputs.
    layer_bucketing.gold_layout_mode()

    # Watermarks make bucket processing incremental and idempotent.
    watermarks = load_watermarks("gold_market_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
        bucket_results,
    ) = _run_alpha26_market_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    status_counts: dict[str, int] = {}
    for result in bucket_results:
        status_counts[result.status] = int(status_counts.get(result.status, 0)) + 1
    mdc.write_line(
        "layer_handoff_status transition=silver_to_gold status=complete "
        f"bucket_statuses={status_counts} failed={failed}"
    )

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    if failed == 0:
        try:
            reconciliation_orphans, reconciliation_deleted_blobs = _run_market_reconciliation(
                silver_container=job_cfg.silver_container,
                gold_container=job_cfg.gold_container,
            )
        except Exception as exc:
            reconciliation_failed = 1
            mdc.write_error(f"Gold market reconciliation failed: {exc}")
            mdc.write_line(
                "reconciliation_result layer=gold domain=market "
                "status=failed orphan_count=unknown deleted_blobs=unknown cutoff_rows_dropped=unknown"
            )

    if watermarks_dirty and reconciliation_failed == 0:
        save_watermarks("gold_market_features", watermarks)

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        "Gold market alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} reconciled_orphans={reconciliation_orphans} "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-market-job"

    # Ensure the API dependency is awake before running the batch job.
    ensure_api_awake_from_env(required=True)
    exit_code = main()

    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="market", job_name=job_name)
    raise SystemExit(exit_code)
