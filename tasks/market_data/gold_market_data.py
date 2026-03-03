import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common import layer_bucketing
from tasks.technical_analysis.technical_indicators import (
    add_candlestick_patterns,
    add_heikin_ashi_and_ichimoku,
)
from tasks.common.silver_contracts import normalize_columns_to_snake_case
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_market_tables,
)


@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str
    max_workers: int
    tickers: Sequence[str]


def _is_truthy(raw: Optional[str], *, default: bool = False) -> bool:
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


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


def _percentile_rank_last(window: np.ndarray) -> float:
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
    out = _snake_case_columns(df)

    required = {"date", "open", "high", "low", "close", "volume", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = _coerce_datetime(out["date"])

    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    close = out["close"]
    high = out["high"]
    low = out["low"]
    volume = out["volume"]

    # Returns
    for window in (1, 5, 20, 60):
        out[f"return_{window}d"] = close.pct_change(periods=window)

    daily_return = out["return_1d"]

    # Volatility (std of daily returns)
    for window in (20, 60):
        out[f"vol_{window}d"] = daily_return.rolling(window=window, min_periods=window).std()

    # Drawdown (vs rolling max)
    out["rolling_max_252d"] = close.rolling(window=252, min_periods=1).max()
    out["drawdown_1y"] = _safe_div(close, out["rolling_max_252d"]) - 1.0

    # ATR (14d simple average)
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

    # SMAs & cross-over state flags
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

    # Compression: Bollinger width (20d, 2std) and range/close
    close_std_20 = close.rolling(window=20, min_periods=20).std()
    bb_mid_20 = out["sma_20d"]
    bb_upper_20 = bb_mid_20 + 2 * close_std_20
    bb_lower_20 = bb_mid_20 - 2 * close_std_20
    out["bb_width_20d"] = _safe_div((bb_upper_20 - bb_lower_20), bb_mid_20)
    out["range_close"] = _safe_div((high - low), close)

    # Range/Compression
    high_20 = high.rolling(window=20, min_periods=20).max()
    low_20 = low.rolling(window=20, min_periods=20).min()
    out["range_20"] = _safe_div((high_20 - low_20), close)
    out["compression_score"] = out["range_20"].rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    # Volume: z-score (20d) and percentile rank (252d)
    vol_mean_20 = volume.rolling(window=20, min_periods=20).mean()
    vol_std_20 = volume.rolling(window=20, min_periods=20).std()
    out["volume_z_20d"] = _safe_div((volume - vol_mean_20), vol_std_20)
    out["volume_pct_rank_252d"] = volume.rolling(window=252, min_periods=1).apply(_percentile_rank_last, raw=True)

    out = add_candlestick_patterns(out)
    out = add_heikin_ashi_and_ichimoku(out)

    # Internal helper columns (prefixed with "_") are implementation detail only.
    helper_cols = [col for col in out.columns if str(col).startswith("_")]
    if helper_cols:
        out = out.drop(columns=helper_cols, errors="ignore")

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
        date_col="date",
        backfill_start=backfill_start,
        context=f"gold market {ticker}",
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

    df_features = normalize_columns_to_snake_case(df_features)

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
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER")
    gold_container = os.environ.get("AZURE_CONTAINER_GOLD")

    from core import core as mdc
    from core import config as common_cfg

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    if hasattr(common_cfg, "DEBUG_SYMBOLS") and common_cfg.DEBUG_SYMBOLS:
        mdc.write_line(
            f"DEBUG MODE: Restricting execution to {len(common_cfg.DEBUG_SYMBOLS)} symbols: {common_cfg.DEBUG_SYMBOLS}"
        )
        df_symbols = df_symbols[df_symbols["Symbol"].isin(common_cfg.DEBUG_SYMBOLS)]

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        clean = symbol.replace(".", "-")
        tickers.append(clean)

    # De-duplicate while preserving order
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
    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold market reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold market reconciliation requires gold storage client.")

    silver_symbols = collect_delta_market_symbols(client=silver_client, root_prefix="market-data")
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="market")
    orphan_symbols, deleted_blobs = purge_orphan_market_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        downstream_path_builder=DataPaths.get_gold_features_path,
        delete_prefix=gold_client.delete_prefix,
    )
    if orphan_symbols:
        mdc.write_line(
            "Gold market reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Gold market reconciliation: no orphan symbols detected.")

    backfill_start, _ = get_backfill_range()
    cutoff_symbols = gold_symbols.difference(set(orphan_symbols))
    cutoff_stats = enforce_backfill_cutoff_on_tables(
        symbols=cutoff_symbols,
        table_paths_for_symbol=lambda symbol: [DataPaths.get_gold_features_path(symbol)],
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
    return len(orphan_symbols), deleted_blobs


def _run_alpha26_market_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str]]:
    from core import core as mdc
    from core.pipeline import DataPaths
    from core import delta_core

    force_rebuild = layer_bucketing.gold_alpha26_force_rebuild()
    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    failed = 0
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    symbol_to_bucket: dict[str, str] = {}
    watermarks_dirty = False

    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        silver_path = DataPaths.get_silver_market_bucket_path(bucket)
        gold_path = DataPaths.get_gold_market_bucket_path(bucket)
        watermark_key = f"bucket::{bucket}"
        silver_commit = delta_core.get_delta_last_commit(silver_container, silver_path)
        prior = watermarks.get(watermark_key, {})
        if (
            (not force_rebuild)
            and silver_commit is not None
            and prior.get("silver_last_commit") is not None
            and prior.get("silver_last_commit") >= silver_commit
        ):
            skipped_unchanged += 1
            continue

        if silver_commit is None:
            skipped_missing_source += 1
            df_gold_bucket = pd.DataFrame(columns=["date", "symbol"])
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
                            date_col="date",
                            backfill_start=backfill_start,
                            context=f"gold market alpha26 {ticker}",
                        )
                        if df_features is None or df_features.empty:
                            continue
                        symbol_frames.append(df_features)
                        symbol_to_bucket[ticker] = bucket
                    except Exception as exc:
                        failed += 1
                        mdc.write_warning(f"Gold market alpha26 compute failed for {ticker}: {exc}")
            if symbol_frames:
                df_gold_bucket = pd.concat(symbol_frames, ignore_index=True)
                df_gold_bucket = normalize_columns_to_snake_case(df_gold_bucket)
            else:
                df_gold_bucket = pd.DataFrame(columns=["date", "symbol"])

        try:
            delta_core.store_delta(df_gold_bucket, gold_container, gold_path, mode="overwrite")
            if backfill_start is not None:
                delta_core.vacuum_delta_table(
                    gold_container,
                    gold_path,
                    retention_hours=0,
                    dry_run=False,
                    enforce_retention_duration=False,
                    full=True,
                )
            processed += 1
            if silver_commit is not None:
                watermarks[watermark_key] = {
                    "silver_last_commit": silver_commit,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                watermarks_dirty = True
        except Exception as exc:
            failed += 1
            mdc.write_error(f"Gold market alpha26 write failed bucket={bucket}: {exc}")

    index_path = layer_bucketing.write_layer_symbol_index(
        layer="gold",
        domain="market",
        symbol_to_bucket=symbol_to_bucket,
    )
    return processed, skipped_unchanged, skipped_missing_source, failed, watermarks_dirty, len(symbol_to_bucket), index_path


def main() -> int:
    from core import core as mdc
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold market features: {backfill_start_iso}")
    layer_bucketing.gold_layout_mode()

    watermarks = load_watermarks("gold_market_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = _run_alpha26_market_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    if watermarks_dirty:
        save_watermarks("gold_market_features", watermarks)
    total_failed = failed
    mdc.write_line(
        "Gold market alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker
    from core import core as mdc

    job_name = "gold-market-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0 and _is_truthy(os.environ.get("GOLD_MARKET_BY_DATE_ENABLED"), default=False):
        from tasks.market_data.materialize_gold_market_by_date import main as materialize_by_date_main

        mdc.write_line("Running Gold market by-date materialization (GOLD_MARKET_BY_DATE_ENABLED=true)...")
        exit_code = materialize_by_date_main([])
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="market", job_name=job_name)
    raise SystemExit(exit_code)
