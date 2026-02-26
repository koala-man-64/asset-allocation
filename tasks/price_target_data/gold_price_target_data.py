import os
import re
import multiprocessing as mp
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.silver_contracts import normalize_columns_to_snake_case
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_tables,
)


@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str
    max_workers: int
    tickers: Sequence[str]


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

    # Backward compatibility: older tables used `ticker` instead of `symbol`.
    if "symbol" not in out.columns and "ticker" in out.columns:
        out = out.rename(columns={"ticker": "symbol"})

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

    df_features = normalize_columns_to_snake_case(df_features)

    try:
        delta_core.store_delta(df_features, gold_container, gold_path, mode="overwrite", schema_mode="overwrite")
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
    orphan_symbols, deleted_blobs = purge_orphan_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        downstream_path_builder=DataPaths.get_gold_price_targets_path,
        delete_prefix=gold_client.delete_prefix,
    )
    if orphan_symbols:
        mdc.write_line(
            "Gold price-target reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs}"
        )
    else:
        mdc.write_line("Gold price-target reconciliation: no orphan symbols detected.")

    backfill_start, _ = get_backfill_range()
    cutoff_symbols = gold_symbols.difference(set(orphan_symbols))
    cutoff_stats = enforce_backfill_cutoff_on_tables(
        symbols=cutoff_symbols,
        table_paths_for_symbol=lambda symbol: [DataPaths.get_gold_price_targets_path(symbol)],
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


def main() -> int:
    from core import core as mdc
    from core.pipeline import DataPaths
    from core import delta_core

    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold price-target features: {backfill_start_iso}")

    watermarks = load_watermarks("gold_price_target_features")
    watermarks_dirty = False

    tasks = []
    commit_map: Dict[str, float | None] = {}
    skipped_unchanged = 0
    for ticker in job_cfg.tickers:
        raw_path = DataPaths.get_price_target_path(ticker)
        gold_path = DataPaths.get_gold_price_targets_path(ticker)
        silver_commit = delta_core.get_delta_last_commit(job_cfg.silver_container, raw_path)
        commit_map[ticker] = silver_commit

        if silver_commit is not None:
            prior = watermarks.get(ticker, {})
            if prior.get("silver_last_commit") is not None and prior.get("silver_last_commit") >= silver_commit:
                skipped_unchanged += 1
                continue

        tasks.append((ticker, raw_path, gold_path, job_cfg.silver_container, job_cfg.gold_container, backfill_start_iso))

    mp_context = mp.get_context("spawn")
    results: List[Dict[str, Any]] = []

    mdc.write_line("Starting price target feature engineering pool...")
    with ProcessPoolExecutor(max_workers=job_cfg.max_workers, mp_context=mp_context) as executor:
        futures = {executor.submit(_process_ticker, task): task[0] for task in tasks}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                mdc.write_error(f"Unhandled failure for {ticker}: {exc}")
                results.append({"ticker": ticker, "status": "failed_unhandled", "error": str(exc)})
                continue

            status = result.get("status")
            if status == "ok":
                mdc.write_line(f"[OK] {ticker}: {result.get('rows')} rows -> {result.get('gold_path')}")
            else:
                mdc.write_warning(f"[{status}] {ticker}: {result.get('error') or ''}".strip())
            results.append(result)

    ok = sum(1 for r in results if r.get("status") == "ok")
    skipped = sum(1 for r in results if r.get("status") == "skipped_no_data")
    failed = len(results) - ok - skipped
    for result in results:
        if result.get("status") != "ok":
            continue
        ticker = result.get("ticker")
        if not ticker:
            continue
        silver_commit = commit_map.get(ticker)
        if silver_commit is None:
            continue
        watermarks[ticker] = {
            "silver_last_commit": silver_commit,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        watermarks_dirty = True
    if watermarks_dirty:
        save_watermarks("gold_price_target_features", watermarks)

    reconciliation_orphans = 0
    reconciliation_deleted_blobs = 0
    reconciliation_failed = 0
    try:
        reconciliation_orphans, reconciliation_deleted_blobs = _run_price_target_reconciliation(
            silver_container=job_cfg.silver_container,
            gold_container=job_cfg.gold_container,
        )
    except Exception as exc:
        reconciliation_failed = 1
        mdc.write_error(f"Gold price-target reconciliation failed: {exc}")

    total_failed = failed + reconciliation_failed
    mdc.write_line(
        f"Price target feature engineering complete: ok={ok}, skipped_no_data={skipped}, "
        f"skipped_unchanged={skipped_unchanged}, reconciled_orphans={reconciliation_orphans}, "
        f"reconciliation_deleted_blobs={reconciliation_deleted_blobs}, failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-price-target-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="price-target", job_name=job_name)
    raise SystemExit(exit_code)
