import os
import re
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List

import numpy as np
import pandas as pd


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


def _process_ticker(task: Tuple[str, str, str, str, str]) -> Dict[str, Any]:
    from scripts.common import delta_core

    ticker, raw_path, gold_path, silver_container, gold_container = task

    df_raw = delta_core.load_delta(silver_container, raw_path)
    if df_raw is None or df_raw.empty:
        return {"ticker": ticker, "status": "skipped_no_data", "raw_path": raw_path}

    try:
        df_features = compute_features(df_raw)
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_compute", "raw_path": raw_path, "error": str(exc)}

    try:
        delta_core.store_delta(df_features, gold_container, gold_path, mode="overwrite", schema_mode="overwrite")
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

    from scripts.common import core as mdc
    from scripts.common import config as common_cfg

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


def main() -> int:
    from scripts.common import core as mdc
    from scripts.common.pipeline import DataPaths

    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()

    tasks = []
    for ticker in job_cfg.tickers:
        raw_path = DataPaths.get_price_target_path(ticker)
        gold_path = DataPaths.get_gold_price_targets_path(ticker)
        tasks.append((ticker, raw_path, gold_path, job_cfg.silver_container, job_cfg.gold_container))

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
    mdc.write_line(f"Price target feature engineering complete: ok={ok}, skipped={skipped}, failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    from scripts.common import core as mdc

    job_name = "feature-engineering-targets"
    with mdc.JobLock(job_name):
        raise SystemExit(main())
