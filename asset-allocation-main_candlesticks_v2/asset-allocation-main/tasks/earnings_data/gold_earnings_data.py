import os
import re
import multiprocessing as mp
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks


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
    
    # We want to fill feature columns forward, but some might be flags (handle separately if needed)
    # For now, ffill everything, then fixing flags downstream is easier.
    out = out.ffill()
    out = out.reset_index().rename(columns={"index": date_col})
    return out


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = _snake_case_columns(df)

    required = {"date", "symbol", "reported_eps", "eps_estimate"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = _coerce_datetime(out["date"])
    out["symbol"] = out["symbol"].astype(str)

    out["reported_eps"] = pd.to_numeric(out["reported_eps"], errors="coerce")
    out["eps_estimate"] = pd.to_numeric(out["eps_estimate"], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    estimate_abs = out["eps_estimate"].abs()
    out["surprise_pct"] = _safe_div(out["reported_eps"] - out["eps_estimate"], estimate_abs)

    grouped_surprise = out.groupby("symbol", sort=False)["surprise_pct"]
    out["surprise_mean_4q"] = grouped_surprise.transform(
        lambda series: series.rolling(window=4, min_periods=4).mean()
    )
    out["surprise_std_8q"] = grouped_surprise.transform(
        lambda series: series.rolling(window=8, min_periods=8).std()
    )

    beat = (out["surprise_pct"] > 0).astype(float)
    beat = beat.where(out["surprise_pct"].notna())
    out["beat_rate_8q"] = beat.groupby(out["symbol"], sort=False).transform(
        lambda series: series.rolling(window=8, min_periods=8).mean()
    )

    # -------------------------------------------------------------------------
    # Conversion to Daily Time Series
    # -------------------------------------------------------------------------
    # 1. Mark the actual earnings day before resampling
    out["is_earnings_day"] = 1.0
    out["last_earnings_date"] = out["date"]

    # 2. Resample daily (per ticker)
    # Since we have multiple tickers mixed, apply per ticker
    out = out.groupby("symbol", sort=False, group_keys=False).apply(
        lambda x: _resample_daily_ffill(x, "date")
    ).reset_index(drop=True)

    # 3. Fix flags after ffill
    # _resample_daily_ffill ffilled 'is_earnings_day', so it's 1 everywhere after first earnings.
    # We want it to be 1 only on the original dates.
    # Logic: if date == last_earnings_date, then 1, else 0
    out["is_earnings_day"] = np.where(out["date"] == out["last_earnings_date"], 1.0, 0.0)

    # 4. Calculate days since earnings
    out["days_since_earnings"] = (out["date"] - out["last_earnings_date"]).dt.days

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _process_ticker(task: Tuple[str, str, str, str, str]) -> Dict[str, Any]:
    from core import delta_core

    ticker, raw_path, gold_path, silver_container, gold_container = task

    df_raw = delta_core.load_delta(silver_container, raw_path)
    if df_raw is None or df_raw.empty:
        return {"ticker": ticker, "status": "skipped_no_data", "raw_path": raw_path}

    try:
        df_features = compute_features(df_raw)
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_compute", "raw_path": raw_path, "error": str(exc)}

    try:
        delta_core.store_delta(df_features, gold_container, gold_path, mode="overwrite")
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

    from core import config as cfg
    from core import core as mdc

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    if hasattr(cfg, "DEBUG_SYMBOLS") and cfg.DEBUG_SYMBOLS:
        mdc.write_line(
            f"DEBUG MODE: Restricting execution to {len(cfg.DEBUG_SYMBOLS)} symbols: {cfg.DEBUG_SYMBOLS}"
        )
        df_symbols = df_symbols[df_symbols["Symbol"].isin(cfg.DEBUG_SYMBOLS)]

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    tickers = list(dict.fromkeys(tickers))

    max_workers = _get_max_workers()
    mdc.write_line(f"Earnings feature engineering configured for {len(tickers)} tickers (max_workers={max_workers})")

    return FeatureJobConfig(
        silver_container=silver_container,
        gold_container=gold_container,
        max_workers=max_workers,
        tickers=tickers,
    )


def main() -> int:
    from core import core as mdc
    from core.pipeline import DataPaths
    from core import delta_core

    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()

    watermarks = load_watermarks("gold_earnings_features")
    watermarks_dirty = False

    tasks = []
    commit_map: Dict[str, float | None] = {}
    skipped_unchanged = 0
    for ticker in job_cfg.tickers:
        raw_path = DataPaths.get_earnings_path(ticker)
        gold_path = DataPaths.get_gold_earnings_path(ticker)
        silver_commit = delta_core.get_delta_last_commit(job_cfg.silver_container, raw_path)
        commit_map[ticker] = silver_commit

        if watermarks is not None and silver_commit is not None:
            prior = watermarks.get(ticker, {})
            if prior.get("silver_last_commit") is not None and prior.get("silver_last_commit") >= silver_commit:
                skipped_unchanged += 1
                continue

        tasks.append((ticker, raw_path, gold_path, job_cfg.silver_container, job_cfg.gold_container))

    mp_context = mp.get_context("spawn")
    results: List[Dict[str, Any]] = []

    mdc.write_line("Starting earnings feature engineering pool...")
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
    mdc.write_line(
        f"Earnings feature engineering complete: ok={ok}, skipped_no_data={skipped}, "
        f"skipped_unchanged={skipped_unchanged}, failed={failed}"
    )
    if watermarks is not None:
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
            save_watermarks("gold_earnings_features", watermarks)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.earnings_data.materialize_gold_earnings_by_date import (
        discover_year_months_from_data,
        main as by_date_main,
    )

    job_name = "gold-earnings-job"
    raise SystemExit(
        run_partner_then_by_date(
            job_name=job_name,
            partner_main=main,
            by_date_main=by_date_main,
            year_months_provider=discover_year_months_from_data,
        )
    )
