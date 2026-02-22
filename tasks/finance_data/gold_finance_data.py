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
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.silver_contracts import normalize_columns_to_snake_case

@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str
    max_workers: int
    tickers: Sequence[str]


_NUMBER_RE = re.compile(r"^\s*([-+]?\d*\.?\d+)\s*([kKmMbBtT])?\s*$")


def _coerce_datetime(series: pd.Series) -> pd.Series:
    value = pd.to_datetime(series, errors="coerce", format="%m/%d/%Y")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    return numerator.where(denominator != 0).divide(denominator.where(denominator != 0))


def _normalize_column_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name).strip().lower())


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
 
 
def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    """
    Computes the slope of a linear regression over a rolling window.
    X is assumed to be `range(window)`.
    Slope formula: (N * sum(xy) - sum(x) * sum(y)) / (N * sum(x^2) - (sum(x))^2)
    with x = 0, 1, ..., N-1.
    """
    if len(series) < window:
        return pd.Series(np.nan, index=series.index)
 
    # Pre-calculated constants for x = 0, 1, ..., window-1
    n = window
    sum_x = (n * (n - 1)) // 2
    sum_xx = (n * (n - 1) * (2 * n - 1)) // 6
    denom = n * sum_xx - sum_x * sum_x
    
    # sum_xy is a bit trickier with rolling(). Warning: O(N*W) if done naively.
    # Convolution approach: convolve series with [0, 1, ..., window-1]
    # Pandas rolling doesn't support weighted sum directly easily without apply (slow).
    # Optimization: sum_xy[t] = sum_xy[t-1] - sum_y[t-1] + y[t]*(window-1) + dropped_y * 0 ? No.
    # Let's use stride_tricks or just apply for now as these datasets aren't huge (quarterly data).
    # Since it's quarterly data, standard apply with numpy polyfit(deg=1) might be fast enough.
    
    def slope_1d(y_window):
        if np.isnan(y_window).any():
            return np.nan
        # x is 0..window-1
        # slope = (N*sum(xy) - sum(x)sum(y)) / denom
        x = np.arange(window)
        sum_xy = np.dot(x, y_window)
        s_y = y_window.sum()
        return (n * sum_xy - sum_x * s_y) / denom

    return series.rolling(window=window, min_periods=window).apply(slope_1d, raw=True)



def _resolve_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df is None or df.empty:
        return None

    normalized_to_actual: Dict[str, str] = {}
    for col in df.columns:
        normalized_to_actual.setdefault(_normalize_column_name(col), col)

    for candidate in candidates:
        candidate_norm = _normalize_column_name(candidate)
        match = normalized_to_actual.get(candidate_norm)
        if match:
            return match

        prefix_matches = [
            (normalized, actual)
            for normalized, actual in normalized_to_actual.items()
            if normalized.startswith(candidate_norm)
        ]
        if prefix_matches:
            best = min(prefix_matches, key=lambda pair: len(pair[0]))
            return best[1]

    return None


def _parse_human_number(value: Any) -> float:
    if value is None:
        return float("nan")

    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "n/a", "na", "-", "--"}:
        return float("nan")

    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative = True
        text = text[1:-1].strip()

    percent = False
    if text.endswith("%"):
        percent = True
        text = text[:-1].strip()

    text = text.replace(",", "")
    match = _NUMBER_RE.match(text)
    if not match:
        try:
            parsed = float(text)
        except ValueError:
            return float("nan")
    else:
        parsed = float(match.group(1))
        suffix = (match.group(2) or "").lower()
        multiplier = {"k": 1e3, "m": 1e6, "b": 1e9, "t": 1e12}.get(suffix, 1.0)
        parsed *= multiplier

    if percent:
        parsed /= 100.0
    if negative:
        parsed *= -1.0
    return parsed


def _coerce_numeric(series: pd.Series) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return series.apply(_parse_human_number).astype("float64")


def _prepare_table(df: Optional[pd.DataFrame], ticker: str) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None

    out = _snake_case_columns(df)

    if "date" not in out.columns:
        return None

    out["date"] = _coerce_datetime(out["date"])
    out = out.dropna(subset=["date"]).copy()
    if out.empty:
        return None

    out["symbol"] = ticker
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)
    return out


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    out = _snake_case_columns(df)

    required = {"date", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = _coerce_datetime(out["date"])
    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    symbol_key = out["symbol"]

    revenue_col = _resolve_column(out, ["Total Revenue", "Revenue"])
    gross_profit_col = _resolve_column(out, ["Gross Profit"])
    operating_income_col = _resolve_column(out, ["Operating Income", "Operating Income or Loss"])
    net_income_col = _resolve_column(out, ["Net Income", "Net Income Common Stockholders"])
    free_cash_flow_col = _resolve_column(out, ["Free Cash Flow"])
    operating_cash_flow_col = _resolve_column(
        out,
        [
            "Operating Cash Flow",
            "Total Cash From Operating Activities",
            "Cash Flow From Continuing Operating Activities",
            "Net Cash Provided by Operating Activities",
        ],
    )

    total_debt_col = _resolve_column(out, ["Total Debt"])
    long_term_debt_col = _resolve_column(
        out,
        [
            "Long Term Debt",
            "Long Term Debt And Capital Lease Obligation",
            "Long Term Debt & Capital Lease Obligation",
            "Long-term Debt",
            "Long-Term Debt",
        ],
    )
    total_assets_col = _resolve_column(out, ["Total Assets"])
    current_assets_col = _resolve_column(out, ["Current Assets", "Total Current Assets"])
    current_liabilities_col = _resolve_column(out, ["Current Liabilities", "Total Current Liabilities"])
    shares_outstanding_col = _resolve_column(
        out,
        [
            "Shares Outstanding",
            "Common Stock Shares Outstanding",
            "Common Shares Outstanding",
            "Ordinary Shares Number",
            "Share Issued",
        ],
    )

    pe_ratio_col = _resolve_column(
        out,
        [
            "Trailing P/E",
            "PE Ratio (TTM)",
            "P/E Ratio",
            "P/E",
            "PE Ratio",
        ],
    )
    ev_ebitda_col = _resolve_column(
        out,
        [
            "Enterprise Value/EBITDA",
            "EV/EBITDA",
            "EV / EBITDA",
        ],
    )
    market_cap_col = _resolve_column(out, ["Market Cap", "Market Cap (intraday)"])
    ebitda_col = _resolve_column(out, ["EBITDA", "Normalized EBITDA"])
    forward_pe_col = _resolve_column(out, ["Forward P/E", "Forward PE"])
    ev_revenue_col = _resolve_column(
        out,
        [
            "Enterprise Value/Revenue",
            "EV/Revenue",
            "EV / Revenue",
        ],
    )

    revenue = _coerce_numeric(out[revenue_col]) if revenue_col else pd.Series(np.nan, index=out.index)
    gross_profit = _coerce_numeric(out[gross_profit_col]) if gross_profit_col else pd.Series(np.nan, index=out.index)
    operating_income = (
        _coerce_numeric(out[operating_income_col]) if operating_income_col else pd.Series(np.nan, index=out.index)
    )
    net_income = _coerce_numeric(out[net_income_col]) if net_income_col else pd.Series(np.nan, index=out.index)
    free_cash_flow = (
        _coerce_numeric(out[free_cash_flow_col]) if free_cash_flow_col else pd.Series(np.nan, index=out.index)
    )
    operating_cash_flow = (
        _coerce_numeric(out[operating_cash_flow_col]) if operating_cash_flow_col else pd.Series(np.nan, index=out.index)
    )
    ebitda = _coerce_numeric(out[ebitda_col]) if ebitda_col else pd.Series(np.nan, index=out.index)
    forward_pe = _coerce_numeric(out[forward_pe_col]) if forward_pe_col else pd.Series(np.nan, index=out.index)
    ev_revenue = _coerce_numeric(out[ev_revenue_col]) if ev_revenue_col else pd.Series(np.nan, index=out.index)

    if revenue_col:
        out[revenue_col] = revenue
    if gross_profit_col:
        out[gross_profit_col] = gross_profit
    if operating_income_col:
        out[operating_income_col] = operating_income
    if net_income_col:
        out[net_income_col] = net_income
    if free_cash_flow_col:
        out[free_cash_flow_col] = free_cash_flow
    if operating_cash_flow_col:
        out[operating_cash_flow_col] = operating_cash_flow

    total_debt = _coerce_numeric(out[total_debt_col]) if total_debt_col else pd.Series(np.nan, index=out.index)
    long_term_debt = (
        _coerce_numeric(out[long_term_debt_col])
        if long_term_debt_col
        else total_debt.copy()
    )
    total_assets = _coerce_numeric(out[total_assets_col]) if total_assets_col else pd.Series(np.nan, index=out.index)
    current_assets = (
        _coerce_numeric(out[current_assets_col]) if current_assets_col else pd.Series(np.nan, index=out.index)
    )
    current_liabilities = (
        _coerce_numeric(out[current_liabilities_col]) if current_liabilities_col else pd.Series(np.nan, index=out.index)
    )
    shares_outstanding = (
        _coerce_numeric(out[shares_outstanding_col]) if shares_outstanding_col else pd.Series(np.nan, index=out.index)
    )
    cash_and_equivalents_col = _resolve_column(
        out, 
        [
            "Cash And Cash Equivalents", 
            "Cash & Cash Equivalents",
            "Cash and Cash Equivalents"
        ]
    )
    cash_and_equivalents = (
        _coerce_numeric(out[cash_and_equivalents_col]) if cash_and_equivalents_col else pd.Series(np.nan, index=out.index)
    )

    if total_debt_col:
        out[total_debt_col] = total_debt
    if long_term_debt_col:
        out[long_term_debt_col] = long_term_debt
    if total_assets_col:
        out[total_assets_col] = total_assets
    if current_assets_col:
        out[current_assets_col] = current_assets
    if current_liabilities_col:
        out[current_liabilities_col] = current_liabilities
    if shares_outstanding_col:
        out[shares_outstanding_col] = shares_outstanding

    pe_ratio = _coerce_numeric(out[pe_ratio_col]) if pe_ratio_col else pd.Series(np.nan, index=out.index)
    ev_ebitda = _coerce_numeric(out[ev_ebitda_col]) if ev_ebitda_col else pd.Series(np.nan, index=out.index)
    market_cap = _coerce_numeric(out[market_cap_col]) if market_cap_col else pd.Series(np.nan, index=out.index)

    out["rev_qoq"] = _safe_div(revenue, revenue.groupby(symbol_key, sort=False).shift(1)) - 1.0
    out["rev_yoy"] = _safe_div(revenue, revenue.groupby(symbol_key, sort=False).shift(4)) - 1.0
    out["net_inc_yoy"] = _safe_div(net_income, net_income.groupby(symbol_key, sort=False).shift(4)) - 1.0
    out["fcf_yoy"] = _safe_div(free_cash_flow, free_cash_flow.groupby(symbol_key, sort=False).shift(4)) - 1.0
    
    # Slopes (6q window)
    out["rev_growth_slope_6q"] = revenue.groupby(symbol_key, sort=False, group_keys=False).apply(
        lambda x: _rolling_slope(x, 6)
    )
    out["fcf_slope_6q"] = free_cash_flow.groupby(symbol_key, sort=False, group_keys=False).apply(
        lambda x: _rolling_slope(x, 6)
    )

    out["gross_margin"] = _safe_div(gross_profit, revenue)
    out["op_margin"] = _safe_div(operating_income, revenue)
    out["fcf_margin"] = _safe_div(free_cash_flow, revenue)
    out["ebitda_margin"] = _safe_div(ebitda, revenue)
    
    # Margin delta QoQ (using gross margin)
    out["margin_delta_qoq"] = out["gross_margin"] - out["gross_margin"].groupby(symbol_key, sort=False).shift(1)

    out["debt_to_assets"] = _safe_div(total_debt, total_assets)
    out["current_ratio"] = _safe_div(current_assets, current_liabilities)
    out["net_debt"] = total_debt - cash_and_equivalents
    out["shares_change_yoy"] = _safe_div(shares_outstanding, shares_outstanding.groupby(symbol_key, sort=False).shift(4)) - 1.0

    # ------------------------------------------------------------------
    # Fundamentals: Piotroski F-score (aligned to canonical definitions)
    # - Uses trailing-4-quarter (TTM) values for income/cashflow metrics.
    # - Compares against the same quarter 1 year ago (shift=4).
    # ------------------------------------------------------------------
    net_income_ttm = net_income.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    operating_cash_flow_ttm = operating_cash_flow.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    revenue_ttm = revenue.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )
    gross_profit_ttm = gross_profit.groupby(symbol_key, sort=False).transform(
        lambda series: series.rolling(window=4, min_periods=4).sum()
    )

    out["net_income_ttm"] = net_income_ttm
    out["operating_cash_flow_ttm"] = operating_cash_flow_ttm
    out["roa_ttm"] = _safe_div(net_income_ttm, total_assets)
    out["long_term_debt_to_assets"] = _safe_div(long_term_debt, total_assets)
    out["gross_margin_ttm"] = _safe_div(gross_profit_ttm, revenue_ttm)
    out["asset_turnover_ttm"] = _safe_div(revenue_ttm, total_assets)
    out["shares_outstanding"] = shares_outstanding

    roa_lag = out["roa_ttm"].groupby(symbol_key, sort=False).shift(4)
    lt_debt_lag = out["long_term_debt_to_assets"].groupby(symbol_key, sort=False).shift(4)
    current_ratio_lag = out["current_ratio"].groupby(symbol_key, sort=False).shift(4)
    gross_margin_lag = out["gross_margin_ttm"].groupby(symbol_key, sort=False).shift(4)
    asset_turnover_lag = out["asset_turnover_ttm"].groupby(symbol_key, sort=False).shift(4)
    shares_outstanding_lag = out["shares_outstanding"].groupby(symbol_key, sort=False).shift(4)

    out["piotroski_roa_pos"] = (out["roa_ttm"] > 0).astype(int)
    out["piotroski_cfo_pos"] = (out["operating_cash_flow_ttm"] > 0).astype(int)
    out["piotroski_delta_roa_pos"] = (out["roa_ttm"] > roa_lag).astype(int)
    out["piotroski_accruals_pos"] = (out["operating_cash_flow_ttm"] > out["net_income_ttm"]).astype(int)
    out["piotroski_leverage_decrease"] = (out["long_term_debt_to_assets"] < lt_debt_lag).astype(int)
    out["piotroski_liquidity_increase"] = (out["current_ratio"] > current_ratio_lag).astype(int)
    out["piotroski_no_new_shares"] = (out["shares_outstanding"] <= shares_outstanding_lag).astype(int)
    out["piotroski_gross_margin_increase"] = (out["gross_margin_ttm"] > gross_margin_lag).astype(int)
    out["piotroski_asset_turnover_increase"] = (out["asset_turnover_ttm"] > asset_turnover_lag).astype(int)

    piotroski_components = [
        "piotroski_roa_pos",
        "piotroski_cfo_pos",
        "piotroski_delta_roa_pos",
        "piotroski_accruals_pos",
        "piotroski_leverage_decrease",
        "piotroski_liquidity_increase",
        "piotroski_no_new_shares",
        "piotroski_gross_margin_increase",
        "piotroski_asset_turnover_increase",
    ]
    out["piotroski_f_score"] = out[piotroski_components].sum(axis=1)

    out["pe_ratio"] = pe_ratio
    out["ev_ebitda"] = ev_ebitda
    out["market_cap"] = market_cap
    out["fpe"] = forward_pe
    out["ev_rev"] = ev_revenue

    rev_yoy_positive = out["rev_yoy"].where(out["rev_yoy"] > 0)
    out["peg_proxy"] = _safe_div(out["pe_ratio"], rev_yoy_positive)

    out = out.replace([np.inf, -np.inf], np.nan)
    return out


def _align_to_existing_schema(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
    from core import delta_core

    existing_cols = delta_core.get_delta_schema_columns(container, path)
    if not existing_cols:
        return df.reset_index(drop=True)

    out = df.copy()
    for col in existing_cols:
        if col not in out.columns:
            out[col] = pd.NA

    ordered_cols = list(existing_cols) + [col for col in out.columns if col not in existing_cols]
    out = out[ordered_cols]
    return out.reset_index(drop=True)


def _process_ticker(task: Tuple[str, str, str, str, str, str, str, str, Optional[str]]) -> Dict[str, Any]:
    from core import core as mdc
    from core import delta_core

    ticker, income_path, balance_path, cashflow_path, valuation_path, gold_path, silver_container, gold_container, backfill_start_iso = task

    df_income = _prepare_table(delta_core.load_delta(silver_container, income_path), ticker)
    df_balance = _prepare_table(delta_core.load_delta(silver_container, balance_path), ticker)
    df_cashflow = _prepare_table(delta_core.load_delta(silver_container, cashflow_path), ticker)

    df_valuation_raw = delta_core.load_delta(silver_container, valuation_path)
    df_valuation = _prepare_table(df_valuation_raw, ticker) if df_valuation_raw is not None else None

    if not any([df_income is not None, df_balance is not None, df_cashflow is not None, df_valuation is not None]):
        return {"ticker": ticker, "status": "skipped_no_data"}

    base_dates = []
    for table in (df_income, df_balance, df_cashflow, df_valuation):
        if table is not None:
            base_dates.append(table[["date", "symbol"]])
    keys = pd.concat(base_dates, ignore_index=True).drop_duplicates(subset=["symbol", "date"], keep="last")

    merged = keys
    for table, suffix in (
        (df_income, "_is"),
        (df_balance, "_bs"),
        (df_cashflow, "_cf"),
        (df_valuation, "_val"),
    ):
        if table is None:
            continue
        merged = merged.merge(table, on=["symbol", "date"], how="left", suffixes=("", suffix))

    try:
        df_features = compute_features(merged)
    except Exception as exc:
        return {"ticker": ticker, "status": "failed_compute", "error": str(exc)}

    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    df_features, _ = apply_backfill_start_cutoff(
        df_features,
        date_col="date",
        backfill_start=backfill_start,
        context=f"gold finance {ticker}",
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

    try:
        df_features = _align_to_existing_schema(df_features, gold_container, gold_path)
        df_features = normalize_columns_to_snake_case(df_features)
        delta_core.store_delta(df_features, gold_container, gold_path, mode="overwrite", schema_mode="merge")
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

    tickers: List[str] = [str(sym) for sym in df_symbols["Symbol"].astype(str).tolist()]
    tickers = list(dict.fromkeys(tickers))

    max_workers = _get_max_workers()
    mdc.write_line(f"Finance feature engineering configured for {len(tickers)} tickers (max_workers={max_workers})")

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
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying BACKFILL_START_DATE cutoff to gold finance features: {backfill_start_iso}")

    watermarks = load_watermarks("gold_finance_features")
    watermarks_dirty = False

    tasks = []
    commit_map: Dict[str, float | None] = {}
    skipped_unchanged = 0
    for ticker in job_cfg.tickers:
        income_path = DataPaths.get_finance_path("Income Statement", ticker, "quarterly_financials")
        balance_path = DataPaths.get_finance_path("Balance Sheet", ticker, "quarterly_balance-sheet")
        cashflow_path = DataPaths.get_finance_path("Cash Flow", ticker, "quarterly_cash-flow")
        valuation_path = DataPaths.get_finance_path("Valuation", ticker, "quarterly_valuation_measures")
        gold_path = DataPaths.get_gold_finance_path(ticker)

        commits = [
            delta_core.get_delta_last_commit(job_cfg.silver_container, income_path),
            delta_core.get_delta_last_commit(job_cfg.silver_container, balance_path),
            delta_core.get_delta_last_commit(job_cfg.silver_container, cashflow_path),
            delta_core.get_delta_last_commit(job_cfg.silver_container, valuation_path),
        ]
        silver_commit = max([c for c in commits if c is not None], default=None)
        commit_map[ticker] = silver_commit

        if watermarks is not None and silver_commit is not None:
            prior = watermarks.get(ticker, {})
            if prior.get("silver_last_commit") is not None and prior.get("silver_last_commit") >= silver_commit:
                skipped_unchanged += 1
                continue

        tasks.append(
            (
                ticker,
                income_path,
                balance_path,
                cashflow_path,
                valuation_path,
                gold_path,
                job_cfg.silver_container,
                job_cfg.gold_container,
                backfill_start_iso,
            )
        )

    mp_context = mp.get_context("spawn")
    results: List[Dict[str, Any]] = []

    mdc.write_line("Starting finance feature engineering pool...")
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
        f"Finance feature engineering complete: ok={ok}, skipped_no_data={skipped}, "
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
            save_watermarks("gold_finance_features", watermarks)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-finance-job"
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="finance", job_name=job_name)
    raise SystemExit(exit_code)
