import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Sequence, Tuple, Dict, Any, List, Optional

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks
from tasks.common.backfill import apply_backfill_start_cutoff, get_backfill_range
from tasks.common.silver_contracts import align_to_existing_schema, normalize_columns_to_snake_case
from tasks.common import layer_bucketing
from tasks.common.market_reconciliation import (
    collect_delta_market_symbols,
    collect_delta_silver_finance_symbols,
    enforce_backfill_cutoff_on_bucket_tables,
    purge_orphan_rows_from_bucket_tables,
)

@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str


_NUMBER_RE = re.compile(r"^\s*([-+]?\d*\.?\d+)\s*([kKmMbBtT])?\s*$")
_FREE_CASH_FLOW_DERIVATION_LABEL = (
    "free_cash_flow missing in source; derivable via operating_cash_flow - abs(capital_expenditures)"
)
_TOTAL_DEBT_DERIVATION_LABEL = (
    "total_debt missing in source; derivable via short_long_term_debt_total or long_term_debt + short_term/current_debt"
)
_CASH_AND_EQUIVALENTS_DERIVATION_LABEL = (
    "cash_and_equivalents missing in source; derivable via cash_and_cash_equivalents_at_carrying_value or cash_and_short_term_investments"
)
_EV_EBITDA_DERIVATION_LABEL = (
    "ev_ebitda missing in source; derivable via enterprise_value/ebitda or ev_revenue*revenue/ebitda"
)
_REQUIRED_FEATURE_COLUMN_ALIASES: Dict[str, Tuple[str, ...]] = {
    "revenue": ("Total Revenue", "Revenue"),
    "gross_profit": ("Gross Profit",),
    "operating_income": ("Operating Income", "Operating Income or Loss"),
    "net_income": ("Net Income", "Net Income Common Stockholders"),
    "free_cash_flow": ("Free Cash Flow",),
    "operating_cash_flow": (
        "Operating Cash Flow",
        "Total Cash From Operating Activities",
        "Cash Flow From Continuing Operating Activities",
        "Net Cash Provided by Operating Activities",
    ),
    "total_debt": ("Total Debt",),
    "long_term_debt": (
        "Long Term Debt",
        "Long Term Debt And Capital Lease Obligation",
        "Long Term Debt & Capital Lease Obligation",
        "Long-term Debt",
        "Long-Term Debt",
    ),
    "total_assets": ("Total Assets",),
    "current_assets": ("Current Assets", "Total Current Assets"),
    "current_liabilities": ("Current Liabilities", "Total Current Liabilities"),
    "shares_outstanding": (
        "Shares Outstanding",
        "Common Stock Shares Outstanding",
        "Common Shares Outstanding",
        "Ordinary Shares Number",
        "Share Issued",
    ),
    "pe_ratio": ("Trailing P/E", "PE Ratio (TTM)", "P/E Ratio", "P/E", "PE Ratio"),
    "ev_ebitda": ("Enterprise Value/EBITDA", "EV/EBITDA", "EV / EBITDA"),
    "market_cap": ("Market Cap", "Market Cap (intraday)"),
    "ebitda": ("EBITDA", "Normalized EBITDA"),
    "forward_pe": ("Forward P/E", "Forward PE"),
    "ev_revenue": ("Enterprise Value/Revenue", "EV/Revenue", "EV / Revenue"),
    "cash_and_equivalents": (
        "Cash And Cash Equivalents",
        "Cash & Cash Equivalents",
        "Cash and Cash Equivalents",
    ),
}
_CAPITAL_EXPENDITURES_ALIASES: Tuple[str, ...] = (
    "Capital Expenditures",
    "Capital Expenditure",
)
_TOTAL_DEBT_FALLBACK_ALIASES: Tuple[str, ...] = (
    "Short Long Term Debt Total",
    "Short/Long Term Debt Total",
    "Short Long Term Debt",
)
_SHORT_TERM_DEBT_ALIASES: Tuple[str, ...] = (
    "Short Term Debt",
    "Current Debt",
    "Current Long Term Debt",
)
_CASH_AND_EQUIVALENTS_FALLBACK_ALIASES: Tuple[str, ...] = (
    "Cash And Cash Equivalents At Carrying Value",
    "Cash and Cash Equivalents at Carrying Value",
    "Cash And Short Term Investments",
    "Cash and Short Term Investments",
)
_GOLD_FINANCE_ALPHA26_SUBDOMAINS: Tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
)


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

    return None


def _require_column(df: pd.DataFrame, *, label: str, candidates: Sequence[str]) -> str:
    resolved = _resolve_column(df, candidates)
    if resolved:
        return resolved
    raise ValueError(
        f"Missing required source column for {label}; accepted aliases={list(candidates)}"
    )


def _build_missing_source_column_message(
    label: str,
    candidates: Sequence[str],
    *,
    derivation_inputs: Optional[str] = None,
) -> str:
    message = f"Missing required source column for {label}; accepted aliases={list(candidates)}"
    if derivation_inputs:
        message = f"{message} or derivation inputs {derivation_inputs}"
    return message


def _append_unique(values: List[str], item: str) -> None:
    if item not in values:
        values.append(item)


def _derive_free_cash_flow_if_missing(out: pd.DataFrame) -> Tuple[str, bool]:
    """
    Return the free-cash-flow column name, deriving it when absent.

    Derivation follows the common finance convention:
    free_cash_flow = operating_cash_flow - capex_outflow
    where capex_outflow is treated as an absolute outflow amount to handle both
    positive and negative source sign conventions.
    """
    free_cash_flow_col = _resolve_column(
        out, _REQUIRED_FEATURE_COLUMN_ALIASES["free_cash_flow"]
    )
    if free_cash_flow_col:
        return free_cash_flow_col, False

    operating_cash_flow_col = _resolve_column(
        out, _REQUIRED_FEATURE_COLUMN_ALIASES["operating_cash_flow"]
    )
    capital_expenditures_col = _resolve_column(out, _CAPITAL_EXPENDITURES_ALIASES)
    if not operating_cash_flow_col or not capital_expenditures_col:
        raise ValueError(
            _build_missing_source_column_message(
                "free_cash_flow",
                _REQUIRED_FEATURE_COLUMN_ALIASES["free_cash_flow"],
                derivation_inputs=(
                    f"operating_cash_flow aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['operating_cash_flow'])} "
                    f"+ capital_expenditures aliases={list(_CAPITAL_EXPENDITURES_ALIASES)}"
                ),
            )
        )

    operating_cash_flow = _coerce_numeric(out[operating_cash_flow_col])
    capital_expenditures = _coerce_numeric(out[capital_expenditures_col]).abs()
    out["free_cash_flow"] = operating_cash_flow - capital_expenditures
    return "free_cash_flow", True


def _derive_total_debt_if_missing(out: pd.DataFrame) -> Tuple[str, bool]:
    total_debt_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["total_debt"])
    if total_debt_col:
        return total_debt_col, False

    total_debt_fallback_col = _resolve_column(out, _TOTAL_DEBT_FALLBACK_ALIASES)
    if total_debt_fallback_col:
        out["total_debt"] = _coerce_numeric(out[total_debt_fallback_col])
        return "total_debt", True

    long_term_debt_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["long_term_debt"])
    short_term_debt_col = _resolve_column(out, _SHORT_TERM_DEBT_ALIASES)
    if long_term_debt_col and short_term_debt_col:
        long_term_debt = _coerce_numeric(out[long_term_debt_col])
        short_term_debt = _coerce_numeric(out[short_term_debt_col])
        out["total_debt"] = long_term_debt.add(short_term_debt, fill_value=0.0)
        return "total_debt", True

    raise ValueError(
        _build_missing_source_column_message(
            "total_debt",
            _REQUIRED_FEATURE_COLUMN_ALIASES["total_debt"],
            derivation_inputs=(
                f"short_long_term_debt_total aliases={list(_TOTAL_DEBT_FALLBACK_ALIASES)} "
                f"or long_term_debt aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['long_term_debt'])} "
                f"+ short_term_debt aliases={list(_SHORT_TERM_DEBT_ALIASES)}"
            ),
        )
    )


def _derive_cash_and_equivalents_if_missing(out: pd.DataFrame) -> Tuple[str, bool]:
    cash_and_equivalents_col = _resolve_column(
        out, _REQUIRED_FEATURE_COLUMN_ALIASES["cash_and_equivalents"]
    )
    if cash_and_equivalents_col:
        return cash_and_equivalents_col, False

    fallback_col = _resolve_column(out, _CASH_AND_EQUIVALENTS_FALLBACK_ALIASES)
    if fallback_col:
        out["cash_and_equivalents"] = _coerce_numeric(out[fallback_col])
        return "cash_and_equivalents", True

    raise ValueError(
        _build_missing_source_column_message(
            "cash_and_equivalents",
            _REQUIRED_FEATURE_COLUMN_ALIASES["cash_and_equivalents"],
            derivation_inputs=f"fallback aliases={list(_CASH_AND_EQUIVALENTS_FALLBACK_ALIASES)}",
        )
    )


def _derive_ev_ebitda_if_missing(out: pd.DataFrame) -> Tuple[str, bool]:
    ev_ebitda_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["ev_ebitda"])
    if ev_ebitda_col:
        return ev_ebitda_col, False

    market_cap_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["market_cap"])
    ebitda_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["ebitda"])
    if market_cap_col and ebitda_col:
        try:
            total_debt_col, _ = _derive_total_debt_if_missing(out)
            cash_and_equivalents_col, _ = _derive_cash_and_equivalents_if_missing(out)
            market_cap = _coerce_numeric(out[market_cap_col])
            total_debt = _coerce_numeric(out[total_debt_col])
            cash_and_equivalents = _coerce_numeric(out[cash_and_equivalents_col])
            ebitda = _coerce_numeric(out[ebitda_col])
            enterprise_value = market_cap + total_debt - cash_and_equivalents
            out["ev_ebitda"] = _safe_div(enterprise_value, ebitda)
            return "ev_ebitda", True
        except ValueError:
            pass

    ev_revenue_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["ev_revenue"])
    revenue_col = _resolve_column(out, _REQUIRED_FEATURE_COLUMN_ALIASES["revenue"])
    if ev_revenue_col and revenue_col and ebitda_col:
        ev_revenue = _coerce_numeric(out[ev_revenue_col])
        revenue = _coerce_numeric(out[revenue_col])
        ebitda = _coerce_numeric(out[ebitda_col])
        out["ev_ebitda"] = _safe_div(ev_revenue * revenue, ebitda)
        return "ev_ebitda", True

    raise ValueError(
        _build_missing_source_column_message(
            "ev_ebitda",
            _REQUIRED_FEATURE_COLUMN_ALIASES["ev_ebitda"],
            derivation_inputs=(
                f"(market_cap aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['market_cap'])} "
                f"+ total_debt aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['total_debt'])} "
                f"- cash_and_equivalents aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['cash_and_equivalents'])}) "
                f"/ ebitda aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['ebitda'])} "
                f"or ev_revenue aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['ev_revenue'])} "
                f"* revenue aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['revenue'])} / "
                f"ebitda aliases={list(_REQUIRED_FEATURE_COLUMN_ALIASES['ebitda'])}"
            ),
        )
    )


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


def _prepare_table(df: Optional[pd.DataFrame], ticker: str, *, source_label: str) -> pd.DataFrame:
    if df is None or df.empty:
        raise ValueError(f"Missing required Silver source table for {source_label} ({ticker}).")

    out = _snake_case_columns(df)

    if "date" not in out.columns:
        raise ValueError(f"Required date column missing in {source_label} for {ticker}.")

    out["date"] = _coerce_datetime(out["date"])
    out = out.dropna(subset=["date"]).copy()
    if out.empty:
        raise ValueError(f"No valid dated rows in {source_label} for {ticker}.")

    out["symbol"] = ticker
    out = out.sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)
    return out


def _preflight_feature_schema(df: pd.DataFrame) -> Dict[str, Any]:
    out = _snake_case_columns(df)
    missing_requirements: List[str] = []
    recoverable_drift: List[str] = []

    derivable_requirements = {
        "free_cash_flow",
        "total_debt",
        "cash_and_equivalents",
        "ev_ebitda",
    }
    derivation_checks = (
        (_derive_free_cash_flow_if_missing, _FREE_CASH_FLOW_DERIVATION_LABEL),
        (_derive_total_debt_if_missing, _TOTAL_DEBT_DERIVATION_LABEL),
        (_derive_cash_and_equivalents_if_missing, _CASH_AND_EQUIVALENTS_DERIVATION_LABEL),
        (_derive_ev_ebitda_if_missing, _EV_EBITDA_DERIVATION_LABEL),
    )
    for derive_fn, label in derivation_checks:
        try:
            _, derived = derive_fn(out)
            if derived:
                _append_unique(recoverable_drift, label)
        except ValueError as exc:
            _append_unique(missing_requirements, str(exc))

    for label, candidates in _REQUIRED_FEATURE_COLUMN_ALIASES.items():
        if label in derivable_requirements:
            continue
        if _resolve_column(out, candidates) is None:
            _append_unique(
                missing_requirements,
                _build_missing_source_column_message(label, candidates),
            )

    return {
        "missing_requirements": missing_requirements,
        "recoverable_drift": recoverable_drift,
        "available_columns": sorted(str(col) for col in out.columns),
    }


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

    revenue_col = _require_column(
        out, label="revenue", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["revenue"]
    )
    gross_profit_col = _require_column(
        out, label="gross_profit", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["gross_profit"]
    )
    operating_income_col = _require_column(
        out,
        label="operating_income",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["operating_income"],
    )
    net_income_col = _require_column(
        out,
        label="net_income",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["net_income"],
    )
    free_cash_flow_col, _ = _derive_free_cash_flow_if_missing(out)
    operating_cash_flow_col = _require_column(
        out,
        label="operating_cash_flow",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["operating_cash_flow"],
    )

    total_debt_col, _ = _derive_total_debt_if_missing(out)
    long_term_debt_col = _require_column(
        out,
        label="long_term_debt",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["long_term_debt"],
    )
    total_assets_col = _require_column(
        out, label="total_assets", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["total_assets"]
    )
    current_assets_col = _require_column(
        out, label="current_assets", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["current_assets"]
    )
    current_liabilities_col = _require_column(
        out,
        label="current_liabilities",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["current_liabilities"],
    )
    shares_outstanding_col = _require_column(
        out,
        label="shares_outstanding",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["shares_outstanding"],
    )

    pe_ratio_col = _require_column(
        out,
        label="pe_ratio",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["pe_ratio"],
    )
    ev_ebitda_col, _ = _derive_ev_ebitda_if_missing(out)
    market_cap_col = _require_column(
        out, label="market_cap", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["market_cap"]
    )
    ebitda_col = _require_column(
        out, label="ebitda", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["ebitda"]
    )
    forward_pe_col = _require_column(
        out, label="forward_pe", candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["forward_pe"]
    )
    ev_revenue_col = _require_column(
        out,
        label="ev_revenue",
        candidates=_REQUIRED_FEATURE_COLUMN_ALIASES["ev_revenue"],
    )
    cash_and_equivalents_col, _ = _derive_cash_and_equivalents_if_missing(out)

    revenue = _coerce_numeric(out[revenue_col])
    gross_profit = _coerce_numeric(out[gross_profit_col])
    operating_income = _coerce_numeric(out[operating_income_col])
    net_income = _coerce_numeric(out[net_income_col])
    free_cash_flow = _coerce_numeric(out[free_cash_flow_col])
    operating_cash_flow = _coerce_numeric(out[operating_cash_flow_col])
    ebitda = _coerce_numeric(out[ebitda_col])
    forward_pe = _coerce_numeric(out[forward_pe_col])
    ev_revenue = _coerce_numeric(out[ev_revenue_col])

    out[revenue_col] = revenue
    out[gross_profit_col] = gross_profit
    out[operating_income_col] = operating_income
    out[net_income_col] = net_income
    out[free_cash_flow_col] = free_cash_flow
    out[operating_cash_flow_col] = operating_cash_flow

    total_debt = _coerce_numeric(out[total_debt_col])
    long_term_debt = _coerce_numeric(out[long_term_debt_col])
    total_assets = _coerce_numeric(out[total_assets_col])
    current_assets = _coerce_numeric(out[current_assets_col])
    current_liabilities = _coerce_numeric(out[current_liabilities_col])
    shares_outstanding = _coerce_numeric(out[shares_outstanding_col])
    cash_and_equivalents = _coerce_numeric(out[cash_and_equivalents_col])

    out[total_debt_col] = total_debt
    out[long_term_debt_col] = long_term_debt
    out[total_assets_col] = total_assets
    out[current_assets_col] = current_assets
    out[current_liabilities_col] = current_liabilities
    out[shares_outstanding_col] = shares_outstanding

    pe_ratio = _coerce_numeric(out[pe_ratio_col])
    ev_ebitda = _coerce_numeric(out[ev_ebitda_col])
    market_cap = _coerce_numeric(out[market_cap_col])

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


def _build_job_config() -> FeatureJobConfig:
    silver_container = os.environ.get("AZURE_CONTAINER_SILVER")
    gold_container = os.environ.get("AZURE_CONTAINER_GOLD")

    if not silver_container or not str(silver_container).strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_SILVER' is required.")
    if not gold_container or not str(gold_container).strip():
        raise ValueError("Environment variable 'AZURE_CONTAINER_GOLD' is required.")

    return FeatureJobConfig(
        silver_container=str(silver_container).strip(),
        gold_container=str(gold_container).strip(),
    )


def _empty_gold_finance_bucket_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns]"),
            "symbol": pd.Series(dtype="string"),
        }
    )


def _gold_finance_bucket_paths(bucket: str) -> list[str]:
    from core.pipeline import DataPaths

    return [DataPaths.get_gold_finance_bucket_path(sub_domain, bucket) for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS]


def _legacy_gold_finance_bucket_path(bucket: str) -> str:
    from core.pipeline import DataPaths

    return DataPaths.get_legacy_gold_finance_bucket_path(bucket)


def _normalize_sub_domain(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _load_existing_gold_finance_symbol_to_bucket_map(*, sub_domain: Optional[str] = None) -> dict[str, str]:
    out: dict[str, str] = {}
    existing = layer_bucketing.load_layer_symbol_index(layer="gold", domain="finance")
    if existing is None or existing.empty:
        return out
    if "symbol" not in existing.columns or "bucket" not in existing.columns:
        return out

    valid_buckets = set(layer_bucketing.ALPHABET_BUCKETS)
    expected_sub_domain = _normalize_sub_domain(sub_domain)
    sub_domain_series = (
        existing["sub_domain"].fillna("").astype(str).map(_normalize_sub_domain)
        if "sub_domain" in existing.columns
        else pd.Series([""] * len(existing), index=existing.index, dtype="string")
    )

    for idx, row in existing.iterrows():
        row_sub_domain = str(sub_domain_series.loc[idx] or "")
        if expected_sub_domain:
            if row_sub_domain != expected_sub_domain:
                continue
        elif row_sub_domain:
            continue
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
    out = {symbol: current_bucket for symbol, current_bucket in existing.items() if current_bucket != touched_bucket}
    out.update(touched_symbol_to_bucket)
    return out


def _load_gold_finance_bucket_template(
    *,
    container: str,
    candidate_paths: Sequence[str],
) -> tuple[pd.DataFrame, bool]:
    from core import delta_core

    for path in candidate_paths:
        try:
            df_existing = delta_core.load_delta(container, path)
        except Exception:
            continue
        if df_existing is None:
            continue
        return normalize_columns_to_snake_case(df_existing).iloc[0:0].copy(), True
    return _empty_gold_finance_bucket_frame(), False


def _delete_legacy_gold_finance_bucket(*, client: Any, bucket: str) -> int:
    if client is None or not hasattr(client, "delete_prefix"):
        return 0
    legacy_path = _legacy_gold_finance_bucket_path(bucket)
    try:
        return int(client.delete_prefix(legacy_path) or 0)
    except Exception:
        return 0


def _run_finance_reconciliation(*, silver_container: str, gold_container: str) -> tuple[int, int]:
    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    silver_client = mdc.get_storage_client(silver_container)
    gold_client = mdc.get_storage_client(gold_container)
    if silver_client is None:
        raise RuntimeError("Gold finance reconciliation requires silver storage client.")
    if gold_client is None:
        raise RuntimeError("Gold finance reconciliation requires gold storage client.")

    silver_symbols = collect_delta_silver_finance_symbols(client=silver_client)
    gold_symbols = collect_delta_market_symbols(client=gold_client, root_prefix="finance")
    orphan_symbols, purge_stats = purge_orphan_rows_from_bucket_tables(
        upstream_symbols=silver_symbols,
        downstream_symbols=gold_symbols,
        table_paths_for_symbol=lambda symbol: [
            DataPaths.get_gold_finance_bucket_path(sub_domain, layer_bucketing.bucket_letter(symbol))
            for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS
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
            "Gold finance reconciliation purged orphan symbols: "
            f"count={len(orphan_symbols)} deleted_blobs={deleted_blobs} "
            f"tables_rewritten={purge_stats.tables_rewritten} rows_deleted={purge_stats.rows_deleted}"
        )
    else:
        mdc.write_line("Gold finance reconciliation: no orphan symbols detected.")
    if purge_stats.errors > 0:
        mdc.write_warning(f"Gold finance orphan purge encountered errors={purge_stats.errors}.")

    backfill_start, _ = get_backfill_range()
    cutoff_stats = enforce_backfill_cutoff_on_bucket_tables(
        table_paths=[
            DataPaths.get_gold_finance_bucket_path(sub_domain, bucket)
            for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS
            for bucket in layer_bucketing.ALPHABET_BUCKETS
        ],
        load_table=lambda path: delta_core.load_delta(gold_container, path),
        store_table=lambda df, path: delta_core.store_delta(df, gold_container, path, mode="overwrite"),
        delete_prefix=gold_client.delete_prefix,
        date_column_candidates=("date", "Date"),
        backfill_start=backfill_start,
        context="gold finance reconciliation cutoff",
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
            "Gold finance reconciliation cutoff sweep: "
            f"tables_scanned={cutoff_stats.tables_scanned} "
            f"tables_rewritten={cutoff_stats.tables_rewritten} "
            f"deleted_blobs={cutoff_stats.deleted_blobs} "
            f"rows_dropped={cutoff_stats.rows_dropped}"
        )
    if cutoff_stats.errors > 0:
        mdc.write_warning(f"Gold finance reconciliation cutoff sweep encountered errors={cutoff_stats.errors}.")
    return len(orphan_symbols), deleted_blobs


def _run_alpha26_finance_gold(
    *,
    silver_container: str,
    gold_container: str,
    backfill_start_iso: Optional[str],
    watermarks: dict,
) -> tuple[int, int, int, int, bool, int, Optional[str]]:
    from core import core as mdc
    from core import delta_core

    force_rebuild = layer_bucketing.gold_alpha26_force_rebuild()
    backfill_start = pd.to_datetime(backfill_start_iso).normalize() if backfill_start_iso else None
    gold_client = mdc.get_storage_client(gold_container)
    processed = 0
    skipped_unchanged = 0
    skipped_missing_source = 0
    failed = 0
    watermarks_dirty = False
    symbol_to_bucket = _load_existing_gold_finance_symbol_to_bucket_map()
    symbols_by_sub_domain: dict[str, dict[str, str]] = {
        sub_domain: _load_existing_gold_finance_symbol_to_bucket_map(sub_domain=sub_domain)
        for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS
    }

    for bucket in layer_bucketing.ALPHABET_BUCKETS:
        from core.pipeline import DataPaths

        silver_paths = {
            "income_statement": DataPaths.get_silver_finance_bucket_path("income_statement", bucket),
            "balance_sheet": DataPaths.get_silver_finance_bucket_path("balance_sheet", bucket),
            "cash_flow": DataPaths.get_silver_finance_bucket_path("cash_flow", bucket),
            "valuation": DataPaths.get_silver_finance_bucket_path("valuation", bucket),
        }
        gold_paths = _gold_finance_bucket_paths(bucket)
        legacy_gold_path = _legacy_gold_finance_bucket_path(bucket)
        commits = [
            delta_core.get_delta_last_commit(silver_container, silver_paths["income_statement"]),
            delta_core.get_delta_last_commit(silver_container, silver_paths["balance_sheet"]),
            delta_core.get_delta_last_commit(silver_container, silver_paths["cash_flow"]),
            delta_core.get_delta_last_commit(silver_container, silver_paths["valuation"]),
        ]
        silver_commit = max([c for c in commits if c is not None], default=None)
        gold_commits = {path: delta_core.get_delta_last_commit(gold_container, path) for path in gold_paths}
        missing_gold_paths = [path for path, commit in gold_commits.items() if commit is None]
        legacy_gold_commit = delta_core.get_delta_last_commit(gold_container, legacy_gold_path)
        watermark_key = f"bucket::{bucket}"
        prior = watermarks.get(watermark_key, {})
        skip_due_watermark = (
            (not force_rebuild)
            and silver_commit is not None
            and prior.get("silver_last_commit") is not None
            and prior.get("silver_last_commit") >= silver_commit
        )
        if skip_due_watermark and not missing_gold_paths:
            skipped_unchanged += 1
            deleted_legacy = _delete_legacy_gold_finance_bucket(client=gold_client, bucket=bucket)
            if deleted_legacy > 0:
                mdc.write_line(
                    f"Gold finance alpha26 removed legacy bucket path {legacy_gold_path}: deleted_blobs={deleted_legacy}"
                )
            continue

        df_gold_bucket: Optional[pd.DataFrame] = None
        bucket_symbol_to_bucket: dict[str, str] = {}
        template_schema_available = False
        migrated_from_legacy = False

        if skip_due_watermark and missing_gold_paths and legacy_gold_commit is not None:
            try:
                df_gold_bucket = delta_core.load_delta(gold_container, legacy_gold_path)
                if df_gold_bucket is None:
                    df_gold_bucket = _empty_gold_finance_bucket_frame()
                df_gold_bucket = normalize_columns_to_snake_case(df_gold_bucket)
                template_schema_available = True
                migrated_from_legacy = True
                if "symbol" in df_gold_bucket.columns:
                    bucket_symbol_to_bucket = {
                        str(symbol).strip().upper(): bucket
                        for symbol in df_gold_bucket["symbol"].dropna().astype(str).tolist()
                        if str(symbol).strip()
                    }
            except Exception as exc:
                failed += 1
                mdc.write_warning(f"Gold finance alpha26 legacy migration failed bucket={bucket}: {exc}")
                continue

        if df_gold_bucket is None and silver_commit is None:
            skipped_missing_source += 1
            template_candidates = [path for path, commit in gold_commits.items() if commit is not None]
            if legacy_gold_commit is not None:
                template_candidates.append(legacy_gold_path)
            df_gold_bucket, template_schema_available = _load_gold_finance_bucket_template(
                container=gold_container,
                candidate_paths=template_candidates,
            )

        if df_gold_bucket is None:
            tables = {
                key: delta_core.load_delta(silver_container, path)
                for key, path in silver_paths.items()
            }
            symbol_candidates: set[str] = set()
            for frame in tables.values():
                if frame is None or frame.empty or "symbol" not in frame.columns:
                    continue
                symbol_candidates.update(
                    str(sym).strip().upper()
                    for sym in frame["symbol"].dropna().astype(str).tolist()
                    if str(sym).strip()
                )

            symbol_frames: list[pd.DataFrame] = []
            recoverable_schema_drift = 0
            recoverable_schema_drift_samples: list[str] = []
            for ticker in sorted(symbol_candidates):
                try:
                    df_income = _prepare_table(
                        tables.get("income_statement", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="income_statement",
                    )
                    df_balance = _prepare_table(
                        tables.get("balance_sheet", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="balance_sheet",
                    )
                    df_cashflow = _prepare_table(
                        tables.get("cash_flow", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="cash_flow",
                    )
                    df_valuation = _prepare_table(
                        tables.get("valuation", pd.DataFrame()).query("symbol == @ticker").copy(),
                        ticker,
                        source_label="valuation",
                    )
                except Exception as exc:
                    failed += 1
                    mdc.write_warning(f"Gold finance alpha26 source failed for {ticker}: {exc}")
                    continue

                base_dates = []
                for table in (df_income, df_balance, df_cashflow, df_valuation):
                    base_dates.append(table[["date", "symbol"]])
                keys = pd.concat(base_dates, ignore_index=True).drop_duplicates(
                    subset=["symbol", "date"], keep="last"
                )

                merged = keys
                for table, suffix in (
                    (df_income, "_is"),
                    (df_balance, "_bs"),
                    (df_cashflow, "_cf"),
                    (df_valuation, "_val"),
                ):
                    merged = merged.merge(table, on=["symbol", "date"], how="left", suffixes=("", suffix))

                preflight = _preflight_feature_schema(merged)
                if preflight["missing_requirements"]:
                    failed += 1
                    mdc.write_warning(
                        "Gold finance alpha26 schema preflight failed for "
                        f"{ticker}: missing={preflight['missing_requirements']} "
                        f"available_columns={preflight['available_columns']}"
                    )
                    continue
                if preflight["recoverable_drift"]:
                    recoverable_schema_drift += 1
                    if len(recoverable_schema_drift_samples) < 5:
                        recoverable_schema_drift_samples.append(ticker)

                try:
                    df_features = compute_features(merged)
                    df_features, _ = apply_backfill_start_cutoff(
                        df_features,
                        date_col="date",
                        backfill_start=backfill_start,
                        context=f"gold finance alpha26 {ticker}",
                    )
                    if df_features is None or df_features.empty:
                        continue
                    symbol_frames.append(df_features)
                    bucket_symbol_to_bucket[ticker] = bucket
                except Exception as exc:
                    failed += 1
                    mdc.write_warning(f"Gold finance alpha26 compute failed for {ticker}: {exc}")

            if recoverable_schema_drift > 0:
                mdc.write_line(
                    "Gold finance alpha26 schema drift recovered via preflight fallback: "
                    f"bucket={bucket} count={recoverable_schema_drift} "
                    f"sample_tickers={recoverable_schema_drift_samples}"
                )

            if symbol_frames:
                df_gold_bucket = pd.concat(symbol_frames, ignore_index=True)
                df_gold_bucket = normalize_columns_to_snake_case(df_gold_bucket)
            else:
                df_gold_bucket = _empty_gold_finance_bucket_frame()

        bucket_failed = False
        writes_completed = 0
        for gold_path in gold_paths:
            existing_cols = delta_core.get_delta_schema_columns(gold_container, gold_path)
            if df_gold_bucket.empty and not existing_cols and not template_schema_available:
                mdc.write_line(
                    f"Skipping Gold finance empty bucket write for {gold_path}: no existing Delta schema."
                )
                continue
            df_to_store = align_to_existing_schema(
                df_gold_bucket.reset_index(drop=True),
                container=gold_container,
                path=gold_path,
            )
            try:
                delta_core.store_delta(df_to_store, gold_container, gold_path, mode="overwrite")
                if backfill_start is not None:
                    delta_core.vacuum_delta_table(
                        gold_container,
                        gold_path,
                        retention_hours=0,
                        dry_run=False,
                        enforce_retention_duration=False,
                        full=True,
                    )
                writes_completed += 1
            except Exception as exc:
                bucket_failed = True
                failed += 1
                mdc.write_error(f"Gold finance alpha26 write failed bucket={bucket} path={gold_path}: {exc}")

        if bucket_failed or writes_completed <= 0:
            continue

        processed += 1
        symbol_to_bucket = _merge_symbol_to_bucket_map(
            symbol_to_bucket,
            touched_bucket=bucket,
            touched_symbol_to_bucket=bucket_symbol_to_bucket,
        )
        for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS:
            symbols_by_sub_domain[sub_domain] = _merge_symbol_to_bucket_map(
                symbols_by_sub_domain[sub_domain],
                touched_bucket=bucket,
                touched_symbol_to_bucket=bucket_symbol_to_bucket,
            )
        if silver_commit is not None and not migrated_from_legacy:
            watermarks[watermark_key] = {
                "silver_last_commit": silver_commit,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            watermarks_dirty = True

        deleted_legacy = _delete_legacy_gold_finance_bucket(client=gold_client, bucket=bucket)
        if deleted_legacy > 0:
            mdc.write_line(
                f"Gold finance alpha26 removed legacy bucket path {legacy_gold_path}: deleted_blobs={deleted_legacy}"
            )

    index_path = layer_bucketing.write_layer_symbol_index(
        layer="gold",
        domain="finance",
        symbol_to_bucket=symbol_to_bucket,
    )
    for sub_domain in _GOLD_FINANCE_ALPHA26_SUBDOMAINS:
        sub_index_path = layer_bucketing.write_layer_symbol_index(
            layer="gold",
            domain="finance",
            symbol_to_bucket=symbols_by_sub_domain[sub_domain],
            sub_domain=sub_domain,
        )
        if sub_index_path:
            index_path = sub_index_path
    return processed, skipped_unchanged, skipped_missing_source, failed, watermarks_dirty, len(symbol_to_bucket), index_path


def main() -> int:
    from core import core as mdc
    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()
    backfill_start, _ = get_backfill_range()
    backfill_start_iso = backfill_start.date().isoformat() if backfill_start is not None else None
    if backfill_start_iso:
        mdc.write_line(f"Applying historical cutoff to gold finance features: {backfill_start_iso}")
    layer_bucketing.gold_layout_mode()
		
    watermarks = load_watermarks("gold_finance_features")
    (
        processed,
        skipped_unchanged,
        skipped_missing_source,
        failed,
        watermarks_dirty,
        alpha26_symbols,
        alpha26_index_path,
    ) = _run_alpha26_finance_gold(
        silver_container=job_cfg.silver_container,
        gold_container=job_cfg.gold_container,
        backfill_start_iso=backfill_start_iso,
        watermarks=watermarks,
    )
    if watermarks_dirty:
        save_watermarks("gold_finance_features", watermarks)
    total_failed = failed
    mdc.write_line(
        "Gold finance alpha26 complete: "
        f"processed_buckets={processed} skipped_unchanged={skipped_unchanged} "
        f"skipped_missing_source={skipped_missing_source} symbols={alpha26_symbols} "
        f"index_path={alpha26_index_path or 'unavailable'} failed={total_failed}"
    )
    return 0 if total_failed == 0 else 1


if __name__ == "__main__":
    from tasks.common.job_trigger import ensure_api_awake_from_env
    from tasks.common.system_health_markers import write_system_health_marker

    job_name = "gold-finance-job"
    ensure_api_awake_from_env(required=True)
    exit_code = main()
    if exit_code == 0:
        write_system_health_marker(layer="gold", domain="finance", job_name=job_name)
    raise SystemExit(exit_code)
