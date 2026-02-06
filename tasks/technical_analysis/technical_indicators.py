"""Technical Analysis Indicators (built from Silver market data).

This job derives technical analysis indicators (including candlestick patterns) from the cleaned OHLCV
data in the Silver layer (market-data/<ticker>) and writes the results as a
Gold per-ticker Delta table (technical-analysis/<ticker>), plus a cross-sectional
table (technical_analysis_by_date) partitioned by year_month and date.

Design goals
------------
- Deterministic, vectorized OHLC-based pattern detection.
- Context-aware where patterns are otherwise ambiguous (e.g., Hammer vs Hanging Man).
- Minimal dependencies: pandas/numpy only.

Output
------
For each symbol/date, the per-ticker output table includes:
- Base OHLCV columns (snake_case)
- Candle geometry metrics (body, range, shadows, ATR)
- Pattern flags (0/1) for the patterns shown in the provided spec/image

The by-date table is a concatenation of the per-ticker tables for a given
year-month. It normalizes `date` to the day boundary for partitioning and also
preserves the original timestamp in `timestamp` (useful when the project moves
to intraday bars).

Notes
-----
- Many patterns traditionally depend on "trend" context. We implement a simple
  configurable trend heuristic based on prior closes.
- The thresholds below are intentionally conservative defaults and can be tuned.
"""

from __future__ import annotations

import argparse
import os
import re
import multiprocessing as mp
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from tasks.common.watermarks import load_watermarks, save_watermarks


@dataclass(frozen=True)
class FeatureJobConfig:
    silver_container: str
    gold_container: str
    max_workers: int
    tickers: Sequence[str]


@dataclass(frozen=True)
class MaterializeConfig:
    container: str
    year_month: str
    output_path: str
    max_tickers: Optional[int]


def _parse_year_month_bounds(year_month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    try:
        start = pd.Timestamp(f"{year_month}-01")
    except Exception as exc:
        raise ValueError(f"Invalid year_month '{year_month}'. Expected YYYY-MM.") from exc
    end = start + pd.offsets.MonthBegin(1)
    return start, end


def _load_ticker_universe() -> List[str]:
    """Fallback ticker universe when we cannot list Delta tables in the gold container."""
    from core import core as mdc

    df_symbols = mdc.get_symbols()
    df_symbols = df_symbols.dropna(subset=["Symbol"]).copy()

    tickers: List[str] = []
    for symbol in df_symbols["Symbol"].astype(str).tolist():
        if "." in symbol:
            continue
        tickers.append(symbol.replace(".", "-"))

    return list(dict.fromkeys(tickers))


def _extract_tickers_from_delta_tables(blob_names: Iterable[str], *, root_prefix: str) -> List[str]:
    """Extract tickers that have a valid Delta log under `<root_prefix>/<ticker>/_delta_log/`."""

    root_prefix = root_prefix.strip("/")
    tickers: set[str] = set()
    for blob_name in blob_names:
        parts = str(blob_name).strip("/").split("/")
        if len(parts) < 4:
            continue
        if parts[0] != root_prefix:
            continue

        ticker = parts[1].strip()
        if not ticker:
            continue

        if parts[2] != "_delta_log":
            continue

        log_file = parts[3]
        if log_file.endswith(".json") or log_file.endswith(".checkpoint.parquet"):
            tickers.add(ticker)

    return sorted(tickers)


def _try_load_tickers_from_container(container: str, *, root_prefix: str) -> Optional[List[str]]:
    """Attempt to list tickers by scanning the container for Delta logs (preferred)."""

    from core.core import write_warning
    from core import core as mdc

    client = mdc.get_storage_client(container)
    if client is None:
        return None

    prefix = root_prefix.strip("/") + "/"
    try:
        blobs = client.container_client.list_blobs(name_starts_with=prefix)
        return _extract_tickers_from_delta_tables((b.name for b in blobs), root_prefix=root_prefix)
    except Exception as exc:
        write_warning(
            f"Unable to list per-ticker Delta tables under {prefix} in container={container}: {exc}. "
            "Falling back to symbol universe."
        )
        return None


def _resolve_gold_container(container_raw: Optional[str]) -> str:
    """Resolve the gold container name for by-date materialization."""
    value = (container_raw or os.environ.get("AZURE_CONTAINER_GOLD") or os.environ.get("AZURE_FOLDER_MARKET") or "").strip()
    if not value:
        raise ValueError("Missing gold container. Set AZURE_CONTAINER_GOLD (preferred) or AZURE_FOLDER_MARKET.")
    return value


def _build_materialize_config(argv: Optional[List[str]]) -> MaterializeConfig:
    from core.pipeline import DataPaths

    parser = argparse.ArgumentParser(
        description="Materialize candlestick indicators into a cross-sectional Delta table (partitioned by date)."
    )
    parser.add_argument("--container", help="Gold container (default: AZURE_CONTAINER_GOLD).")
    parser.add_argument("--year-month", required=True, help="Year-month partition to materialize (YYYY-MM).")
    parser.add_argument(
        "--output-path",
        default=DataPaths.get_technical_analysis_by_date_path(),
        help="Output Delta table path within the container.",
    )
    parser.add_argument("--max-tickers", type=int, default=None, help="Optional limit for debugging.")
    args = parser.parse_args(argv)

    container = _resolve_gold_container(args.container)
    max_tickers = int(args.max_tickers) if args.max_tickers is not None else None
    if max_tickers is not None and max_tickers <= 0:
        max_tickers = None

    return MaterializeConfig(
        container=container,
        year_month=str(args.year_month).strip(),
        output_path=str(args.output_path).strip().lstrip("/"),
        max_tickers=max_tickers,
    )


def _coerce_datetime(series: pd.Series) -> pd.Series:
    value = pd.to_datetime(series, errors="coerce")
    if hasattr(value.dt, "tz_convert") and value.dt.tz is not None:
        value = value.dt.tz_convert(None)
    return value


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denom = denominator.where(denominator != 0)
    return numerator.where(denom.notna()).divide(denom)


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


def _get_int_env(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _get_float_env(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value


def _approx_equal(a: pd.Series, b: pd.Series, tol: pd.Series) -> pd.Series:
    return (a - b).abs() <= tol


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute candlestick indicator features from OHLCV data."""

    out = _snake_case_columns(df)

    required = {"date", "open", "high", "low", "close", "volume", "symbol"}
    missing = required.difference(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["date"] = _coerce_datetime(out["date"])
    out["symbol"] = out["symbol"].astype(str)
    for col in ["open", "high", "low", "close", "volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out = out.dropna(subset=["date"]).sort_values(["symbol", "date"]).reset_index(drop=True)
    out = out.drop_duplicates(subset=["symbol", "date"], keep="last").reset_index(drop=True)

    # --- Candle geometry ---
    o = out["open"]
    h = out["high"]
    l = out["low"]
    c = out["close"]

    out["range"] = (h - l).clip(lower=0)
    out["body"] = (c - o).abs()
    out["is_bull"] = (c > o).astype(int)
    out["is_bear"] = (c < o).astype(int)
    out["upper_shadow"] = h - pd.concat([o, c], axis=1).max(axis=1)
    out["lower_shadow"] = pd.concat([o, c], axis=1).min(axis=1) - l

    out["body_to_range"] = _safe_div(out["body"], out["range"])
    out["upper_to_range"] = _safe_div(out["upper_shadow"], out["range"])
    out["lower_to_range"] = _safe_div(out["lower_shadow"], out["range"])

    # ATR(14) for scale-aware tolerances.
    prev_close = c.groupby(out["symbol"]).shift(1)
    tr_components = pd.concat(
        [
            (h - l),
            (h - prev_close).abs(),
            (l - prev_close).abs(),
        ],
        axis=1,
    )
    out["true_range"] = tr_components.max(axis=1)
    out["atr_14d"] = out.groupby("symbol")["true_range"].transform(lambda s: s.rolling(14, min_periods=14).mean())

    # Equality tolerance used for tweezer highs/lows.
    # - 5% of ATR (when available)
    # - else 0.1% of price
    # - floor at $0.01
    tol_atr = 0.05 * out["atr_14d"].fillna(0.0)
    tol_px = 0.001 * c.abs().fillna(0.0)
    out["_eq_tol"] = np.maximum(np.maximum(tol_atr, tol_px), 0.01)

    # --- Configurable thresholds ---
    trend_window = _get_int_env("CANDLE_CONTEXT_TREND_WINDOW", default=3)

    doji_max_body_to_range = _get_float_env("CANDLE_DOJI_MAX_BODY_TO_RANGE", default=0.05)
    spinning_max_body_to_range = _get_float_env("CANDLE_SPINNING_MAX_BODY_TO_RANGE", default=0.30)
    long_body_min_body_to_range = _get_float_env("CANDLE_LONG_BODY_MIN_BODY_TO_RANGE", default=0.60)
    marubozu_min_body_to_range = _get_float_env("CANDLE_MARUBOZU_MIN_BODY_TO_RANGE", default=0.90)
    marubozu_max_shadow_to_range = _get_float_env("CANDLE_MARUBOZU_MAX_SHADOW_TO_RANGE", default=0.05)

    hammer_min_shadow_to_body = _get_float_env("CANDLE_HAMMER_MIN_SHADOW_TO_BODY", default=2.0)
    small_shadow_max_to_body = _get_float_env("CANDLE_SMALL_SHADOW_MAX_TO_BODY", default=0.25)

    # Gap tolerance for "gap-based" patterns (stars, kickers, abandoned baby).
    #
    # Daily equities can have true session gaps; crypto/FX often won't. For intraday
    # (future), strict gaps are rare, so we make gaps "tolerant" by requiring a small
    # minimum separation between real bodies.
    gap_tol_atr_frac = _get_float_env("CANDLE_GAP_TOL_ATR_FRAC", default=0.01)
    gap_tol_px_frac = _get_float_env("CANDLE_GAP_TOL_PX_FRAC", default=0.0005)
    gap_tol_floor = _get_float_env("CANDLE_GAP_TOL_FLOOR", default=0.0)
    out["_gap_tol"] = np.maximum(
        np.maximum(gap_tol_atr_frac * out["atr_14d"].fillna(0.0), gap_tol_px_frac * c.abs().fillna(0.0)),
        gap_tol_floor,
    )

    # --- Trend context helpers ---
    # We use a simple trend heuristic based on *prior* closes to avoid look-ahead.
    def _downtrend_before(offset: int) -> pd.Series:
        # Compare close[t-offset] vs close[t-offset-trend_window]
        a = c.groupby(out["symbol"]).shift(offset)
        b = c.groupby(out["symbol"]).shift(offset + trend_window)
        return a < b

    def _uptrend_before(offset: int) -> pd.Series:
        a = c.groupby(out["symbol"]).shift(offset)
        b = c.groupby(out["symbol"]).shift(offset + trend_window)
        return a > b

    # --- Base candle-type flags ---
    out["pat_doji"] = (out["body_to_range"] <= doji_max_body_to_range).astype(int)

    # Spinning top: small body + meaningful shadows on both sides.
    # (Shadow comparisons are body-scaled to avoid range=0 edge cases.)
    body = out["body"].replace(0, np.nan)
    out["pat_spinning_top"] = (
        (out["body_to_range"] > doji_max_body_to_range)
        & (out["body_to_range"] <= spinning_max_body_to_range)
        & (out["upper_shadow"] >= 0.5 * body)
        & (out["lower_shadow"] >= 0.5 * body)
    ).fillna(False).astype(int)

    # Marubozu (directional)
    out["pat_bullish_marubozu"] = (
        (c > o)
        & (out["body_to_range"] >= marubozu_min_body_to_range)
        & (out["upper_to_range"] <= marubozu_max_shadow_to_range)
        & (out["lower_to_range"] <= marubozu_max_shadow_to_range)
    ).fillna(False).astype(int)
    out["pat_bearish_marubozu"] = (
        (c < o)
        & (out["body_to_range"] >= marubozu_min_body_to_range)
        & (out["upper_to_range"] <= marubozu_max_shadow_to_range)
        & (out["lower_to_range"] <= marubozu_max_shadow_to_range)
    ).fillna(False).astype(int)

    # Star (gap) candle relative to previous real body.
    prev_o = o.groupby(out["symbol"]).shift(1)
    prev_c = c.groupby(out["symbol"]).shift(1)
    prev_body_hi = pd.concat([prev_o, prev_c], axis=1).max(axis=1)
    prev_body_lo = pd.concat([prev_o, prev_c], axis=1).min(axis=1)
    body_hi = pd.concat([o, c], axis=1).max(axis=1)
    body_lo = pd.concat([o, c], axis=1).min(axis=1)
    gap_tol_0 = out["_gap_tol"]
    gap_tol_1 = out.groupby("symbol")["_gap_tol"].shift(1)
    gap_tol_star = pd.concat([gap_tol_0, gap_tol_1], axis=1).max(axis=1)
    gap_up = body_lo > (prev_body_hi + gap_tol_star)
    gap_down = body_hi < (prev_body_lo - gap_tol_star)
    small_body = out["body_to_range"] <= spinning_max_body_to_range
    out["pat_star_gap_up"] = (small_body & gap_up).fillna(False).astype(int)
    out["pat_star_gap_down"] = (small_body & gap_down).fillna(False).astype(int)
    out["pat_star"] = ((out["pat_star_gap_up"] == 1) | (out["pat_star_gap_down"] == 1)).astype(int)

    # --- Single-candle patterns ---
    # Hammer / Hanging man share shape; trend context differentiates.
    hammer_shape = (
        (out["body_to_range"] <= spinning_max_body_to_range)
        & (out["lower_shadow"] >= hammer_min_shadow_to_body * out["body"].replace(0, np.nan))
        & (out["upper_shadow"] <= small_shadow_max_to_body * out["body"].replace(0, np.nan))
    ).fillna(False)
    out["pat_hammer"] = (hammer_shape & _downtrend_before(offset=1)).astype(int)
    out["pat_hanging_man"] = (hammer_shape & _uptrend_before(offset=1)).astype(int)

    inv_hammer_shape = (
        (out["body_to_range"] <= spinning_max_body_to_range)
        & (out["upper_shadow"] >= hammer_min_shadow_to_body * out["body"].replace(0, np.nan))
        & (out["lower_shadow"] <= small_shadow_max_to_body * out["body"].replace(0, np.nan))
    ).fillna(False)
    out["pat_inverted_hammer"] = (inv_hammer_shape & _downtrend_before(offset=1)).astype(int)
    out["pat_shooting_star"] = (inv_hammer_shape & _uptrend_before(offset=1)).astype(int)

    # Dragonfly / Gravestone doji (doji subtypes)
    out["pat_dragonfly_doji"] = (
        (out["pat_doji"] == 1)
        & (out["upper_to_range"] <= 0.10)
        & (out["lower_to_range"] >= 0.60)
        & _downtrend_before(offset=1)
    ).fillna(False).astype(int)

    out["pat_gravestone_doji"] = (
        (out["pat_doji"] == 1)
        & (out["lower_to_range"] <= 0.10)
        & (out["upper_to_range"] >= 0.60)
        & _uptrend_before(offset=1)
    ).fillna(False).astype(int)

    # Bullish/Bearish spinning tops (context-specific variants)
    out["pat_bullish_spinning_top"] = ((out["pat_spinning_top"] == 1) & _downtrend_before(offset=1)).astype(int)
    out["pat_bearish_spinning_top"] = ((out["pat_spinning_top"] == 1) & _uptrend_before(offset=1)).astype(int)

    # --- Double-candle patterns (flag on candle 2) ---
    o1 = o.groupby(out["symbol"]).shift(1)
    h1 = h.groupby(out["symbol"]).shift(1)
    l1 = l.groupby(out["symbol"]).shift(1)
    c1 = c.groupby(out["symbol"]).shift(1)
    body1 = (c1 - o1).abs()
    body2 = out["body"]

    candle1_bear = c1 < o1
    candle1_bull = c1 > o1
    candle2_bull = c > o
    candle2_bear = c < o

    # Trend before candle 1 for 2-candle patterns: use offset=2 (close before candle1).
    downtrend_before_2 = _downtrend_before(offset=2)
    uptrend_before_2 = _uptrend_before(offset=2)

    # Engulfing (real body engulf)
    out["pat_bullish_engulfing"] = (
        downtrend_before_2
        & candle1_bear
        & candle2_bull
        & (o <= c1)
        & (c >= o1)
    ).fillna(False).astype(int)

    out["pat_bearish_engulfing"] = (
        uptrend_before_2
        & candle1_bull
        & candle2_bear
        & (o >= c1)
        & (c <= o1)
    ).fillna(False).astype(int)

    # Harami (body inside prior body)
    body1_hi = pd.concat([o1, c1], axis=1).max(axis=1)
    body1_lo = pd.concat([o1, c1], axis=1).min(axis=1)
    body2_hi = pd.concat([o, c], axis=1).max(axis=1)
    body2_lo = pd.concat([o, c], axis=1).min(axis=1)

    out["pat_bullish_harami"] = (
        downtrend_before_2
        & candle1_bear
        & candle2_bull
        & (body2_lo > body1_lo)
        & (body2_hi < body1_hi)
    ).fillna(False).astype(int)

    out["pat_bearish_harami"] = (
        uptrend_before_2
        & candle1_bull
        & candle2_bear
        & (body2_lo > body1_lo)
        & (body2_hi < body1_hi)
    ).fillna(False).astype(int)

    # Piercing line
    midpoint_1 = (o1 + c1) / 2.0
    out["pat_piercing_line"] = (
        downtrend_before_2
        & candle1_bear
        & (body1 / (h1 - l1).replace(0, np.nan) >= long_body_min_body_to_range)
        & candle2_bull
        & (o < c1)
        & (c > midpoint_1)
        & (c < o1)
    ).fillna(False).astype(int)

    # Dark cloud line
    out["pat_dark_cloud_line"] = (
        uptrend_before_2
        & candle1_bull
        & (body1 / (h1 - l1).replace(0, np.nan) >= long_body_min_body_to_range)
        & candle2_bear
        & (o > c1)
        & (c < midpoint_1)
        & (c > o1)
    ).fillna(False).astype(int)

    # Tweezers
    tol = out["_eq_tol"]
    out["pat_tweezer_bottom"] = (
        downtrend_before_2
        & _approx_equal(l1, l, tol)
        & candle1_bear
        & candle2_bull
    ).fillna(False).astype(int)
    out["pat_tweezer_top"] = (
        uptrend_before_2
        & _approx_equal(h1, h, tol)
        & candle1_bull
        & candle2_bear
    ).fillna(False).astype(int)

    # Kickers (gap + strong reversal candle)
    # Bullish: bearish candle followed by bullish candle gapping above prior real body.
    prior_body_hi = body1_hi
    prior_body_lo = body1_lo
    gap_tol_kicker = gap_tol_star
    out["pat_bullish_kicker"] = (
        downtrend_before_2
        & candle1_bear
        & candle2_bull
        & (body_lo > (prior_body_hi + gap_tol_kicker))
        & (body2 / out["range"].replace(0, np.nan) >= long_body_min_body_to_range)
    ).fillna(False).astype(int)
    out["pat_bearish_kicker"] = (
        uptrend_before_2
        & candle1_bull
        & candle2_bear
        & (body_hi < (prior_body_lo - gap_tol_kicker))
        & (body2 / out["range"].replace(0, np.nan) >= long_body_min_body_to_range)
    ).fillna(False).astype(int)

    # --- Triple-candle patterns (flag on candle 3) ---
    # Shifted candles: t-2, t-1, t
    o2 = o.groupby(out["symbol"]).shift(2)
    h2 = h.groupby(out["symbol"]).shift(2)
    l2 = l.groupby(out["symbol"]).shift(2)
    c2 = c.groupby(out["symbol"]).shift(2)

    o3 = o.groupby(out["symbol"]).shift(0)
    c3 = c

    body_c1 = (c2 - o2).abs()
    rng_c1 = (h2 - l2).replace(0, np.nan)

    # Trend before candle 1 for 3-candle patterns: offset=3 (close before candle1).
    downtrend_before_3 = _downtrend_before(offset=3)
    uptrend_before_3 = _uptrend_before(offset=3)

    # Candle 2 (t-1)
    o_mid = o1
    c_mid = c1
    rng_mid = (h1 - l1).replace(0, np.nan)
    body_mid = body1
    small_mid = (body_mid / rng_mid <= spinning_max_body_to_range)
    mid_is_doji = (body_mid / rng_mid <= doji_max_body_to_range)

    # Candle 1 (t-2)
    c1_bear = c2 < o2
    c1_bull = c2 > o2
    c1_long = (body_c1 / rng_c1 >= long_body_min_body_to_range)
    midpoint_c1 = (o2 + c2) / 2.0

    # Candle 3 (t)
    c3_bull = c3 > o3
    c3_bear = c3 < o3
    body_c3 = (c3 - o3).abs()
    rng_c3 = (h - l).replace(0, np.nan)
    c3_long = (body_c3 / rng_c3 >= long_body_min_body_to_range)

    out["pat_morning_star"] = (
        downtrend_before_3
        & c1_bear
        & c1_long
        & small_mid
        & c3_bull
        & c3_long
        & (c3 > midpoint_c1)
    ).fillna(False).astype(int)

    out["pat_morning_doji_star"] = (
        downtrend_before_3
        & c1_bear
        & c1_long
        & mid_is_doji
        & c3_bull
        & c3_long
        & (c3 > midpoint_c1)
    ).fillna(False).astype(int)

    out["pat_evening_star"] = (
        uptrend_before_3
        & c1_bull
        & c1_long
        & small_mid
        & c3_bear
        & c3_long
        & (c3 < midpoint_c1)
    ).fillna(False).astype(int)

    out["pat_evening_doji_star"] = (
        uptrend_before_3
        & c1_bull
        & c1_long
        & mid_is_doji
        & c3_bear
        & c3_long
        & (c3 < midpoint_c1)
    ).fillna(False).astype(int)

    # Abandoned baby requires gaps around the doji.
    # Use real-body gaps (more robust than high/low gaps across assets).
    c1_body_hi = pd.concat([o2, c2], axis=1).max(axis=1)
    c1_body_lo = pd.concat([o2, c2], axis=1).min(axis=1)
    mid_body_hi = pd.concat([o_mid, c_mid], axis=1).max(axis=1)
    mid_body_lo = pd.concat([o_mid, c_mid], axis=1).min(axis=1)
    c3_body_hi = pd.concat([o3, c3], axis=1).max(axis=1)
    c3_body_lo = pd.concat([o3, c3], axis=1).min(axis=1)

    gap_tol_2 = out.groupby("symbol")["_gap_tol"].shift(2)
    gap_tol_12 = pd.concat([gap_tol_1, gap_tol_2], axis=1).max(axis=1)
    gap_tol_01 = pd.concat([gap_tol_0, gap_tol_1], axis=1).max(axis=1)

    gap1_down = mid_body_hi < (c1_body_lo - gap_tol_12)
    gap2_up = c3_body_lo > (mid_body_hi + gap_tol_01)
    gap1_up = mid_body_lo > (c1_body_hi + gap_tol_12)
    gap2_down = c3_body_hi < (mid_body_lo - gap_tol_01)

    out["pat_bullish_abandoned_baby"] = (
        downtrend_before_3 & c1_bear & mid_is_doji & gap1_down & gap2_up & c3_bull
    ).fillna(False).astype(int)
    out["pat_bearish_abandoned_baby"] = (
        uptrend_before_3 & c1_bull & mid_is_doji & gap1_up & gap2_down & c3_bear
    ).fillna(False).astype(int)

    # Three white soldiers / three black crows
    # Shifted for three candles ending at t: t-2, t-1, t
    close_t2 = c2
    close_t1 = c1
    close_t0 = c
    open_t2 = o2
    open_t1 = o1
    open_t0 = o

    bull_t2 = close_t2 > open_t2
    bull_t1 = close_t1 > open_t1
    bull_t0 = close_t0 > open_t0
    bear_t2 = close_t2 < open_t2
    bear_t1 = close_t1 < open_t1
    bear_t0 = close_t0 < open_t0

    rising_closes = (close_t0 > close_t1) & (close_t1 > close_t2)
    falling_closes = (close_t0 < close_t1) & (close_t1 < close_t2)

    # Opens within prior real body
    body2_hi = pd.concat([open_t2, close_t2], axis=1).max(axis=1)
    body2_lo = pd.concat([open_t2, close_t2], axis=1).min(axis=1)
    body1_hi = pd.concat([open_t1, close_t1], axis=1).max(axis=1)
    body1_lo = pd.concat([open_t1, close_t1], axis=1).min(axis=1)
    open1_in_body2 = (open_t1 >= body2_lo) & (open_t1 <= body2_hi)
    open0_in_body1 = (open_t0 >= body1_lo) & (open_t0 <= body1_hi)

    out["pat_three_white_soldiers"] = (
        bull_t2
        & bull_t1
        & bull_t0
        & rising_closes
        & open1_in_body2
        & open0_in_body1
    ).fillna(False).astype(int)

    out["pat_three_black_crows"] = (
        bear_t2
        & bear_t1
        & bear_t0
        & falling_closes
        & open1_in_body2
        & open0_in_body1
    ).fillna(False).astype(int)

    # --- 4-candle patterns: Three Line Strike (flag on candle 4) ---
    o3p = o.groupby(out["symbol"]).shift(3)
    c3p = c.groupby(out["symbol"]).shift(3)
    o2p = o2
    c2p = c2
    o1p = o1
    c1p = c1
    o0p = o
    c0p = c

    bull_3 = c3p > o3p
    bull_2 = c2p > o2p
    bull_1 = c1p > o1p
    bear_3 = c3p < o3p
    bear_2 = c2p < o2p
    bear_1 = c1p < o1p

    rising_3 = (c1p > c2p) & (c2p > c3p)
    falling_3 = (c1p < c2p) & (c2p < c3p)

    # Continuation context:
    # - Bullish TLS: prior 3 bullish, then big bearish engulfing below first open.
    # - Bearish TLS: prior 3 bearish, then big bullish engulfing above first open.
    uptrend_before_4 = _uptrend_before(offset=4)
    downtrend_before_4 = _downtrend_before(offset=4)

    out["pat_bullish_three_line_strike"] = (
        uptrend_before_4
        & bull_3
        & bull_2
        & bull_1
        & rising_3
        & (c0p < o0p)  # 4th candle bearish
        & (o0p >= c1p)
        & (c0p < o3p)
    ).fillna(False).astype(int)

    out["pat_bearish_three_line_strike"] = (
        downtrend_before_4
        & bear_3
        & bear_2
        & bear_1
        & falling_3
        & (c0p > o0p)  # 4th candle bullish
        & (o0p <= c1p)
        & (c0p > o3p)
    ).fillna(False).astype(int)

    # --- Confirmations (3 candles, flag on candle 3) ---
    # Three Inside/Outside Up/Down
    # Reuse candle1 (t-2), candle2 (t-1), candle3 (t)
    # Note: our Harami/Engulfing flags are emitted on the *second* candle of the 2-candle pattern,
    # so for 3-candle confirmations we reference the prior row (shift(1)).
    bullish_harami_prev = out.groupby("symbol")["pat_bullish_harami"].shift(1).fillna(0).astype(int)
    bearish_harami_prev = out.groupby("symbol")["pat_bearish_harami"].shift(1).fillna(0).astype(int)
    bullish_engulfing_prev = out.groupby("symbol")["pat_bullish_engulfing"].shift(1).fillna(0).astype(int)
    bearish_engulfing_prev = out.groupby("symbol")["pat_bearish_engulfing"].shift(1).fillna(0).astype(int)
    out["pat_three_inside_up"] = (
        downtrend_before_3
        & c1_bear
        & c1_long
        & (bullish_harami_prev == 1)  # harami on candle2
        & c3_bull
        & (c3 > o2)
    ).fillna(False).astype(int)

    out["pat_three_outside_up"] = (
        downtrend_before_3
        & c1_bear
        & (bullish_engulfing_prev == 1)  # engulfing on candle2
        & c3_bull
        & (c3 > c1)
    ).fillna(False).astype(int)

    out["pat_three_inside_down"] = (
        uptrend_before_3
        & c1_bull
        & c1_long
        & (bearish_harami_prev == 1)
        & c3_bear
        & (c3 < o2)
    ).fillna(False).astype(int)

    out["pat_three_outside_down"] = (
        uptrend_before_3
        & c1_bull
        & (bearish_engulfing_prev == 1)
        & c3_bear
        & (c3 < c1)
    ).fillna(False).astype(int)

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
        tickers.append(symbol.replace(".", "-"))

    tickers = list(dict.fromkeys(tickers))

    max_workers = _get_max_workers()
    mdc.write_line(f"Candlestick feature engineering configured for {len(tickers)} tickers (max_workers={max_workers})")

    return FeatureJobConfig(
        silver_container=silver_container,
        gold_container=gold_container,
        max_workers=max_workers,
        tickers=tickers,
    )


def main() -> int:
    from core import core as mdc
    from core import delta_core
    from core.pipeline import DataPaths

    mdc.log_environment_diagnostics()
    job_cfg = _build_job_config()

    watermarks = load_watermarks("gold_candlesticks")
    watermarks_dirty = False

    tasks: List[Tuple[str, str, str, str, str]] = []
    commit_map: Dict[str, float | None] = {}
    skipped_unchanged = 0

    for ticker in job_cfg.tickers:
        raw_path = DataPaths.get_market_data_path(ticker)
        gold_path = DataPaths.get_gold_candlesticks_path(ticker)

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

    mdc.write_line("Starting candlestick feature engineering pool...")
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
        f"Candlestick feature engineering complete: ok={ok}, skipped_no_data={skipped}, "
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
            save_watermarks("gold_candlesticks", watermarks)

    return 0 if failed == 0 else 1


def discover_year_months_from_data(*, container: Optional[str] = None, max_tickers: Optional[int] = None) -> List[str]:
    """Discover available year-month partitions from per-ticker candlestick tables."""

    from core.core import write_line
    from core.delta_core import load_delta
    from core.pipeline import DataPaths

    container = _resolve_gold_container(container)

    tickers_from_container = _try_load_tickers_from_container(container, root_prefix="candlesticks")
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "container_listing"

    if max_tickers is not None:
        tickers = tickers[: max_tickers]

    if not tickers:
        write_line(
            f"No per-ticker candlestick tables found (source={ticker_source}); no year_months discovered."
        )
        return []

    year_months: set[str] = set()
    for ticker in tickers:
        src_path = DataPaths.get_gold_candlesticks_path(ticker)
        df = load_delta(container, src_path, columns=["date"])
        if df is None or df.empty or "date" not in df.columns:
            continue

        ts = pd.to_datetime(df["date"], errors="coerce").dropna()
        if ts.empty:
            continue
        # Normalize to day boundary to match by-date partitioning.
        for value in ts.dt.normalize().dt.strftime("%Y-%m").unique().tolist():
            if value:
                year_months.add(str(value))

    discovered = sorted(year_months)
    write_line(
        f"Discovered {len(discovered)} year_month(s) from gold candlestick tables in {container}."
    )
    return discovered


def materialize_candlesticks_by_date(cfg: MaterializeConfig) -> int:
    """Materialize `candlesticks_by_date` for a given year_month."""

    from core.core import write_line
    from core.delta_core import load_delta, store_delta
    from core.pipeline import DataPaths

    start, end = _parse_year_month_bounds(cfg.year_month)

    tickers_from_container = _try_load_tickers_from_container(cfg.container, root_prefix="candlesticks")
    if tickers_from_container is None:
        tickers = _load_ticker_universe()
        ticker_source = "symbol_universe"
    else:
        tickers = tickers_from_container
        ticker_source = "container_listing"

    if cfg.max_tickers is not None:
        tickers = tickers[: cfg.max_tickers]

    write_line(
        f"Materializing candlesticks_by_date for {cfg.year_month}: container={cfg.container} "
        f"tickers={len(tickers)} ticker_source={ticker_source} output_path={cfg.output_path}"
    )

    if not tickers:
        write_line(f"No per-ticker candlestick tables found (source={ticker_source}); nothing to materialize.")
        return 0

    frames: List[pd.DataFrame] = []
    for ticker in tickers:
        src_path = DataPaths.get_gold_candlesticks_path(ticker)
        df = load_delta(
            cfg.container,
            src_path,
            filters=[("date", ">=", start.to_pydatetime()), ("date", "<", end.to_pydatetime())],
        )
        if df is None or df.empty:
            continue
        frames.append(df)

    if not frames:
        write_line(f"No candlestick rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out = pd.concat(frames, ignore_index=True)
    if "date" not in out.columns:
        write_line(f"No 'date' column found while materializing {cfg.year_month}; nothing to materialize.")
        return 0

    # Forward-compatible with intraday: preserve timestamp while partitioning by day.
    out["timestamp"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["timestamp"])
    if out.empty:
        write_line(f"No valid timestamp rows found for {cfg.year_month}; nothing to materialize.")
        return 0

    out["date"] = out["timestamp"].dt.normalize()
    out["year_month"] = out["date"].dt.strftime("%Y-%m")
    out = out[out["year_month"] == cfg.year_month].copy()
    if out.empty:
        write_line(f"No rows remain after year_month filter for {cfg.year_month}; nothing to materialize.")
        return 0

    predicate = f"year_month = '{cfg.year_month}'"
    store_delta(
        out,
        container=cfg.container,
        path=cfg.output_path,
        mode="overwrite",
        partition_by=["year_month", "date"],
        merge_schema=True,
        predicate=predicate,
    )

    write_line(f"Materialized {len(out)} row(s) into {cfg.container}/{cfg.output_path} ({cfg.year_month}).")
    return 0


def by_date_main(argv: Optional[List[str]] = None) -> int:
    cfg = _build_materialize_config(argv)
    return materialize_candlesticks_by_date(cfg)


if __name__ == "__main__":
    from core.by_date_pipeline import run_partner_then_by_date
    from tasks.common.job_trigger import trigger_next_job_from_env

    job_name = "gold-candlesticks-job"

    def _year_months_provider() -> List[str]:
        return discover_year_months_from_data(container=os.environ.get("AZURE_CONTAINER_GOLD"))

    exit_code = run_partner_then_by_date(
        job_name=job_name,
        partner_main=main,
        by_date_main=by_date_main,
        year_months_provider=_year_months_provider,
    )
    if exit_code == 0:
        trigger_next_job_from_env()
    raise SystemExit(exit_code)
