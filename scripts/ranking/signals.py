"""
Derived ranking signals built from the canonical rankings Delta table.

This module intentionally keeps derived tables in a fixed (long) schema to avoid
schema churn when strategies are added/removed. The UI can pivot a small slice
at query time.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from scripts.common.core import write_line
from scripts.common.delta_core import load_delta, store_delta


DEFAULT_TOP_N = 50
CANONICAL_RANKINGS_PATH = "platinum/rankings"
RANKING_SIGNALS_PATH = "gold/ranking_signals"
COMPOSITE_SIGNALS_PATH = "gold/composite_signals"


def _ensure_required_columns(df: pd.DataFrame, required: Sequence[str], label: str) -> None:
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{label} is missing required columns: {missing}")


def _parse_year_month_bounds(year_month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    try:
        start = pd.Timestamp(f"{year_month}-01")
    except Exception as exc:
        raise ValueError(f"Invalid year_month '{year_month}'. Expected YYYY-MM.") from exc

    end = start + pd.offsets.MonthBegin(1)
    return start, end


def _derive_year_month(dates: pd.Series) -> pd.Series:
    normalized = pd.to_datetime(dates, errors="coerce")
    return normalized.dt.strftime("%Y-%m")


def get_strategy_weights(
    strategies: Sequence[str],
    *,
    env_var: str = "RANKING_COMPOSITE_STRATEGY_WEIGHTS",
) -> Dict[str, float]:
    """
    Returns a dict of strategy -> weight.

    - If env var is unset, defaults to equal weights across all provided strategies.
    - If env var is set, it must be a JSON object containing a weight for each strategy.
    """

    strategies = [str(s) for s in strategies]
    raw = os.environ.get(env_var)
    if not raw:
        return {s: 1.0 for s in strategies}

    try:
        weights = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{env_var} must be valid JSON (got: {raw!r}).") from exc

    if not isinstance(weights, dict):
        raise ValueError(f"{env_var} must be a JSON object mapping strategy->weight.")

    missing = [s for s in strategies if s not in weights]
    if missing:
        raise ValueError(f"{env_var} missing weights for strategies: {missing}")

    normalized: Dict[str, float] = {}
    for strategy in strategies:
        value = weights[strategy]
        try:
            normalized[strategy] = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{env_var} weight for {strategy} must be numeric (got: {value!r}).") from exc

    if sum(normalized.values()) <= 0:
        raise ValueError(f"{env_var} weights must sum to > 0.")

    return normalized


def compute_ranking_signals(rankings: pd.DataFrame) -> pd.DataFrame:
    """
    Builds per-strategy rank percentiles from canonical rankings.

    Expected input columns: date, symbol, strategy, rank (score optional).
    """

    if rankings is None or rankings.empty:
        return pd.DataFrame()

    required = ["date", "symbol", "strategy", "rank"]
    _ensure_required_columns(rankings, required, "Canonical rankings")

    working = rankings.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.dropna(subset=["date"])
    if working.empty:
        return pd.DataFrame()

    working["symbol"] = working["symbol"].astype(str)
    working["strategy"] = working["strategy"].astype(str)
    working["rank"] = pd.to_numeric(working["rank"], errors="coerce")
    working = working.dropna(subset=["rank"])
    if working.empty:
        return pd.DataFrame()

    working["rank"] = working["rank"].astype(int)
    working["year_month"] = _derive_year_month(working["date"])

    group_cols = ["date", "strategy"]
    counts = working.groupby(group_cols)["symbol"].transform("count")
    counts = counts.astype(int)

    denom = (counts - 1).replace(0, pd.NA).astype("Float64")
    percentiles = 1 - ((working["rank"] - 1) / denom)
    percentiles = percentiles.fillna(1.0).astype(float)

    working["n_symbols"] = counts
    working["rank_percentile"] = percentiles

    keep = ["date", "year_month", "symbol", "strategy", "rank", "rank_percentile", "n_symbols"]
    if "score" in working.columns:
        keep.append("score")

    return working[keep].reset_index(drop=True)


def compute_composite_signals(
    signals: pd.DataFrame,
    *,
    weights: Dict[str, float],
    top_n: int = DEFAULT_TOP_N,
) -> pd.DataFrame:
    """
    Builds composite percentiles and composite rank per date from per-strategy signals.
    """

    if signals is None or signals.empty:
        return pd.DataFrame()

    required = ["date", "year_month", "symbol", "strategy", "rank", "rank_percentile"]
    _ensure_required_columns(signals, required, "Ranking signals")

    working = signals.copy()
    working["date"] = pd.to_datetime(working["date"], errors="coerce")
    working = working.dropna(subset=["date"])
    if working.empty:
        return pd.DataFrame()

    working["symbol"] = working["symbol"].astype(str)
    working["strategy"] = working["strategy"].astype(str)

    missing_weights = sorted({s for s in working["strategy"].unique().tolist() if s not in weights})
    if missing_weights:
        raise ValueError(f"Missing weights for strategies: {missing_weights}")

    working["weight"] = working["strategy"].map(weights).astype(float)
    weight_sum = working.groupby(["date", "symbol"])["weight"].transform("sum")
    weighted_score = working["rank_percentile"].astype(float) * working["weight"]
    composite_percentile = weighted_score.groupby([working["date"], working["symbol"]]).transform("sum") / weight_sum

    composite = (
        working.assign(composite_percentile=composite_percentile)
        .groupby(["date", "year_month", "symbol"], as_index=False)
        .agg(
            composite_percentile=("composite_percentile", "first"),
            strategies_present=("strategy", "nunique"),
            strategies_hit=("rank", lambda s: int((pd.Series(s) <= top_n).sum())),
        )
    )

    composite["composite_rank"] = (
        composite.groupby("date")["composite_percentile"]
        .rank(method="dense", ascending=False)
        .astype(int)
    )

    return composite.sort_values(["date", "composite_rank", "symbol"]).reset_index(drop=True)


def load_canonical_rankings_for_year_month(container: str, year_month: str) -> pd.DataFrame:
    """
    Loads canonical rankings for the given year_month across all strategies.
    """

    start, end = _parse_year_month_bounds(year_month)
    df = load_delta(
        container,
        CANONICAL_RANKINGS_PATH,
        filters=[("date", ">=", start.to_pydatetime()), ("date", "<", end.to_pydatetime())],
    )
    return pd.DataFrame() if df is None else df


@dataclass(frozen=True)
class MaterializeResult:
    year_month: str
    rankings_rows: int
    signals_rows: int
    composite_rows: int


def materialize_signals_for_year_month(
    *,
    container: str,
    year_month: str,
    top_n: int = DEFAULT_TOP_N,
) -> MaterializeResult:
    """
    Materializes:
    - gold/ranking_signals (long, fixed schema)
    - gold/composite_signals (long, fixed schema)

    Overwrites only the given year_month partition (safe because year_month is the partition column).
    """

    rankings = load_canonical_rankings_for_year_month(container, year_month)
    if rankings.empty:
        write_line(f"No canonical rankings found for {year_month}; skipping materialization.")
        return MaterializeResult(year_month=year_month, rankings_rows=0, signals_rows=0, composite_rows=0)

    signals = compute_ranking_signals(rankings)
    if signals.empty:
        write_line(f"No ranking signals produced for {year_month}; skipping materialization.")
        return MaterializeResult(
            year_month=year_month,
            rankings_rows=len(rankings),
            signals_rows=0,
            composite_rows=0,
        )

    strategies = sorted(signals["strategy"].unique().tolist())
    weights = get_strategy_weights(strategies)
    composite = compute_composite_signals(signals, weights=weights, top_n=top_n)

    predicate = f"year_month = '{year_month}'"
    store_delta(
        signals,
        container=container,
        path=RANKING_SIGNALS_PATH,
        mode="overwrite",
        partition_by=["year_month"],
        merge_schema=True,
        predicate=predicate,
    )
    store_delta(
        composite,
        container=container,
        path=COMPOSITE_SIGNALS_PATH,
        mode="overwrite",
        partition_by=["year_month"],
        merge_schema=True,
        predicate=predicate,
    )

    return MaterializeResult(
        year_month=year_month,
        rankings_rows=len(rankings),
        signals_rows=len(signals),
        composite_rows=len(composite),
    )

