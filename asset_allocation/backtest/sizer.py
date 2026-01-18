from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable

import numpy as np
import pandas as pd

from asset_allocation.backtest.models import PortfolioSnapshot
from asset_allocation.backtest.strategy import StrategyDecision


@dataclass(frozen=True)
class TargetWeights:
    weights: Dict[str, float]


class Sizer(ABC):
    @abstractmethod
    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
    ) -> TargetWeights:
        raise NotImplementedError


class EqualWeightSizer(Sizer):
    def __init__(self, *, max_positions: int = 10):
        self._max_positions = int(max_positions)

    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
    ) -> TargetWeights:
        selected: Iterable[str] = [s for s, score in decision.scores.items() if score and score > 0]
        selected_list = list(selected)[: max(0, self._max_positions)]
        if not selected_list:
            return TargetWeights(weights={})
        weight = 1.0 / len(selected_list)
        return TargetWeights(weights={symbol: weight for symbol in selected_list})


def _safe_float(value: object) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _build_mu(decision: StrategyDecision, *, mu_scale: float) -> pd.Series:
    rows = {}
    for symbol, score in (decision.scores or {}).items():
        score_f = _safe_float(score)
        if score_f is None:
            continue
        rows[str(symbol)] = score_f * mu_scale
    return pd.Series(rows, dtype="float64")


def _compute_close_returns(
    prices: pd.DataFrame, *, symbols: list[str], as_of: date, lookback_days: int
) -> pd.DataFrame:
    if prices is None or prices.empty:
        return pd.DataFrame()

    df = prices.copy()
    if "date" not in df.columns or "symbol" not in df.columns or "close" not in df.columns:
        raise ValueError("prices must include columns: date, symbol, close")

    df = df[df["date"] <= as_of]
    df = df[df["symbol"].astype(str).isin(set(symbols))]
    if df.empty:
        return pd.DataFrame()

    df = df[["date", "symbol", "close"]].copy()
    df["symbol"] = df["symbol"].astype(str)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "symbol", "close"])
    if df.empty:
        return pd.DataFrame()

    close = df.pivot_table(index="date", columns="symbol", values="close", aggfunc="last").sort_index()
    returns = close.pct_change().dropna(how="all")
    if returns.empty:
        return pd.DataFrame()

    window = returns.tail(max(0, int(lookback_days)))
    # Require aligned samples for a well-defined covariance matrix.
    window = window.dropna(how="any")
    return window


def _compute_covariance(returns: pd.DataFrame) -> pd.DataFrame:
    if returns is None or returns.empty:
        return pd.DataFrame()
    cov = returns.cov(ddof=0)
    cov = cov.dropna(axis=0, how="all").dropna(axis=1, how="all")
    return cov


class KellySizer(Sizer):
    """
    Kelly sizing: w = kelly_fraction * pinv(Sigma) @ mu

    - mu is derived from StrategyDecision.scores via mu_scale (expected daily return per score unit).
    - Sigma is estimated from trailing close-to-close returns over lookback_days, using only dates <= as_of.
    - Output weights are unconstrained; Constraints.apply(...) should enforce leverage/position caps.
    """

    def __init__(
        self,
        *,
        kelly_fraction: float = 0.5,
        lookback_days: int = 20,
        mu_scale: float,
    ):
        kf = float(kelly_fraction)
        if math.isnan(kf) or math.isinf(kf) or not (0.0 <= kf <= 1.0):
            raise ValueError("kelly_fraction must be in [0, 1].")
        lb = int(lookback_days)
        if lb < 2:
            raise ValueError("lookback_days must be >= 2.")
        ms = float(mu_scale)
        if math.isnan(ms) or math.isinf(ms):
            raise ValueError("mu_scale must be a finite float.")

        self._kelly_fraction = kf
        self._lookback_days = lb
        self._mu_scale = ms

    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
    ) -> TargetWeights:
        if self._kelly_fraction == 0.0:
            return TargetWeights(weights={})

        mu = _build_mu(decision, mu_scale=self._mu_scale)
        mu = mu.dropna()
        if mu.empty:
            return TargetWeights(weights={})

        returns = _compute_close_returns(
            prices,
            symbols=list(mu.index),
            as_of=as_of,
            lookback_days=self._lookback_days,
        )
        cov = _compute_covariance(returns)
        if cov.empty:
            return TargetWeights(weights={})

        # Align to the covariance matrix universe (drop symbols without usable risk data).
        mu = mu.reindex(cov.index).dropna()
        cov = cov.reindex(index=mu.index, columns=mu.index)
        if mu.empty or cov.empty:
            return TargetWeights(weights={})

        inv_cov = np.linalg.pinv(cov.to_numpy(dtype="float64", copy=False))
        raw = inv_cov @ mu.to_numpy(dtype="float64", copy=False)
        weights = pd.Series(raw, index=mu.index, dtype="float64") * self._kelly_fraction

        out: Dict[str, float] = {}
        for symbol, weight in weights.items():
            w = float(weight)
            if abs(w) < 1e-12:
                continue
            out[str(symbol)] = w
        return TargetWeights(weights=out)
