from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

from asset_allocation.backtest.models import PortfolioSnapshot
from asset_allocation.backtest.strategy import StrategyDecision
from asset_allocation.backtest.optimization import Optimizer


@dataclass(frozen=True)
class TargetWeights:
    weights: Dict[str, float]


class Sizer(ABC):
    def __init__(self, optimizer: Optional[Optimizer] = None):
        self.optimizer = optimizer

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
    def __init__(self, *, max_positions: int = 10, optimizer: Optional[Optimizer] = None):
        super().__init__(optimizer=optimizer)
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


class OptimizationSizer(Sizer):
    """
    Sizer that uses an Optimizer to determine weights.
    Requires Strategy to provide Expected Returns (scores) and optionally Covariance.
    """
    def __init__(self, optimizer: Optimizer, lookback_days: int = 252):
        super().__init__(optimizer=optimizer)
        if optimizer is None:
            raise ValueError("OptimizationSizer requires an optimizer instance.")
        self.lookback_days = int(lookback_days)

    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
    ) -> TargetWeights:
        # Filter universe to symbols with positive scores (Long Only for now)
        universe = [s for s, score in decision.scores.items() if score is not None]
        if not universe:
            return TargetWeights(weights={})

        # Estimate Expected Returns (from Strategy Scores)
        # We treat the raw strategy scores as 'Alpha' or 'Expected Return' for MVO
        expected_returns = pd.Series({s: decision.scores[s] for s in universe})

        # Estimate Covariance Matrix (Historical)
        # Filter prices to universe and lookback window
        window_start = as_of - pd.Timedelta(days=self.lookback_days * 2) # buffer for holidays
        recent_prices = prices[
            (prices["date"] >= pd.Timestamp(window_start)) & 
            (prices["date"] <= pd.Timestamp(as_of)) &
            (prices["symbol"].isin(universe))
        ]
        
        # pivot to wide format: Date x Symbol
        pivot_prices = recent_prices.pivot(index="date", columns="symbol", values="close")
        if pivot_prices.empty:
            return TargetWeights(weights={})
            
        returns = pivot_prices.pct_change().dropna()
        if returns.empty:
            return TargetWeights(weights={})
            
        cov_matrix = returns.cov()

        # Run Optimization
        target_weights = self.optimizer.optimize(
            universe=universe,
            expected_returns=expected_returns,
            covariance_matrix=cov_matrix,
            current_weights=portfolio.positions  # Pass current positions for turnover logic (future)
        )

        return TargetWeights(weights=target_weights)


def _safe_float(value: object) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _build_mu(decision: StrategyDecision, *, mu_scale: float) -> pd.Series:
    """Conventionalize scores into a vector of return expectations."""
    # This helper was likely from a previous version, preserved for compatibility if needed.
    return pd.Series(decision.scores) * mu_scale
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


class LongShortScoreSizer(Sizer):
    """
    Long/short sizing using signed strategy scores:
      - score > 0 => long candidate
      - score < 0 => short candidate

    Produces target weights that (when both sides are present) approximately match:
      long_gross  = (gross_target + net_target) / 2
      short_gross = (gross_target - net_target) / 2

    Within each side, weights can be equal-weighted or score-weighted.
    Optional StrategyDecision.scales can reduce exposure for specific symbols (e.g., partial exits).
    """

    def __init__(
        self,
        *,
        max_longs: int = 10,
        max_shorts: int = 10,
        gross_target: float = 1.0,
        net_target: float = 0.0,
        weight_mode: str = "equal",
        sticky_holdings: bool = True,
        score_power: float = 1.0,
        min_abs_score: float = 0.0,
    ):
        self._max_longs = int(max_longs)
        self._max_shorts = int(max_shorts)

        gross = float(gross_target)
        net = float(net_target)
        if math.isnan(gross) or math.isinf(gross) or gross <= 0:
            raise ValueError("gross_target must be > 0.")
        if math.isnan(net) or math.isinf(net) or abs(net) > gross:
            raise ValueError("net_target must be finite and satisfy abs(net_target) <= gross_target.")
        self._gross_target = gross
        self._net_target = net

        mode = str(weight_mode).strip().lower()
        if mode not in {"equal", "score"}:
            raise ValueError("weight_mode must be 'equal' or 'score'.")
        self._weight_mode = mode
        self._sticky_holdings = bool(sticky_holdings)

        sp = float(score_power)
        if math.isnan(sp) or math.isinf(sp) or sp <= 0:
            raise ValueError("score_power must be > 0.")
        self._score_power = sp

        mas = float(min_abs_score)
        if math.isnan(mas) or math.isinf(mas) or mas < 0:
            raise ValueError("min_abs_score must be >= 0.")
        self._min_abs_score = mas

    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
    ) -> TargetWeights:
        scores = decision.scores or {}
        scales = decision.scales or {}

        long_items: list[tuple[str, float]] = []
        short_items: list[tuple[str, float]] = []

        for symbol, score in scores.items():
            score_f = _safe_float(score)
            if score_f is None or abs(score_f) <= 0:
                continue
            if abs(score_f) < self._min_abs_score:
                continue
            sym = str(symbol)
            if score_f > 0:
                long_items.append((sym, score_f))
            else:
                short_items.append((sym, score_f))

        long_items.sort(key=lambda x: x[1], reverse=True)
        short_items.sort(key=lambda x: x[1])  # more negative first

        def _select_side(
            *,
            candidates: list[tuple[str, float]],
            held: set[str],
            max_positions: int,
            side: str,
        ) -> list[tuple[str, float]]:
            if max_positions <= 0:
                return []
            if not self._sticky_holdings or not held:
                return candidates[:max_positions]

            pinned: list[tuple[str, float]] = []
            remainder: list[tuple[str, float]] = []
            held = {str(s) for s in held}
            for sym, score_val in candidates:
                if sym in held:
                    pinned.append((sym, score_val))
                else:
                    remainder.append((sym, score_val))

            # If we have more pinned holdings than capacity, keep the strongest.
            if len(pinned) > max_positions:
                if side == "long":
                    pinned.sort(key=lambda x: x[1], reverse=True)
                else:
                    pinned.sort(key=lambda x: x[1])  # more negative first
                return pinned[:max_positions]

            slots = max_positions - len(pinned)
            return pinned + remainder[:slots]

        held_longs = {s for s, sh in (portfolio.positions or {}).items() if float(sh) > 0}
        held_shorts = {s for s, sh in (portfolio.positions or {}).items() if float(sh) < 0}

        long_selected = _select_side(
            candidates=long_items, held=held_longs, max_positions=max(0, self._max_longs), side="long"
        )
        short_selected = _select_side(
            candidates=short_items, held=held_shorts, max_positions=max(0, self._max_shorts), side="short"
        )

        long_gross = (self._gross_target + self._net_target) / 2.0
        short_gross = (self._gross_target - self._net_target) / 2.0
        long_gross = max(0.0, float(long_gross))
        short_gross = max(0.0, float(short_gross))

        weights: Dict[str, float] = {}

        def _alloc_side(
            *,
            items: list[tuple[str, float]],
            gross_budget: float,
            side: str,
        ) -> Dict[str, float]:
            if gross_budget <= 0 or not items:
                return {}

            raw: Dict[str, float] = {}
            for sym, score_val in items:
                scale = _safe_float(scales.get(sym, 1.0))
                if scale is None:
                    scale = 1.0
                if scale <= 0:
                    continue
                if self._weight_mode == "equal":
                    base = 1.0
                else:
                    base = abs(float(score_val)) ** self._score_power
                raw[sym] = base * float(scale)

            total = sum(raw.values())
            if total <= 0:
                return {}

            if side == "long":
                return {s: (v / total) * gross_budget for s, v in raw.items()}
            return {s: -(v / total) * gross_budget for s, v in raw.items()}

        weights.update(_alloc_side(items=long_selected, gross_budget=long_gross, side="long"))
        weights.update(_alloc_side(items=short_selected, gross_budget=short_gross, side="short"))

        # Drop tiny weights to reduce churn.
        weights = {s: float(w) for s, w in weights.items() if abs(float(w)) >= 1e-12}
        return TargetWeights(weights=weights)
