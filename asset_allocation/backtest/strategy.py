from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional

import pandas as pd

from asset_allocation.backtest.models import PortfolioSnapshot


@dataclass(frozen=True)
class StrategyDecision:
    scores: Dict[str, float]


class Strategy(ABC):
    @abstractmethod
    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> StrategyDecision:
        raise NotImplementedError


class BuyAndHoldStrategy(Strategy):
    """
    Toy strategy for Phase 1: long 100% in the first universe symbol after the first bar.
    """

    def __init__(self, *, symbol: str):
        self._symbol = symbol

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> StrategyDecision:
        return StrategyDecision(scores={self._symbol: 1.0})


class TopNSignalStrategy(Strategy):
    """
    Signal-driven strategy for Phase 2.

    Expects `signals` to include a numeric column (default: 'composite_percentile') and 'symbol'.
    Selects the Top-N symbols by that column on each decision date (close), to be executed at next open.
    """

    def __init__(
        self,
        *,
        signal_column: str = "composite_percentile",
        top_n: int = 10,
        min_signal: Optional[float] = None,
        higher_is_better: bool = True,
    ):
        self._signal_column = str(signal_column)
        self._top_n = int(top_n)
        self._min_signal = float(min_signal) if min_signal is not None else None
        self._higher_is_better = bool(higher_is_better)

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> StrategyDecision:
        if signals is None or signals.empty:
            return StrategyDecision(scores={})

        if "symbol" not in signals.columns:
            raise ValueError("signals must include 'symbol'.")
        if self._signal_column not in signals.columns:
            raise ValueError(f"signals missing required column: {self._signal_column!r}")

        df = signals[["symbol", self._signal_column]].copy()
        df["symbol"] = df["symbol"].astype(str)
        df["signal"] = pd.to_numeric(df[self._signal_column], errors="coerce")
        df = df.dropna(subset=["signal"])
        if df.empty:
            return StrategyDecision(scores={})

        df = df.drop_duplicates(subset=["symbol"], keep="last")
        if self._min_signal is not None:
            df = df[df["signal"] >= self._min_signal]
        if df.empty:
            return StrategyDecision(scores={})

        df = df.sort_values("signal", ascending=not self._higher_is_better)
        df = df.head(max(0, self._top_n))
        return StrategyDecision(scores={row["symbol"]: float(row["signal"]) for _, row in df.iterrows()})
