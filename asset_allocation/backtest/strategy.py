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
    def __init__(self, *, rebalance: str | int = "daily"):
        self._rebalance = rebalance
        self._last_rebalance_date: Optional[date] = None

    def check_rebalance(self, current_date: date) -> bool:
        """
        Determines if rebalancing should occur on the current date.
        Updates _last_rebalance_date if True.
        """
        if self._rebalance == "daily":
            return True

        # First run always rebalances
        if self._last_rebalance_date is None:
            self._last_rebalance_date = current_date
            return True

        should_run = False
        if isinstance(self._rebalance, int):
            days_diff = (current_date - self._last_rebalance_date).days
            should_run = days_diff >= self._rebalance
        elif self._rebalance == "weekly":
            # Rebalance if week changed or > 7 days gap
            # isocalendar()[1] is week number
            should_run = (
                current_date.isocalendar()[1] != self._last_rebalance_date.isocalendar()[1]
                or (current_date - self._last_rebalance_date).days >= 7
            )
        elif self._rebalance == "monthly":
            should_run = current_date.month != self._last_rebalance_date.month
        elif self._rebalance == "quarterly":
            # Change in quarter ((month-1)//3 + 1)
            curr_q = (current_date.month - 1) // 3 + 1
            last_q = (self._last_rebalance_date.month - 1) // 3 + 1
            should_run = curr_q != last_q or current_date.year != self._last_rebalance_date.year
        elif self._rebalance == "annually":
            should_run = current_date.year != self._last_rebalance_date.year
        else:
            # Fallback for unknown strings -> treat as daily usually, or error?
            # For now, safe default is True to avoid locking up account
            return True

        if should_run:
            self._last_rebalance_date = current_date
            return True
        return False

    @abstractmethod
    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        raise NotImplementedError


class BuyAndHoldStrategy(Strategy):
    """
    Toy strategy for Phase 1: long 100% in the first universe symbol after the first bar.
    """

    def __init__(self, *, symbol: str, rebalance: str | int = "daily"):
        super().__init__(rebalance=rebalance)
        self._symbol = symbol

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None
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
        rebalance: str | int = "daily",
    ):
        super().__init__(rebalance=rebalance)
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
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None

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


class StaticUniverseStrategy(Strategy):
    """
    Allocates equal signals relative to all symbols in the provided list.
    Sizer will normalize weights (e.g. EqualWeightSizer will make them 1/N).
    """

    def __init__(self, *, symbols: List[str], rebalance: str | int = "daily"):
        super().__init__(rebalance=rebalance)
        self._symbols = symbols

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None
        return StrategyDecision(scores={s: 1.0 for s in self._symbols})
