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

