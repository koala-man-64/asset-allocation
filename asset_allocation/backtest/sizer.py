from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable

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

