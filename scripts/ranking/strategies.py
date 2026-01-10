"""
Ranking Strategies Interface and Implementations.
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from scripts.common.core import write_line
from scripts.ranking.core import RankingResult


class AbstractStrategy(ABC):
    """
    Abstract base class for all ranking strategies.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the strategy."""
        ...

    @property
    def sources_used(self) -> List[str]:
        """Delta source names required by this strategy (market data is implicit)."""
        return []

    @abstractmethod
    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        """
        Computes rankings based on the provided data.

        Args:
            data: A DataFrame containing market/finance data for the relevant symbols.
            ranking_date: The date for which the ranking is generated.

        Returns:
            A list of RankingResult objects sorted by rank.
        """
        ...

    def _ensure_columns(self, data: pd.DataFrame, required: List[str]) -> Optional[List[str]]:
        missing = [col for col in required if col not in data.columns]
        if missing:
            write_line(f"Warning: Missing columns {missing} for {self.name}. Skipping.")
            return missing
        return None


class MomentumStrategy(AbstractStrategy):
    """
    Strategy: ranks symbols by 60-day return (Momentum).
    """

    @property
    def name(self) -> str:
        return "Momentum_60D"

    @property
    def sources_used(self) -> List[str]:
        return []

    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        write_line(f"Executing {self.name} strategy...")

        required_cols = ["symbol", "return_60d"]
        if self._ensure_columns(data, required_cols) is not None:
            return []

        valid = data.dropna(subset=["return_60d"])
        if valid.empty:
            return []

        valid = valid.sort_values("return_60d", ascending=False)

        results = []
        for rank, (_, row) in enumerate(valid.iterrows(), start=1):
            results.append(
                RankingResult(
                    date=ranking_date,
                    strategy=self.name,
                    symbol=row["symbol"],
                    rank=rank,
                    score=float(row["return_60d"]),
                )
            )
        return results


class ValueStrategy(AbstractStrategy):
    """
    Strategy: ranks symbols by trailing PE ratio (lower is better).
    """

    @property
    def name(self) -> str:
        return "Value_PE"

    @property
    def sources_used(self) -> List[str]:
        return ["finance"]

    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        write_line(f"Executing {self.name} strategy...")

        required_cols = ["symbol", "pe_ratio"]
        if self._ensure_columns(data, required_cols) is not None:
            return []

        valid = data[(data["pe_ratio"] > 0)].dropna(subset=["pe_ratio"])
        if valid.empty:
            return []

        valid = valid.sort_values("pe_ratio", ascending=True)

        results = []
        for rank, (_, row) in enumerate(valid.iterrows(), start=1):
            results.append(
                RankingResult(
                    date=ranking_date,
                    strategy=self.name,
                    symbol=row["symbol"],
                    rank=rank,
                    score=float(row["pe_ratio"]),
                )
            )
        return results


class BrokenGrowthImprovingInternalsStrategy(AbstractStrategy):
    """
    Investing strategy that looks for names with broken growth but improving internal signals.
    """

    _BASE_SCORE = 4

    def __init__(
        self,
        drawdown_threshold: float = -0.3,
        margin_delta_threshold: float = 0.0,
    ):
        self.drawdown_threshold = drawdown_threshold
        self.margin_delta_threshold = margin_delta_threshold

    @property
    def name(self) -> str:
        return "BrokenGrowthWithImprovingInternals"

    @property
    def sources_used(self) -> List[str]:
        return ["finance", "price_targets"]

    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        write_line(f"Executing {self.name} strategy...")

        required_cols = [
            "symbol",
            "drawdown_1y",
            "rev_yoy",
            "rev_growth_slope_6q",
            "ebitda_margin",
            "margin_delta_qoq",
            "tp_mean_change_30d",
            "rev_net",
            "disp_norm_change_30d",
        ]
        if self._ensure_columns(data, required_cols) is not None:
            return []

        working = data.copy()
        broken = working["drawdown_1y"] <= self.drawdown_threshold
        improving = (working["rev_yoy"] > 0) & (working["rev_growth_slope_6q"] > 0)
        margin_stable = working["margin_delta_qoq"] >= self.margin_delta_threshold
        analysts_improving = (working["rev_net"] > 0) & (working["disp_norm_change_30d"] < 0)
        positive_target = working["tp_mean_change_30d"] > 0

        mask = broken & improving & margin_stable & analysts_improving
        target_rows = working.loc[mask]
        if target_rows.empty:
            return []

        results = []
        for rank, (idx, row) in enumerate(target_rows.iterrows(), start=1):
            meta: Dict[str, bool] = {
                "broken_drawdown": bool(broken.loc[idx]),
                "improving_revenue": bool(improving.loc[idx]),
                "margin_stable": bool(margin_stable.loc[idx]),
                "analysts_improving": bool(analysts_improving.loc[idx]),
                "target_trending_up": bool(positive_target.loc[idx]),
            }

            score = float(self._BASE_SCORE + int(positive_target.loc[idx]))
            results.append(
                RankingResult(
                    date=ranking_date,
                    strategy=self.name,
                    symbol=row["symbol"],
                    rank=rank,
                    score=score,
                    meta=meta,
                )
            )

        return results
