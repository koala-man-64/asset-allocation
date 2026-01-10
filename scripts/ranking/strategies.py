"""
Ranking Strategies Interface and Implementations.
"""
from abc import ABC, abstractmethod
from typing import List, Optional
import pandas as pd
from datetime import date

from scripts.ranking.core import RankingResult
from scripts.common.core import write_line

class AbstractStrategy(ABC):
    """
    Abstract base class for all ranking strategies.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name of the strategy."""
        pass

    @abstractmethod
    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        """
        Computes rankings based on the provided data.
        
        Args:
            data: A DataFrame containing market/finance data for the relevant symbols.
                  Ideally pre-filtered for the specific date or window.
            ranking_date: The date for which the ranking is generated.
            
        Returns:
            A list of RankingResult objects.
        """
        pass

class MomentumStrategy(AbstractStrategy):
    """
    Example Strategy: Ranks symbols by 60-day return (Momentum).
    """
    @property
    def name(self) -> str:
        return "Momentum_60D"

    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        write_line(f"Executing {self.name} strategy...")
        
        # Expecting 'return_60d' and 'symbol' in data
        required_cols = ['symbol', 'return_60d']
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            write_line(f"Warning: Missing columns {missing} for {self.name}. Skipping.")
            return []
            
        # Filter out NaNs
        valid_data = data.dropna(subset=['return_60d']).copy()
        
        if valid_data.empty:
            return []
            
        # Sort descending by return
        valid_data.sort_values('return_60d', ascending=False, inplace=True)
        
        results = []
        rank_counter = 1
        for _, row in valid_data.iterrows():
            results.append(RankingResult(
                date=ranking_date,
                strategy=self.name,
                symbol=row['symbol'],
                rank=rank_counter,
                score=row['return_60d'],
                meta=None
            ))
            rank_counter += 1
            
        return results

class ValueStrategy(AbstractStrategy):
    """
    Example Strategy: Ranks symbols by PE Ratio (Value).
    Lower PE is better (simplified).
    """
    @property
    def name(self) -> str:
        return "Value_PE"

    def rank(self, data: pd.DataFrame, ranking_date: date) -> List[RankingResult]:
        write_line(f"Executing {self.name} strategy...")
        
        required_cols = ['symbol', 'pe_ratio']
        # Note: pe_ratio might come from finance data joined with market data
        missing = [c for c in required_cols if c not in data.columns]
        if missing:
            write_line(f"Warning: Missing columns {missing} for {self.name}. Skipping.")
            return []

        # Filter positive PE only for this simple strategy
        valid_data = data[(data['pe_ratio'] > 0)].dropna(subset=['pe_ratio']).copy()
        
        if valid_data.empty:
            return []
            
        # Sort ascending (lower is better)
        valid_data.sort_values('pe_ratio', ascending=True, inplace=True)
        
        results = []
        rank_counter = 1
        for _, row in valid_data.iterrows():
            results.append(RankingResult(
                date=ranking_date,
                strategy=self.name,
                symbol=row['symbol'],
                rank=rank_counter,
                score=row['pe_ratio'],
                meta=None
            ))
            rank_counter += 1
            
        return results
