from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

class RiskModel(ABC):
    """
    Abstract base class for Risk Models.
    """

    @abstractmethod
    def calculate_risk_metrics(
        self,
        weights: Dict[str, float],
        returns: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        Calculate risk metrics (e.g., Volatility, Factor Exposures).
        """
        pass


class FactorRiskModel(RiskModel):
    """
    Factor Risk Model (e.g., PCA or Predefined Factors).
    """

    def __init__(self, factors: Optional[pd.DataFrame] = None):
        """
        Args:
            factors: DataFrame of factor returns (Date x Factor).
        """
        self.factors = factors

    def calculate_risk_metrics(
        self,
        weights: Dict[str, float],
        returns: pd.DataFrame,
    ) -> Dict[str, float]:
        """
        Calculate simple risk decomposition.
        """
        if not weights or returns.empty:
            return {}
            
        # Align weights and returns
        portfolio_series = pd.Series(weights)
        aligned_returns = returns[portfolio_series.index].fillna(0)
        
        # Portfolio Returns
        port_ret = aligned_returns.dot(portfolio_series)
        
        # Total Volatility
        volatility = port_ret.std() * (252 ** 0.5)
        
        metrics = {
            "predicted_volatility": volatility,
        }

        # If we have factors, calculate exposures
        if self.factors is not None and not self.factors.empty:
            # Join portfolio returns with factor returns
            joined = pd.concat([port_ret.rename("portfolio"), self.factors], axis=1).dropna()
            if not joined.empty:
                # Simple Regression (Beta)
                # For now, just correlation to first factor (Market) as a placeholder
                # In a real implementation, we'd do OLS here
                market_factor = joined.iloc[:, 1]
                beta = joined["portfolio"].cov(market_factor) / market_factor.var()
                metrics["beta_market"] = beta
                
        return metrics
