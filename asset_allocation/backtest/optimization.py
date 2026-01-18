from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

class Optimizer(ABC):
    """
    Abstract base class for Portfolio Optimizers.
    """
    
    @abstractmethod
    def optimize(
        self,
        universe: List[str],
        expected_returns: pd.Series,
        covariance_matrix: pd.DataFrame,
        current_weights: Optional[Dict[str, float]] = None,
        constraints: Optional[Dict] = None,
    ) -> Dict[str, float]:
        """
        Calculate target weights for the portfolio.

        Args:
            universe: List of symbols to optimize over.
            expected_returns: Series of expected returns for each symbol.
            covariance_matrix: DataFrame of covariance between symbols.
            current_weights: Optional current portfolio weights (for turnover constraints).
            constraints: Optional dictionary of constraint parameters.

        Returns:
            Dictionary mapping symbol -> target weight.
        """
        pass


class MeanVarianceOptimizer(Optimizer):
    """
    Mean-Variance Optimizer using cvxpy.
    """
    
    def __init__(self, risk_aversion: float = 1.0):
        self.risk_aversion = risk_aversion
        try:
            import cvxpy as cp
            self.cp = cp
        except ImportError:
            raise ImportError(
                "cvxpy is required for MeanVarianceOptimizer. "
                "Please install it with `pip install cvxpy`."
            )

    def optimize(
        self,
        universe: List[str],
        expected_returns: pd.Series,
        covariance_matrix: pd.DataFrame,
        current_weights: Optional[Dict[str, float]] = None,
        constraints: Optional[Dict] = None,
    ) -> Dict[str, float]:
        
        n_assets = len(universe)
        if n_assets == 0:
            return {}
            
        # Align inputs
        mu = expected_returns.reindex(universe).fillna(0).values
        Sigma = covariance_matrix.reindex(index=universe, columns=universe).fillna(0).values
        
        # Define Variables
        w = self.cp.Variable(n_assets)
        gamma = self.cp.Parameter(nonneg=True, value=self.risk_aversion)
        ret = mu.T @ w
        risk = self.cp.quad_form(w, Sigma)
        
        # Objective: Maximize Utility (Returns - Risk)
        # Note: We can add transaction costs here later
        objective = self.cp.Maximize(ret - gamma * risk)
        
        # Constraints
        # Fully invested (sum(w) == 1) and Long Only (w >= 0)
        cons = [self.cp.sum(w) == 1, w >= 0]
        
        prob = self.cp.Problem(objective, cons)
        
        try:
            prob.solve()
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            return {}

        if w.value is None:
            logger.warning("Optimization returned None (infeasible?)")
            return {}

        # Round small values to zero
        weights = w.value
        weights[weights < 1e-5] = 0.0
        
        # Normalize just in case
        total_weight = np.sum(weights)
        if total_weight > 0:
            weights = weights / total_weight
            
        return {symbol: float(weight) for symbol, weight in zip(universe, weights) if weight > 0}
