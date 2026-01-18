from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional


@dataclass(frozen=True)
class PortfolioSnapshot:
    as_of: date
    cash: float
    positions: Dict[str, float]  # symbol -> shares
    equity: float


@dataclass(frozen=True)
class TradeFill:
    execution_date: date
    symbol: str
    quantity: float
    price: float
    notional: float
    commission: float
    slippage_cost: float
    cash_after: float


@dataclass(frozen=True)
class ExecutionCosts:
    commission: float = 0.0
    slippage_cost: float = 0.0


@dataclass(frozen=True)
class BacktestSummary:
    run_id: str
    run_name: Optional[str]
    start_date: str
    end_date: str
    initial_cash: float
    final_equity: float
    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe_ratio: Optional[float]
    max_drawdown: float
    trades: int

