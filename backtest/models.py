from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional


@dataclass(frozen=True)
class PortfolioSnapshot:
    as_of: date
    cash: float
    positions: Dict[str, float]  # symbol -> shares
    equity: float
    bar_index: Optional[int] = None
    position_states: Dict[str, "PositionState"] = field(default_factory=dict)


@dataclass(frozen=True)
class MarketBar:
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None


@dataclass(frozen=True)
class MarketSnapshot:
    as_of: date
    bar_index: int
    bars: Dict[str, MarketBar]


@dataclass(frozen=True)
class PositionState:
    symbol: str
    shares: float
    avg_entry_price: Optional[float]
    entry_date: Optional[date]
    entry_bar_index: Optional[int]
    last_fill_date: Optional[date]


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
class ExecutionReject:
    execution_date: date
    symbol: str
    reason: str
    requested_qty: float
    executed_qty: float
    requested_notional: Optional[float] = None
    executed_notional: Optional[float] = None


@dataclass(frozen=True)
class ExecutionResult:
    fills: List[TradeFill]
    costs: ExecutionCosts
    rejects: List[ExecutionReject] = field(default_factory=list)


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
