"""Backtesting engine (Phase 1: core simulation + artifacts, no API)."""

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.runner import BacktestRunResult, run_backtest

__all__ = [
    "BacktestConfig",
    "BacktestRunResult",
    "run_backtest",
]

