"""Backtesting engine (Phase 1: core simulation + artifacts, no API)."""

from backtest.config import BacktestConfig
from backtest.runner import BacktestRunResult, run_backtest

__all__ = [
    "BacktestConfig",
    "BacktestRunResult",
    "run_backtest",
]

