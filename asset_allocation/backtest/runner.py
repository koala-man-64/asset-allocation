from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from asset_allocation.backtest.config import BacktestConfig, generate_run_id
from asset_allocation.backtest.constraints import Constraints
from asset_allocation.backtest.engine import BacktestEngine
from asset_allocation.backtest.reporter import Reporter
from asset_allocation.backtest.sizer import EqualWeightSizer, Sizer
from asset_allocation.backtest.strategy import BuyAndHoldStrategy, Strategy


@dataclass(frozen=True)
class BacktestRunResult:
    run_id: str
    output_dir: Path


def _build_strategy(config: BacktestConfig) -> Strategy:
    name = config.strategy.class_name
    params = config.strategy.parameters or {}
    if name == "BuyAndHoldStrategy":
        symbol = str(params.get("symbol") or (config.universe.symbols[0] if config.universe.symbols else ""))
        if not symbol:
            raise ValueError("BuyAndHoldStrategy requires a 'symbol' parameter or a non-empty universe.")
        return BuyAndHoldStrategy(symbol=symbol)
    raise ValueError(f"Unknown strategy.class '{name}' (Phase 1 registry is strict).")


def _build_sizer(config: BacktestConfig) -> Sizer:
    name = config.sizing.class_name
    params = config.sizing.parameters or {}
    if name == "EqualWeightSizer":
        return EqualWeightSizer(max_positions=int(params.get("max_positions", 10)))
    raise ValueError(f"Unknown sizing.class '{name}' (Phase 1 registry is strict).")


def run_backtest(
    config: BacktestConfig,
    *,
    prices: pd.DataFrame,
    signals: Optional[pd.DataFrame] = None,
    run_id: Optional[str] = None,
    output_base_dir: Optional[Path] = None,
) -> BacktestRunResult:
    resolved_run_id = run_id or generate_run_id()
    reporter = Reporter.create(config, run_id=resolved_run_id, output_dir=output_base_dir)

    strategy = _build_strategy(config)
    sizer = _build_sizer(config)
    constraints = Constraints(config=config.constraints)

    engine = BacktestEngine(
        config=config,
        prices=prices,
        signals=signals,
        strategy=strategy,
        sizer=sizer,
        constraints=constraints,
        reporter=reporter,
    )
    engine.run(run_id=resolved_run_id)
    reporter.write_artifacts()

    return BacktestRunResult(run_id=resolved_run_id, output_dir=reporter.output_dir)

