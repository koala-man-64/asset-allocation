from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from asset_allocation.backtest.config import BacktestConfig, generate_run_id
from asset_allocation.backtest.constraints import Constraints
from asset_allocation.backtest.engine import BacktestEngine
from asset_allocation.backtest.reporter import Reporter
from asset_allocation.backtest.sizer import EqualWeightSizer, KellySizer, LongShortScoreSizer, Sizer
from asset_allocation.backtest.strategy import (
    BreakoutStrategy,
    BuyAndHoldStrategy,
    EpisodicPivotStrategy,
    StaticUniverseStrategy,
    Strategy,
    TopNSignalStrategy,
)


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
    if name == "TopNSignalStrategy":
        return TopNSignalStrategy(
            signal_column=str(params.get("signal_column") or "composite_percentile"),
            top_n=int(params.get("top_n", 10)),
            min_signal=float(params["min_signal"]) if "min_signal" in params and params["min_signal"] is not None else None,
            higher_is_better=bool(params.get("higher_is_better", True)),
        )
    if name == "BreakoutStrategy":
        return BreakoutStrategy(
            breakout_score_column=str(params.get("breakout_score_column") or "breakout_score"),
            breakdown_score_column=str(params.get("breakdown_score_column") or "breakdown_score")
            if params.get("breakdown_score_column", "breakdown_score") is not None
            else None,
            enable_shorts=bool(params.get("enable_shorts", True)),
            short_from_breakout=bool(params.get("short_from_breakout", False)),
            min_abs_score=float(params.get("min_abs_score", 0.0)),
            trailing_ma_days=int(params.get("trailing_ma_days", 10)),
            stop_loss_pct=float(params["stop_loss_pct"]) if "stop_loss_pct" in params and params["stop_loss_pct"] is not None else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            partial_exit_days=int(params["partial_exit_days"])
            if "partial_exit_days" in params and params["partial_exit_days"] is not None
            else None,
            partial_exit_fraction=float(params.get("partial_exit_fraction", 0.5)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name == "EpisodicPivotStrategy":
        return EpisodicPivotStrategy(
            ep_score_column=str(params.get("ep_score_column") or "ep_score"),
            min_ep_score=float(params.get("min_ep_score", 0.0)),
            enable_shorts=bool(params.get("enable_shorts", False)),
            trailing_ma_days=int(params.get("trailing_ma_days", 20)),
            stop_loss_pct=float(params["stop_loss_pct"]) if "stop_loss_pct" in params and params["stop_loss_pct"] is not None else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            allow_raw_fields=bool(params.get("allow_raw_fields", True)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name == "StaticUniverseStrategy":
        symbols = params.get("symbols")
        if not symbols or not isinstance(symbols, list):
            # Fallback to config universe if not provided?
            # Or strict. Let's be strict or use universe.
            symbols = config.universe.symbols
        return StaticUniverseStrategy(
            symbols=[str(s) for s in symbols],
            rebalance=params.get("rebalance", "daily"),
        )
    raise ValueError(f"Unknown strategy.class '{name}' (registry is strict).")


def _build_sizer(config: BacktestConfig) -> Sizer:
    name = config.sizing.class_name
    params = config.sizing.parameters or {}
    if name == "EqualWeightSizer":
        return EqualWeightSizer(max_positions=int(params.get("max_positions", 10)))
    if name == "LongShortScoreSizer":
        return LongShortScoreSizer(
            max_longs=int(params.get("max_longs", 10)),
            max_shorts=int(params.get("max_shorts", 10)),
            gross_target=float(params.get("gross_target", 1.0)),
            net_target=float(params.get("net_target", 0.0)),
            weight_mode=str(params.get("weight_mode", "equal")),
            sticky_holdings=bool(params.get("sticky_holdings", True)),
            score_power=float(params.get("score_power", 1.0)),
            min_abs_score=float(params.get("min_abs_score", 0.0)),
        )
    if name == "KellySizer":
        if "mu_scale" not in params:
            raise ValueError("KellySizer requires sizing.parameters.mu_scale (expected daily return per score unit).")
        return KellySizer(
            kelly_fraction=float(params.get("kelly_fraction", 0.5)),
            lookback_days=int(params.get("lookback_days", 20)),
            mu_scale=float(params["mu_scale"]),
        )
    raise ValueError(f"Unknown sizing.class '{name}' (registry is strict).")


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
