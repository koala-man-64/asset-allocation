from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from backtest.config import BacktestConfig, generate_run_id
from backtest.constraints import Constraints
from backtest.engine import BacktestEngine
from backtest.reporter import Reporter
from backtest.sizer import EqualWeightSizer, KellySizer, LongShortScoreSizer, Sizer
from backtest.configured_strategy import ConfiguredStrategy
from backtest.blend import BlendConfig
from backtest.composite_strategy import CompositeStrategy, StrategyLeg
from backtest.strategy import (
    BreakoutStrategy,
    BuyAndHoldStrategy,
    EpisodicPivotStrategy,
    LongShortTopNStrategy,
    StaticUniverseStrategy,
    Strategy,
    TopNSignalStrategy,
)


@dataclass(frozen=True)
class BacktestRunResult:
    run_id: str
    output_dir: Path


def _normalize_configured_params(raw: dict) -> dict:
    params = raw.get("parameters") or {}
    if params and not isinstance(params, dict):
        raise ValueError("strategy.parameters must be an object.")
    merged = dict(params) if isinstance(params, dict) else {}
    for key, value in (raw or {}).items():
        if key in {"type", "class", "class_name", "module", "parameters"}:
            continue
        merged[str(key)] = value
    return merged


def _build_strategy_from_spec(config: BacktestConfig, *, spec: dict, output_dir: Path) -> Strategy:
    if not isinstance(spec, dict):
        raise ValueError("leg.strategy must be an object.")

    stype = spec.get("type")
    if stype is not None and str(stype).strip() == "configured":
        params = _normalize_configured_params(spec)
        return ConfiguredStrategy(config=params, debug_output_dir=output_dir)

    if stype is not None and str(stype).strip() == "composite":
        raise ValueError("Nested composite strategies are not supported.")

    name = spec.get("class") or spec.get("class_name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("leg.strategy.class is required for non-configured strategies.")
    params = spec.get("parameters") or {}
    if params and not isinstance(params, dict):
        raise ValueError("leg.strategy.parameters must be an object.")

    # Reuse the strict registry for legacy strategies.
    if name.strip() == "BuyAndHoldStrategy":
        symbol = str(params.get("symbol") or (config.universe.symbols[0] if config.universe.symbols else ""))
        if not symbol:
            raise ValueError("BuyAndHoldStrategy requires a 'symbol' parameter or a non-empty universe.")
        return BuyAndHoldStrategy(symbol=symbol)
    if name.strip() == "TopNSignalStrategy":
        return TopNSignalStrategy(
            signal_column=str(params.get("signal_column") or "composite_percentile"),
            top_n=int(params.get("top_n", 10)),
            min_signal=float(params["min_signal"]) if "min_signal" in params and params["min_signal"] is not None else None,
            higher_is_better=bool(params.get("higher_is_better", True)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name.strip() == "BreakoutStrategy":
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
            take_profit_pct=float(params["take_profit_pct"]) if "take_profit_pct" in params and params["take_profit_pct"] is not None else None,
            trailing_stop_pct=float(params["trailing_stop_pct"]) if "trailing_stop_pct" in params and params["trailing_stop_pct"] is not None else None,
            time_stop_days=int(params["time_stop_days"]) if "time_stop_days" in params and params["time_stop_days"] is not None else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            partial_exit_days=int(params["partial_exit_days"])
            if "partial_exit_days" in params and params["partial_exit_days"] is not None
            else None,
            partial_exit_fraction=float(params.get("partial_exit_fraction", 0.5)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name.strip() == "LongShortTopNStrategy":
        return LongShortTopNStrategy(
            signal_column=str(params.get("signal_column") or "composite_percentile"),
            k_long=int(params.get("k_long", 0)),
            k_short=int(params.get("k_short", 0)),
            long_if_high=bool(params.get("long_if_high", True)),
            min_abs_score=float(params.get("min_abs_score", 0.0)),
            trailing_ma_days=int(params["trailing_ma_days"])
            if "trailing_ma_days" in params and params["trailing_ma_days"] is not None
            else None,
            stop_loss_pct=float(params["stop_loss_pct"])
            if "stop_loss_pct" in params and params["stop_loss_pct"] is not None
            else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            partial_exit_days=int(params["partial_exit_days"])
            if "partial_exit_days" in params and params["partial_exit_days"] is not None
            else None,
            partial_exit_fraction=float(params.get("partial_exit_fraction", 0.5)),
            max_hold_days=int(params["max_hold_days"]) if "max_hold_days" in params and params["max_hold_days"] is not None else None,
            rebalance=params.get("rebalance", "daily"),
        )
    if name.strip() == "EpisodicPivotStrategy":
        return EpisodicPivotStrategy(
            ep_score_column=str(params.get("ep_score_column") or "ep_score"),
            min_ep_score=float(params.get("min_ep_score", 0.0)),
            enable_shorts=bool(params.get("enable_shorts", False)),
            trailing_ma_days=int(params.get("trailing_ma_days", 20)),
            stop_loss_pct=float(params["stop_loss_pct"]) if "stop_loss_pct" in params and params["stop_loss_pct"] is not None else None,
            take_profit_pct=float(params["take_profit_pct"]) if "take_profit_pct" in params and params["take_profit_pct"] is not None else None,
            trailing_stop_pct=float(params["trailing_stop_pct"]) if "trailing_stop_pct" in params and params["trailing_stop_pct"] is not None else None,
            time_stop_days=int(params["time_stop_days"]) if "time_stop_days" in params and params["time_stop_days"] is not None else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            allow_raw_fields=bool(params.get("allow_raw_fields", True)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name.strip() == "StaticUniverseStrategy":
        symbols = params.get("symbols")
        if not symbols or not isinstance(symbols, list):
            symbols = config.universe.symbols
        return StaticUniverseStrategy(
            symbols=[str(s) for s in symbols],
            rebalance=params.get("rebalance", "daily"),
        )
    raise ValueError(f"Unknown leg strategy.class '{name}' (registry is strict).")


def _build_sizer_from_spec(spec: dict) -> Sizer:
    if not isinstance(spec, dict):
        raise ValueError("leg.sizing must be an object.")
    name = spec.get("class") or spec.get("class_name")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("leg.sizing.class is required.")
    params = spec.get("parameters") or {}
    if params and not isinstance(params, dict):
        raise ValueError("leg.sizing.parameters must be an object.")
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
    if name == "OptimizationSizer":
        from backtest.optimization import MeanVarianceOptimizer
        from backtest.sizer import OptimizationSizer

        optimizer = MeanVarianceOptimizer(risk_aversion=float(params.get("risk_aversion", 1.0)))
        return OptimizationSizer(optimizer=optimizer, lookback_days=int(params.get("lookback_days", 252)))

    raise ValueError(f"Unknown leg sizing.class '{name}' (registry is strict).")


def _build_strategy(config: BacktestConfig, *, output_dir: Path) -> Strategy:
    name = config.strategy.class_name
    params = config.strategy.parameters or {}
    if name == "ConfiguredStrategy":
        return ConfiguredStrategy(config=params, debug_output_dir=output_dir)
    if name == "CompositeStrategy":
        legs_raw = params.get("legs") or []
        if not isinstance(legs_raw, list) or not legs_raw:
            raise ValueError("CompositeStrategy requires strategy.legs to be a non-empty list.")

        blend_raw = params.get("blend") or {}
        if blend_raw and not isinstance(blend_raw, dict):
            raise ValueError("strategy.blend must be an object.")
        blend = BlendConfig(
            method=str(blend_raw.get("method") or "weighted_sum"),  # type: ignore[arg-type]
            normalize_final=str(blend_raw.get("normalize_final") or "none"),  # type: ignore[arg-type]
            target_gross=float(blend_raw["target_gross"]) if blend_raw.get("target_gross") is not None else None,
            target_net=float(blend_raw["target_net"]) if blend_raw.get("target_net") is not None else None,
            allow_overlap=bool(blend_raw.get("allow_overlap", True)),
        )

        default_sizer = _build_sizer(config)

        def _safe_fragment(value: str) -> str:
            cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value).strip())
            return cleaned or "leg"

        legs: list[StrategyLeg] = []
        for idx, leg_raw in enumerate(legs_raw):
            if not isinstance(leg_raw, dict):
                raise ValueError("strategy.legs items must be objects.")
            leg_name = str(leg_raw.get("name") or f"leg{idx+1}").strip()
            alpha = leg_raw.get("weight", leg_raw.get("alpha", None))
            if alpha is None:
                raise ValueError(f"CompositeStrategy leg {leg_name!r} requires a 'weight'.")
            strategy_spec = leg_raw.get("strategy") or {}
            leg_output_dir = output_dir / "legs" / _safe_fragment(leg_name)
            strategy = _build_strategy_from_spec(config, spec=strategy_spec, output_dir=leg_output_dir)

            sizing_spec = leg_raw.get("sizing") or leg_raw.get("sizer")
            sizer = _build_sizer_from_spec(sizing_spec) if sizing_spec is not None else default_sizer

            legs.append(
                StrategyLeg(
                    name=leg_name,
                    alpha=float(alpha),
                    strategy=strategy,
                    sizer=sizer,
                    normalize_leg=str(leg_raw.get("normalize_leg") or "none"),
                    target_gross=float(leg_raw["target_gross"]) if leg_raw.get("target_gross") is not None else None,
                    target_net=float(leg_raw["target_net"]) if leg_raw.get("target_net") is not None else None,
                    enabled=bool(leg_raw.get("enabled", True)),
                )
            )

        return CompositeStrategy(
            legs=legs,
            blend=blend,
            broker_config=config.broker,
            initial_cash=float(config.initial_cash),
        )
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
            take_profit_pct=float(params["take_profit_pct"]) if "take_profit_pct" in params and params["take_profit_pct"] is not None else None,
            trailing_stop_pct=float(params["trailing_stop_pct"]) if "trailing_stop_pct" in params and params["trailing_stop_pct"] is not None else None,
            time_stop_days=int(params["time_stop_days"]) if "time_stop_days" in params and params["time_stop_days"] is not None else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            partial_exit_days=int(params["partial_exit_days"])
            if "partial_exit_days" in params and params["partial_exit_days"] is not None
            else None,
            partial_exit_fraction=float(params.get("partial_exit_fraction", 0.5)),
            rebalance=params.get("rebalance", "daily"),
        )
    if name == "LongShortTopNStrategy":
        return LongShortTopNStrategy(
            signal_column=str(params.get("signal_column") or "composite_percentile"),
            k_long=int(params.get("k_long", 0)),
            k_short=int(params.get("k_short", 0)),
            long_if_high=bool(params.get("long_if_high", True)),
            min_abs_score=float(params.get("min_abs_score", 0.0)),
            trailing_ma_days=int(params["trailing_ma_days"])
            if "trailing_ma_days" in params and params["trailing_ma_days"] is not None
            else None,
            stop_loss_pct=float(params["stop_loss_pct"])
            if "stop_loss_pct" in params and params["stop_loss_pct"] is not None
            else None,
            use_low_for_stop=bool(params.get("use_low_for_stop", True)),
            partial_exit_days=int(params["partial_exit_days"])
            if "partial_exit_days" in params and params["partial_exit_days"] is not None
            else None,
            partial_exit_fraction=float(params.get("partial_exit_fraction", 0.5)),
            max_hold_days=int(params["max_hold_days"]) if "max_hold_days" in params and params["max_hold_days"] is not None else None,
            rebalance=params.get("rebalance", "daily"),
        )
    if name == "EpisodicPivotStrategy":
        return EpisodicPivotStrategy(
            ep_score_column=str(params.get("ep_score_column") or "ep_score"),
            min_ep_score=float(params.get("min_ep_score", 0.0)),
            enable_shorts=bool(params.get("enable_shorts", False)),
            trailing_ma_days=int(params.get("trailing_ma_days", 20)),
            stop_loss_pct=float(params["stop_loss_pct"]) if "stop_loss_pct" in params and params["stop_loss_pct"] is not None else None,
            take_profit_pct=float(params["take_profit_pct"]) if "take_profit_pct" in params and params["take_profit_pct"] is not None else None,
            trailing_stop_pct=float(params["trailing_stop_pct"]) if "trailing_stop_pct" in params and params["trailing_stop_pct"] is not None else None,
            time_stop_days=int(params["time_stop_days"]) if "time_stop_days" in params and params["time_stop_days"] is not None else None,
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
    if name == "OptimizationSizer":
        # Lazy import to avoid hard dependency on optional modules
        from backtest.optimization import MeanVarianceOptimizer
        from backtest.sizer import OptimizationSizer
        
        # Instantiate Optional Optimizer
        # For now, we assume MeanVarianceOptimizer is the default if this sizer is chosen.
        # Params for optimizer could be extracted from sizing.parameters if needed.
        optimizer = MeanVarianceOptimizer(
            risk_aversion=float(params.get("risk_aversion", 1.0))
        )
        return OptimizationSizer(
            optimizer=optimizer,
            lookback_days=int(params.get("lookback_days", 252))
        )

    raise ValueError(f"Unknown sizing.class '{name}' (registry is strict).")


def run_backtest(
    config: BacktestConfig,
    *,
    prices: Optional[pd.DataFrame] = None,
    signals: Optional[pd.DataFrame] = None,
    run_id: Optional[str] = None,
    output_base_dir: Optional[Path] = None,
) -> BacktestRunResult:
    if prices is None:
        from backtest.data_access import load_backtest_inputs

        loaded_prices, loaded_signals = load_backtest_inputs(config)
        prices = loaded_prices
        if signals is None:
            signals = loaded_signals

    resolved_run_id = run_id or generate_run_id()
    reporter = Reporter.create(config, run_id=resolved_run_id, output_dir=output_base_dir)

    strategy = _build_strategy(config, output_dir=reporter.output_dir)
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
