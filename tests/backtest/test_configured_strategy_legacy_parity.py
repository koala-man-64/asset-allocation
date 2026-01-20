from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.configured_strategy import ConfiguredStrategy
from asset_allocation.backtest.configured_strategy.legacy_migrations import (
    configured_config_for_long_short_topn_strategy,
    configured_config_for_topn_signal_strategy,
)
from asset_allocation.backtest.models import PortfolioSnapshot, PositionState
from asset_allocation.backtest.runner import run_backtest
from asset_allocation.backtest.strategy import LongShortTopNStrategy


def _dates(n: int = 5) -> list[date]:
    start = date(2020, 1, 1)
    return [start + timedelta(days=i) for i in range(n)]


def _base_config(tmp_path: Path, *, symbols: list[str], start: date, end: date) -> dict[str, object]:
    return {
        "run_name": "configured_strategy_legacy_parity",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "initial_cash": 1000.0,
        "universe": {"symbols": symbols},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
    }


def _trades_frame(output_dir: Path) -> pd.DataFrame:
    df = pd.read_csv(output_dir / "trades.csv")
    out = df[["execution_date", "symbol", "quantity"]].copy()
    out["quantity"] = pd.to_numeric(out["quantity"], errors="coerce").round(8)
    return out


def test_migrate_topn_signal_strategy_matches_legacy_first_pick(tmp_path: Path) -> None:
    dates = _dates(3)
    prices = pd.DataFrame(
        [
            {"date": d, "symbol": "AAA", "open": 100.0, "close": 100.0}
            for d in dates
        ]
        + [
            {"date": d, "symbol": "BBB", "open": 100.0, "close": 100.0}
            for d in dates
        ]
    )
    signals = pd.DataFrame(
        [
            {"date": d, "symbol": "AAA", "composite_percentile": 2.0}
            for d in dates
        ]
        + [
            {"date": d, "symbol": "BBB", "composite_percentile": 1.0}
            for d in dates
        ]
    )

    base = _base_config(tmp_path, symbols=["AAA", "BBB"], start=dates[0], end=dates[-1])
    base["constraints"] = {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False}
    base["sizing"] = {"class": "EqualWeightSizer", "parameters": {"max_positions": 1}}

    legacy_cfg = dict(base)
    legacy_cfg["strategy"] = {
        "class": "TopNSignalStrategy",
        "parameters": {"signal_column": "composite_percentile", "top_n": 1, "rebalance": 10},
    }

    configured_cfg = dict(base)
    configured_cfg["strategy"] = {
        "type": "configured",
        **configured_config_for_topn_signal_strategy(
            signal_column="composite_percentile",
            top_n=1,
            rebalance=10,
        ),
    }

    legacy_result = run_backtest(
        BacktestConfig.from_dict(legacy_cfg),
        prices=prices,
        signals=signals,
        run_id="RUNTEST-LEGACY-TOPN",
        output_base_dir=tmp_path,
    )
    configured_result = run_backtest(
        BacktestConfig.from_dict(configured_cfg),
        prices=prices,
        signals=signals,
        run_id="RUNTEST-CONFIGURED-TOPN",
        output_base_dir=tmp_path,
    )

    legacy_trades = _trades_frame(legacy_result.output_dir)
    configured_trades = _trades_frame(configured_result.output_dir)

    assert len(legacy_trades) == 1
    assert len(configured_trades) == 1
    assert legacy_trades.iloc[0]["symbol"] == "AAA"
    assert configured_trades.iloc[0]["symbol"] == "AAA"


def test_migrate_long_short_topn_stop_loss_exits_on_non_rebalance_day(tmp_path: Path) -> None:
    dates = _dates(5)
    rows: list[dict[str, object]] = []
    for d in dates:
        rows.append({"date": d, "symbol": "AAA", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0})
    # Trigger stop loss on day 3 close (non-rebalance) via intraday low.
    rows[2]["low"] = 89.0
    rows[2]["close"] = 92.0
    prices = pd.DataFrame(rows)
    signals = pd.DataFrame([{"date": d, "symbol": "AAA", "score": 1.0} for d in dates])

    base = _base_config(tmp_path, symbols=["AAA"], start=dates[0], end=dates[-1])
    base["constraints"] = {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False}
    base["sizing"] = {
        "class": "LongShortScoreSizer",
        "parameters": {"max_longs": 1, "max_shorts": 0, "gross_target": 1.0, "net_target": 1.0, "weight_mode": "equal"},
    }

    legacy_cfg = dict(base)
    legacy_cfg["strategy"] = {
        "class": "LongShortTopNStrategy",
        "parameters": {"signal_column": "score", "k_long": 1, "rebalance": 10, "stop_loss_pct": 0.10, "use_low_for_stop": True},
    }

    configured_cfg = dict(base)
    configured_cfg["strategy"] = {
        "type": "configured",
        **configured_config_for_long_short_topn_strategy(
            signal_column="score",
            k_long=1,
            k_short=0,
            rebalance=10,
            stop_loss_pct=0.10,
            use_low_for_stop=True,
        ),
    }

    legacy_result = run_backtest(
        BacktestConfig.from_dict(legacy_cfg),
        prices=prices,
        signals=signals,
        run_id="RUNTEST-LEGACY-LS-SL",
        output_base_dir=tmp_path,
    )
    configured_result = run_backtest(
        BacktestConfig.from_dict(configured_cfg),
        prices=prices,
        signals=signals,
        run_id="RUNTEST-CONFIGURED-LS-SL",
        output_base_dir=tmp_path,
    )

    legacy_trades = _trades_frame(legacy_result.output_dir)
    configured_trades = _trades_frame(configured_result.output_dir)

    pd.testing.assert_frame_equal(legacy_trades, configured_trades)
    assert len(legacy_trades) == 2
    assert legacy_trades.iloc[0]["quantity"] > 0
    assert legacy_trades.iloc[1]["quantity"] < 0


def test_migrate_long_short_topn_partial_exit_scales_on_non_rebalance_day(tmp_path: Path) -> None:
    dates = _dates(5)
    prices = pd.DataFrame(
        [{"date": d, "symbol": "AAA", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0} for d in dates]
    )
    signals = pd.DataFrame([{"date": d, "symbol": "AAA", "score": 1.0} for d in dates])

    legacy = LongShortTopNStrategy(
        signal_column="score",
        k_long=1,
        k_short=0,
        rebalance=10,
        partial_exit_days=2,
        partial_exit_fraction=0.5,
    )
    configured = ConfiguredStrategy(
        config=configured_config_for_long_short_topn_strategy(
            signal_column="score",
            k_long=1,
            k_short=0,
            rebalance=10,
            partial_exit_days=2,
            partial_exit_fraction=0.5,
        )
    )

    def _slice_prices(as_of: date) -> pd.DataFrame:
        return prices[prices["date"] <= as_of].copy()

    def _slice_signals(as_of: date) -> pd.DataFrame:
        return signals[signals["date"] == as_of].copy()

    # Day 1 close: strategy selects AAA (rebalance).
    empty = PortfolioSnapshot(as_of=dates[0], cash=1000.0, positions={}, equity=1000.0, bar_index=0, position_states={})
    d0_legacy = legacy.on_bar(dates[0], prices=_slice_prices(dates[0]), signals=_slice_signals(dates[0]), portfolio=empty)
    d0_configured = configured.on_bar(dates[0], prices=_slice_prices(dates[0]), signals=_slice_signals(dates[0]), portfolio=empty)
    assert d0_legacy is not None
    assert d0_configured is not None

    # Day 2 close: position is held; no rebalance and no partial exit yet => no decision emitted.
    state = PositionState(
        symbol="AAA",
        shares=10.0,
        avg_entry_price=100.0,
        entry_date=dates[1],
        entry_bar_index=1,
        last_fill_date=dates[1],
    )
    held_d1 = PortfolioSnapshot(
        as_of=dates[1],
        cash=0.0,
        positions={"AAA": 10.0},
        equity=1000.0,
        bar_index=1,
        position_states={"AAA": state},
    )
    assert legacy.on_bar(dates[1], prices=_slice_prices(dates[1]), signals=_slice_signals(dates[1]), portfolio=held_d1) is None
    assert configured.on_bar(dates[1], prices=_slice_prices(dates[1]), signals=_slice_signals(dates[1]), portfolio=held_d1) is None

    # Day 3 close: partial exit triggers on a non-rebalance day via StrategyDecision.scales.
    held_d2 = PortfolioSnapshot(
        as_of=dates[2],
        cash=0.0,
        positions={"AAA": 10.0},
        equity=1000.0,
        bar_index=2,
        position_states={"AAA": state},
    )
    d2_legacy = legacy.on_bar(dates[2], prices=_slice_prices(dates[2]), signals=_slice_signals(dates[2]), portfolio=held_d2)
    d2_configured = configured.on_bar(dates[2], prices=_slice_prices(dates[2]), signals=_slice_signals(dates[2]), portfolio=held_d2)

    assert d2_legacy is not None
    assert d2_configured is not None
    assert d2_legacy.scales.get("AAA") == pytest.approx(0.5)
    assert d2_configured.scales.get("AAA") == pytest.approx(0.5)
