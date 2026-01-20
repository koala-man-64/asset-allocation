from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from asset_allocation.backtest.config import BacktestConfig, validate_config_dict_strict
from asset_allocation.backtest.runner import run_backtest


def _prices_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 3)],
            "symbol": ["AAA", "BBB"] * 3,
            "open": [100.0, 200.0, 110.0, 210.0, 120.0, 220.0],
            "close": [101.0, 201.0, 111.0, 211.0, 121.0, 221.0],
        }
    )


def _base_config(tmp_path: Path) -> dict[str, object]:
    return {
        "run_name": "composite_smoke",
        "start_date": "2020-01-01",
        "end_date": "2020-01-03",
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
        "sizing": {"class": "EqualWeightSizer", "parameters": {"max_positions": 10}},
    }


def test_composite_one_leg_matches_single_strategy(tmp_path: Path) -> None:
    base = _base_config(tmp_path)

    cfg_single = dict(base)
    cfg_single["strategy"] = {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"], "rebalance": "daily"}}
    validate_config_dict_strict(cfg_single)
    single = run_backtest(
        BacktestConfig.from_dict(cfg_single),
        prices=_prices_frame(),
        run_id="RUNTEST-SINGLE",
        output_base_dir=tmp_path,
    )

    cfg_comp = dict(base)
    cfg_comp["strategy"] = {
        "type": "composite",
        "blend": {"method": "weighted_sum", "normalize_final": "none", "allow_overlap": True},
        "legs": [
            {
                "name": "A",
                "weight": 1.0,
                "strategy": {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"], "rebalance": "daily"}},
            }
        ],
    }
    validate_config_dict_strict(cfg_comp)
    comp = run_backtest(
        BacktestConfig.from_dict(cfg_comp),
        prices=_prices_frame(),
        run_id="RUNTEST-COMP-1",
        output_base_dir=tmp_path,
    )

    trades_single = pd.read_csv(single.output_dir / "trades.csv")
    trades_comp = pd.read_csv(comp.output_dir / "trades.csv")
    pd.testing.assert_frame_equal(trades_single, trades_comp, check_like=True)

    assert (comp.output_dir / "blend" / "blended_pre_constraints.csv").exists()
    assert (comp.output_dir / "blend" / "blended_post_constraints.csv").exists()
    assert (comp.output_dir / "legs" / "A" / "weights.csv").exists()


def test_composite_two_identical_legs_matches_single_strategy(tmp_path: Path) -> None:
    base = _base_config(tmp_path)

    cfg_single = dict(base)
    cfg_single["strategy"] = {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"], "rebalance": "daily"}}
    single = run_backtest(
        BacktestConfig.from_dict(cfg_single),
        prices=_prices_frame(),
        run_id="RUNTEST-SINGLE-2",
        output_base_dir=tmp_path,
    )

    cfg_comp = dict(base)
    cfg_comp["strategy"] = {
        "type": "composite",
        "blend": {"method": "weighted_sum", "normalize_final": "none", "allow_overlap": True},
        "legs": [
            {
                "name": "A",
                "weight": 0.5,
                "strategy": {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"], "rebalance": "daily"}},
            },
            {
                "name": "B",
                "weight": 0.5,
                "strategy": {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"], "rebalance": "daily"}},
            },
        ],
    }
    comp = run_backtest(
        BacktestConfig.from_dict(cfg_comp),
        prices=_prices_frame(),
        run_id="RUNTEST-COMP-2",
        output_base_dir=tmp_path,
    )

    trades_single = pd.read_csv(single.output_dir / "trades.csv")
    trades_comp = pd.read_csv(comp.output_dir / "trades.csv")
    pd.testing.assert_frame_equal(trades_single, trades_comp, check_like=True)

