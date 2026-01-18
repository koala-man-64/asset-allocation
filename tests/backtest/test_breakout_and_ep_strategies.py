from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.runner import run_backtest


def _dates() -> list[date]:
    start = date(2020, 1, 1)
    return [start + timedelta(days=i) for i in range(5)]


def _prices_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i, d in enumerate(_dates()):
        rows.append({"date": d, "symbol": "AAA", "open": 100.0 + i, "close": 101.0 + i})
        rows.append({"date": d, "symbol": "BBB", "open": 200.0 + i, "close": 199.0 + i})
    return pd.DataFrame(rows)


def _signals_breakout() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for d in _dates():
        rows.append({"date": d, "symbol": "AAA", "breakout_score": 1.0, "breakdown_score": 0.0})
        rows.append({"date": d, "symbol": "BBB", "breakout_score": 0.0, "breakdown_score": 1.0})
    return pd.DataFrame(rows)


def _signals_ep() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for d in _dates():
        rows.append({"date": d, "symbol": "AAA", "ep_score": 1.0})
        rows.append({"date": d, "symbol": "BBB", "ep_score": -1.0})
    return pd.DataFrame(rows)


def _base_config(tmp_path: Path) -> dict[str, object]:
    return {
        "run_name": "strategy_smoke",
        "start_date": "2020-01-01",
        "end_date": "2020-01-05",
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": True},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
        "sizing": {
            "class": "LongShortScoreSizer",
            "parameters": {
                "max_longs": 1,
                "max_shorts": 1,
                "gross_target": 1.0,
                "net_target": 0.0,
                "weight_mode": "equal",
                "sticky_holdings": False,
            },
        },
    }


def test_breakout_strategy_long_short_smoke(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config["strategy"] = {
        "class": "BreakoutStrategy",
        "parameters": {
            "breakout_score_column": "breakout_score",
            "breakdown_score_column": "breakdown_score",
            "enable_shorts": True,
            "partial_exit_days": None,
            "rebalance": "daily",
        },
    }
    cfg = BacktestConfig.from_dict(config)
    result = run_backtest(
        cfg,
        prices=_prices_frame(),
        signals=_signals_breakout(),
        run_id="RUNTEST-STRAT-BREAKOUT",
        output_base_dir=tmp_path,
    )

    run_dir = result.output_dir
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_positions.parquet").exists()
    assert (run_dir / "metrics_timeseries.parquet").exists()
    assert (run_dir / "summary.json").exists()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert len(trades) >= 1
    assert (trades["quantity"] > 0).any()
    assert (trades["quantity"] < 0).any()

    positions = pd.read_parquet(run_dir / "daily_positions.parquet")
    assert positions["date"].nunique() == 5
    assert positions["symbol"].nunique() == 2
    assert len(positions) == 10  # full snapshot: days * universe


def test_ep_strategy_long_short_smoke(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config["strategy"] = {
        "class": "EpisodicPivotStrategy",
        "parameters": {
            "ep_score_column": "ep_score",
            "enable_shorts": True,
            "rebalance": "daily",
        },
    }
    cfg = BacktestConfig.from_dict(config)
    result = run_backtest(
        cfg,
        prices=_prices_frame(),
        signals=_signals_ep(),
        run_id="RUNTEST-STRAT-EP",
        output_base_dir=tmp_path,
    )

    run_dir = result.output_dir
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_positions.parquet").exists()
    assert (run_dir / "metrics_timeseries.parquet").exists()
    assert (run_dir / "summary.json").exists()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert len(trades) >= 1
    assert (trades["quantity"] > 0).any()
    assert (trades["quantity"] < 0).any()

    positions = pd.read_parquet(run_dir / "daily_positions.parquet")
    assert positions["date"].nunique() == 5
    assert positions["symbol"].nunique() == 2
    assert len(positions) == 10  # full snapshot: days * universe

