from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from backtest.config import BacktestConfig
from backtest.runner import run_backtest


def _dates() -> list[date]:
    start = date(2020, 1, 1)
    return [start + timedelta(days=i) for i in range(5)]


def _prices_frame() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for i, d in enumerate(_dates()):
        rows.append({"date": d, "symbol": "AAA", "open": 100.0 + i, "close": 101.0 + i})
        rows.append({"date": d, "symbol": "BBB", "open": 200.0 + i, "close": 199.0 + i})
    return pd.DataFrame(rows)


def _signals_for_column(column: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for d in _dates():
        rows.append({"date": d, "symbol": "AAA", column: 2.0})
        rows.append({"date": d, "symbol": "BBB", column: 1.0})
    return pd.DataFrame(rows)


def _base_config(tmp_path: Path) -> dict[str, object]:
    return {
        "run_name": "platinum_topn_smoke",
        "start_date": "2020-01-01",
        "end_date": "2020-01-05",
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
    }


def test_longshort_topn_strategy_long_only_smoke(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config["strategy"] = {"class": "LongShortTopNStrategy", "parameters": {"signal_column": "vcp_score", "k_long": 1}}
    config["sizing"] = {
        "class": "LongShortScoreSizer",
        "parameters": {"max_longs": 1, "max_shorts": 0, "gross_target": 1.0, "net_target": 1.0, "weight_mode": "equal"},
    }
    config["constraints"] = {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False}

    cfg = BacktestConfig.from_dict(config)
    result = run_backtest(
        cfg,
        prices=_prices_frame(),
        signals=_signals_for_column("vcp_score"),
        run_id="RUNTEST-PLAT-LONG",
        output_base_dir=tmp_path,
    )

    run_dir = result.output_dir
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_positions.parquet").exists()
    assert (run_dir / "metrics_timeseries.parquet").exists()
    assert (run_dir / "metrics_rolling.parquet").exists()
    assert (run_dir / "summary.json").exists()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert len(trades) >= 1
    assert (trades["quantity"] > 0).any()
    assert not (trades["quantity"] < 0).any()


def test_longshort_topn_strategy_short_only_smoke(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config["strategy"] = {
        "class": "LongShortTopNStrategy",
        "parameters": {"signal_column": "vcp_short_score", "k_short": 1, "long_if_high": False},
    }
    config["sizing"] = {
        "class": "LongShortScoreSizer",
        "parameters": {"max_longs": 0, "max_shorts": 1, "gross_target": 1.0, "net_target": -1.0, "weight_mode": "equal"},
    }
    config["constraints"] = {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": True}

    cfg = BacktestConfig.from_dict(config)
    result = run_backtest(
        cfg,
        prices=_prices_frame(),
        signals=_signals_for_column("vcp_short_score"),
        run_id="RUNTEST-PLAT-SHORT",
        output_base_dir=tmp_path,
    )

    run_dir = result.output_dir
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_positions.parquet").exists()
    assert (run_dir / "metrics_timeseries.parquet").exists()
    assert (run_dir / "metrics_rolling.parquet").exists()
    assert (run_dir / "summary.json").exists()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert len(trades) >= 1
    assert (trades["quantity"] < 0).any()

