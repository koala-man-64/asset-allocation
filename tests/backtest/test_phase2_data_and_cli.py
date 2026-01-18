from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml

from asset_allocation.backtest.cli import main as cli_main
from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.data_access import load_backtest_inputs
from scripts.common import delta_core


def _make_config_dict(tmp_path: Path) -> dict:
    return {
        "run_name": "phase2_smoke",
        "start_date": "2020-01-01",
        "end_date": "2020-01-03",
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "data": {
            "price_source": "local",
            "price_path": str(tmp_path / "prices.csv"),
            "signal_path": str(tmp_path / "signals.csv"),
        },
        "strategy": {"class": "TopNSignalStrategy", "parameters": {"signal_column": "composite_percentile", "top_n": 1}},
        "sizing": {"class": "EqualWeightSizer", "parameters": {"max_positions": 10}},
        "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
    }


def test_load_backtest_inputs_local_filters_universe_and_dates(tmp_path: Path) -> None:
    prices = pd.DataFrame(
        {
            "Date": [date(2019, 12, 31), date(2020, 1, 1), date(2020, 1, 2)],
            "Symbol": ["AAA", "AAA", "BBB"],
            "Open": [1.0, 10.0, 20.0],
            "Close": [1.1, 11.0, 21.0],
        }
    )
    signals = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 2)],
            "symbol": ["AAA", "BBB"],
            "composite_percentile": [1.0, 0.0],
        }
    )

    prices_path = tmp_path / "prices.csv"
    signals_path = tmp_path / "signals.csv"
    prices.to_csv(prices_path, index=False)
    signals.to_csv(signals_path, index=False)

    cfg = BacktestConfig.from_dict(_make_config_dict(tmp_path))
    loaded_prices, loaded_signals = load_backtest_inputs(cfg)

    assert set(loaded_prices["Symbol"].unique().tolist()) == {"AAA", "BBB"}
    assert loaded_prices["Date"].min() >= pd.Timestamp("2020-01-01")
    assert loaded_signals is not None
    assert set(loaded_signals["symbol"].unique().tolist()) == {"AAA", "BBB"}


def test_load_backtest_inputs_delta_supports_per_symbol_prices(tmp_path: Path) -> None:
    # Write per-symbol Delta tables into redirected local storage (tests/conftest.py patches URI).
    df_aaa = pd.DataFrame(
        {
            "Date": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
            "Open": [100.0, 110.0, 120.0],
            "Close": [101.0, 111.0, 121.0],
        }
    )
    df_bbb = pd.DataFrame(
        {
            "Date": [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)],
            "Open": [200.0, 210.0, 220.0],
            "Close": [201.0, 211.0, 221.0],
        }
    )

    delta_core.store_delta(df_aaa, container="silver", path="market-data/AAA", mode="overwrite")
    delta_core.store_delta(df_bbb, container="silver", path="market-data/BBB", mode="overwrite")

    signals = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 1)],
            "symbol": ["AAA", "BBB"],
            "composite_percentile": [1.0, 0.0],
        }
    )
    delta_core.store_delta(signals, container="ranking-data", path="gold/composite_signals", mode="overwrite")

    cfg = BacktestConfig.from_dict(
        {
            "run_name": "phase2_delta",
            "start_date": "2020-01-01",
            "end_date": "2020-01-03",
            "initial_cash": 1000.0,
            "universe": {"symbols": ["AAA", "BBB"]},
            "data": {
                "price_source": "ADLS",
                "price_path": "silver/market-data/{symbol}",
                "signal_path": "ranking-data/gold/composite_signals",
            },
            "strategy": {"class": "TopNSignalStrategy", "parameters": {"signal_column": "composite_percentile", "top_n": 1}},
            "sizing": {"class": "EqualWeightSizer"},
            "broker": {"fill_policy": "next_open"},
            "output": {"local_dir": str(tmp_path)},
        }
    )

    loaded_prices, loaded_signals = load_backtest_inputs(cfg)
    assert loaded_prices is not None and not loaded_prices.empty
    # Symbol column is injected because the per-symbol tables omitted it.
    assert set(loaded_prices["symbol"].unique().tolist()) == {"AAA", "BBB"}
    assert loaded_signals is not None
    assert set(loaded_signals["symbol"].unique().tolist()) == {"AAA", "BBB"}


def test_cli_runs_end_to_end_and_writes_monthly_returns(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    prices = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 2), date(2020, 1, 3), date(2020, 1, 3)],
            "symbol": ["AAA", "BBB"] * 3,
            "open": [100.0, 200.0, 110.0, 210.0, 120.0, 220.0],
            "close": [101.0, 201.0, 111.0, 211.0, 121.0, 221.0],
        }
    )
    signals = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 2)],
            "symbol": ["AAA", "BBB"] * 2,
            "composite_percentile": [1.0, 0.0, 1.0, 0.0],
        }
    )

    prices_path = tmp_path / "prices.csv"
    signals_path = tmp_path / "signals.csv"
    prices.to_csv(prices_path, index=False)
    signals.to_csv(signals_path, index=False)

    config_path = tmp_path / "backtest.yaml"
    config_dict = _make_config_dict(tmp_path)
    config_dict["data"]["price_path"] = str(prices_path)
    config_dict["data"]["signal_path"] = str(signals_path)
    config_path.write_text(yaml.safe_dump(config_dict, sort_keys=False), encoding="utf-8")

    rc = cli_main(["-c", str(config_path), "--run-id", "RUNTEST-CLI", "--output-dir", str(tmp_path)])
    assert rc == 0

    run_dir = tmp_path / "RUNTEST-CLI"
    assert (run_dir / "config.yaml").exists()
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_metrics.csv").exists()
    assert (run_dir / "monthly_returns.csv").exists()
    assert (run_dir / "summary.json").exists()

    out = capsys.readouterr().out
    assert "run_id=RUNTEST-CLI" in out
