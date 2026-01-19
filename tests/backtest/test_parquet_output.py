from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml

from asset_allocation.backtest.cli import main as cli_main

def _make_config_dict(tmp_path: Path) -> dict:
    return {
        "run_name": "parquet_test_run",
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
        "output": {
            "local_dir": str(tmp_path),
            "save_trades_parquet": True,
            "save_config_parquet": True,
            "save_summary_parquet": True,
            "save_metrics_parquet": True,
        },
    }

def test_parquet_output_generation(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    # 1. Setup Data
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

    # 2. Setup Config
    config_path = tmp_path / "backtest.yaml"
    config_dict = _make_config_dict(tmp_path)
    config_path.write_text(yaml.safe_dump(config_dict, sort_keys=False), encoding="utf-8")

    # 3. Run Backtest via CLI
    rc = cli_main(["-c", str(config_path), "--run-id", "PARQUET-TEST", "--output-dir", str(tmp_path)])
    assert rc == 0

    # 4. Verify Artifacts
    run_dir = tmp_path / "PARQUET-TEST"
    
    # Check Trades Parquet
    trades_parquet = run_dir / "trades.parquet"
    assert trades_parquet.exists(), "trades.parquet should exist"
    # Basic content check
    trades_df = pd.read_parquet(trades_parquet)
    assert not trades_df.empty, "trades.parquet should not be empty"

    # Check Config Parquet
    config_parquet = run_dir / "config.parquet"
    assert config_parquet.exists(), "config.parquet should exist"
    config_df = pd.read_parquet(config_parquet)
    assert len(config_df) == 1, "config.parquet should have 1 row"
    assert "config_json" in config_df.columns

    # Check Summary Parquet
    summary_parquet = run_dir / "summary.parquet"
    assert summary_parquet.exists(), "summary.parquet should exist"
    summary_df = pd.read_parquet(summary_parquet)
    assert len(summary_df) == 1, "summary.parquet should have 1 row"
    assert "total_return" in summary_df.columns

    print("Verified parquet outputs successfully!")
