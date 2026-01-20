from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from asset_allocation.backtest.config import BacktestConfig, validate_config_dict_strict
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


def _base_config(tmp_path: Path) -> dict[str, object]:
    return {
        "run_name": "configured_strategy_smoke",
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


def test_configured_strategy_schema_smoke(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config["strategy"] = {
        "type": "configured",
        "rebalance": {"freq": "daily"},
        "universe": {"source": "signals", "filters": [], "require_columns": ["symbol", "date", "open", "close"]},
        "signals": {"provider": "platinum_signals_daily", "columns": ["breakout_score", "breakdown_score"]},
        "scoring": {
            "type": "max_of",
            "scores": [
                {"type": "column", "column": "breakout_score"},
                {"type": "negate_column", "column": "breakdown_score"},
            ],
            "fillna": "drop",
        },
        "selection": {"type": "long_short_topn", "long_short_topn": {"long_n": 1, "short_n": 1, "allow_short": True}},
        "holding_policy": {"type": "replace_all", "replace_all": {"exit_if_not_selected": True}},
        "exits": {"precedence": "exit_over_scale", "rules": []},
        "postprocess": {"steps": []},
        "debug": {"record_intermediates": False, "record_reasons": False},
    }

    validate_config_dict_strict(config)
    cfg = BacktestConfig.from_dict(config)
    result = run_backtest(
        cfg,
        prices=_prices_frame(),
        signals=_signals_breakout(),
        run_id="RUNTEST-CONFIGURED",
        output_base_dir=tmp_path,
    )

    trades = pd.read_csv(result.output_dir / "trades.csv")
    assert len(trades) >= 1
    assert (trades["quantity"] > 0).any()
    assert (trades["quantity"] < 0).any()

