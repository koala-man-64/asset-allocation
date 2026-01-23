from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from backtest.config import BacktestConfig
from backtest.models import PortfolioSnapshot
from backtest.runner import run_backtest
from backtest.sizer import KellySizer
from backtest.strategy import StrategyDecision


def _dates(start: date, n: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(n)]


def _close_from_returns(start_price: float, returns: list[float]) -> list[float]:
    closes = [float(start_price)]
    for r in returns:
        closes.append(closes[-1] * (1.0 + float(r)))
    return closes


def _prices_frame(dates: list[date], closes_by_symbol: dict[str, list[float]]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for symbol, closes in closes_by_symbol.items():
        assert len(closes) == len(dates)
        for d, close in zip(dates, closes, strict=True):
            rows.append({"date": d, "symbol": symbol, "open": close, "close": close})
    return pd.DataFrame(rows)


def test_kelly_sizer_fraction_zero_allocates_nothing() -> None:
    sizer = KellySizer(kelly_fraction=0.0, lookback_days=20, mu_scale=0.01)
    decision = StrategyDecision(scores={"AAA": 1.0})
    snapshot = PortfolioSnapshot(as_of=date(2024, 1, 1), cash=1000.0, positions={}, equity=1000.0)
    prices = pd.DataFrame({"date": [], "symbol": [], "close": []})
    target = sizer.size(snapshot.as_of, decision=decision, prices=prices, portfolio=snapshot)
    assert target.weights == {}


def test_kelly_sizer_diagonal_covariance_weights_match_mu_ratio() -> None:
    # Build returns with zero cross-covariance and equal variance:
    # AAA: [ +0.1, 0.0, -0.1, 0.0 ]
    # BBB: [  0.0, +0.1, 0.0, -0.1 ]
    dates = _dates(date(2024, 1, 1), 5)  # 4 return observations
    closes_aaa = _close_from_returns(100.0, [0.1, 0.0, -0.1, 0.0])
    closes_bbb = _close_from_returns(100.0, [0.0, 0.1, 0.0, -0.1])
    prices = _prices_frame(dates, {"AAA": closes_aaa, "BBB": closes_bbb})

    sizer = KellySizer(kelly_fraction=1.0, lookback_days=4, mu_scale=0.01)
    decision = StrategyDecision(scores={"AAA": 1.0, "BBB": 2.0})  # mu: 0.01 vs 0.02
    snapshot = PortfolioSnapshot(as_of=dates[-1], cash=1000.0, positions={}, equity=1000.0)

    target = sizer.size(snapshot.as_of, decision=decision, prices=prices, portfolio=snapshot)
    assert set(target.weights) == {"AAA", "BBB"}
    assert target.weights["AAA"] > 0
    assert target.weights["BBB"] > 0
    assert (target.weights["AAA"] / target.weights["BBB"]) == pytest.approx(0.5, rel=1e-12)


def test_kelly_sizer_outputs_signed_weights() -> None:
    dates = _dates(date(2024, 1, 1), 5)
    closes_aaa = _close_from_returns(100.0, [0.1, 0.0, -0.1, 0.0])
    closes_bbb = _close_from_returns(100.0, [0.0, 0.1, 0.0, -0.1])
    prices = _prices_frame(dates, {"AAA": closes_aaa, "BBB": closes_bbb})

    sizer = KellySizer(kelly_fraction=0.5, lookback_days=4, mu_scale=0.01)
    decision = StrategyDecision(scores={"AAA": -1.0, "BBB": 1.0})
    snapshot = PortfolioSnapshot(as_of=dates[-1], cash=1000.0, positions={}, equity=1000.0)

    target = sizer.size(snapshot.as_of, decision=decision, prices=prices, portfolio=snapshot)
    assert target.weights["AAA"] < 0
    assert target.weights["BBB"] > 0


def test_runner_builds_kelly_sizer_and_runs(tmp_path: Path) -> None:
    # Smoke test: ensure config wiring works end-to-end (trades may be empty for early windows).
    dates = _dates(date(2024, 1, 1), 6)
    closes_aaa = _close_from_returns(100.0, [0.01, 0.01, -0.005, 0.0, 0.01])
    closes_bbb = _close_from_returns(100.0, [0.0, 0.01, 0.0, -0.005, 0.01])
    prices = _prices_frame(dates, {"AAA": closes_aaa, "BBB": closes_bbb})

    cfg = {
        "run_name": "kelly_smoke",
        "start_date": dates[0].isoformat(),
        "end_date": dates[-1].isoformat(),
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "strategy": {"class": "StaticUniverseStrategy", "parameters": {"symbols": ["AAA", "BBB"]}},
        "sizing": {
            "class": "KellySizer",
            "parameters": {"kelly_fraction": 0.5, "lookback_days": 3, "mu_scale": 0.01},
        },
        "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": True},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path)},
    }

    cfg_obj = BacktestConfig.from_dict(cfg)
    result = run_backtest(config=cfg_obj, prices=prices, run_id="RUNTEST-KELLY", output_base_dir=tmp_path)

    assert (result.output_dir / "summary.json").exists()
