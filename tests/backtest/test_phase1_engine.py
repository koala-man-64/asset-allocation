from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pytest

from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.constraints import Constraints
from asset_allocation.backtest.engine import BacktestEngine
from asset_allocation.backtest.reporter import Reporter
from asset_allocation.backtest.runner import run_backtest
from asset_allocation.backtest.sizer import Sizer, TargetWeights
from asset_allocation.backtest.strategy import Strategy, StrategyDecision


def _prices_frame() -> pd.DataFrame:
    dates = [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]
    return pd.DataFrame(
        {
            "date": dates * 1,
            "symbol": ["AAA"] * 3,
            "open": [100.0, 111.0, 121.0],
            "close": [110.0, 120.0, 130.0],
        }
    )


def _config(tmp_path: Path) -> BacktestConfig:
    return BacktestConfig.from_dict(
        {
            "run_name": "phase1_smoke",
            "start_date": "2020-01-01",
            "end_date": "2020-01-03",
            "initial_cash": 1000.0,
            "universe": {"symbols": ["AAA"]},
            "strategy": {"class": "BuyAndHoldStrategy", "parameters": {"symbol": "AAA"}},
            "sizing": {"class": "EqualWeightSizer", "parameters": {"max_positions": 10}},
            "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False},
            "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
            "output": {"local_dir": str(tmp_path)},
        }
    )


def test_phase1_buy_and_hold_artifacts_and_execution_timing(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    result = run_backtest(cfg, prices=_prices_frame(), run_id="RUNTEST-000001", output_base_dir=tmp_path)

    run_dir = result.output_dir
    assert run_dir.name == "RUNTEST-000001"

    assert (run_dir / "config.yaml").exists()
    assert (run_dir / "trades.csv").exists()
    assert (run_dir / "daily_metrics.csv").exists()
    assert (run_dir / "monthly_returns.csv").exists()
    assert (run_dir / "summary.json").exists()
    assert (tmp_path / "run_index.csv").exists()

    trades = pd.read_csv(run_dir / "trades.csv")
    assert len(trades) == 1
    assert trades.loc[0, "symbol"] == "AAA"
    assert trades.loc[0, "execution_date"] == "2020-01-02"
    assert trades.loc[0, "quantity"] > 0

    metrics = pd.read_csv(run_dir / "daily_metrics.csv")
    assert metrics["date"].tolist() == ["2020-01-01", "2020-01-02", "2020-01-03"]
    assert metrics.loc[0, "portfolio_value"] == pytest.approx(1000.0)
    assert metrics.loc[2, "portfolio_value"] == pytest.approx(1171.171171, rel=1e-6)


class _NoLookaheadStrategy(Strategy):
    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio,
    ) -> StrategyDecision:
        assert not prices.empty
        assert "date" in prices.columns
        assert prices["date"].max() <= as_of
        return StrategyDecision(scores={"AAA": 1.0})


@dataclass(frozen=True)
class _PassthroughSizer(Sizer):
    def size(
        self,
        as_of: date,
        *,
        decision: StrategyDecision,
        prices: pd.DataFrame,
        portfolio,
    ) -> TargetWeights:
        return TargetWeights(weights={"AAA": 1.0})


def test_phase1_engine_never_passes_future_prices_to_strategy(tmp_path: Path) -> None:
    cfg = _config(tmp_path)
    reporter = Reporter.create(cfg, run_id="RUNTEST-LOOKAHEAD", output_dir=tmp_path)
    engine = BacktestEngine(
        config=cfg,
        prices=_prices_frame(),
        signals=None,
        strategy=_NoLookaheadStrategy(),
        sizer=_PassthroughSizer(),
        constraints=Constraints(config=cfg.constraints),
        reporter=reporter,
    )

    engine.run(run_id="RUNTEST-LOOKAHEAD")
    reporter.write_artifacts()
