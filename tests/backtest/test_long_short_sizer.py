from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from asset_allocation.backtest.models import PortfolioSnapshot
from asset_allocation.backtest.sizer import LongShortScoreSizer
from asset_allocation.backtest.strategy import StrategyDecision


def _snapshot(*, positions: dict[str, float] | None = None) -> PortfolioSnapshot:
    return PortfolioSnapshot(as_of=date(2024, 1, 1), cash=0.0, positions=positions or {}, equity=1.0)


def test_long_short_sizer_targets_gross_and_net_equal_weight() -> None:
    sizer = LongShortScoreSizer(
        max_longs=2,
        max_shorts=2,
        gross_target=1.0,
        net_target=0.0,
        weight_mode="equal",
        sticky_holdings=False,
    )
    decision = StrategyDecision(scores={"L1": 1.0, "L2": 2.0, "S1": -1.0, "S2": -2.0})
    out = sizer.size(date(2024, 1, 1), decision=decision, prices=pd.DataFrame(), portfolio=_snapshot())
    assert sum(out.weights.values()) == pytest.approx(0.0, abs=1e-12)
    assert sum(abs(w) for w in out.weights.values()) == pytest.approx(1.0, abs=1e-12)
    assert out.weights["L1"] == pytest.approx(0.25, abs=1e-12)
    assert out.weights["L2"] == pytest.approx(0.25, abs=1e-12)
    assert out.weights["S1"] == pytest.approx(-0.25, abs=1e-12)
    assert out.weights["S2"] == pytest.approx(-0.25, abs=1e-12)


def test_long_short_sizer_targets_nonzero_net() -> None:
    sizer = LongShortScoreSizer(
        max_longs=1,
        max_shorts=1,
        gross_target=1.0,
        net_target=0.4,
        weight_mode="equal",
        sticky_holdings=False,
    )
    decision = StrategyDecision(scores={"L1": 1.0, "S1": -1.0})
    out = sizer.size(date(2024, 1, 1), decision=decision, prices=pd.DataFrame(), portfolio=_snapshot())
    assert sum(out.weights.values()) == pytest.approx(0.4, abs=1e-12)
    assert sum(abs(w) for w in out.weights.values()) == pytest.approx(1.0, abs=1e-12)
    assert out.weights["L1"] == pytest.approx(0.7, abs=1e-12)
    assert out.weights["S1"] == pytest.approx(-0.3, abs=1e-12)


def test_long_short_sizer_applies_scales() -> None:
    sizer = LongShortScoreSizer(
        max_longs=2,
        max_shorts=0,
        gross_target=1.0,
        net_target=1.0,
        weight_mode="equal",
        sticky_holdings=False,
    )
    decision = StrategyDecision(scores={"A": 1.0, "B": 1.0}, scales={"A": 0.5})
    out = sizer.size(date(2024, 1, 1), decision=decision, prices=pd.DataFrame(), portfolio=_snapshot())
    assert sum(out.weights.values()) == pytest.approx(1.0, abs=1e-12)
    assert out.weights["A"] < out.weights["B"]
    assert out.weights["A"] == pytest.approx(1.0 / 3.0, rel=1e-12)
    assert out.weights["B"] == pytest.approx(2.0 / 3.0, rel=1e-12)


def test_long_short_sizer_sticky_holdings_keeps_current_position() -> None:
    sizer = LongShortScoreSizer(
        max_longs=2,
        max_shorts=0,
        gross_target=1.0,
        net_target=1.0,
        weight_mode="equal",
        sticky_holdings=True,
    )
    decision = StrategyDecision(scores={"A": 1.0, "B": 3.0, "C": 2.0})
    out = sizer.size(date(2024, 1, 1), decision=decision, prices=pd.DataFrame(), portfolio=_snapshot(positions={"A": 10.0}))
    assert set(out.weights) == {"A", "B"}
