from __future__ import annotations

from datetime import date

import pytest

from asset_allocation.backtest.broker import SimulatedBroker
from asset_allocation.backtest.config import BrokerConfig
from asset_allocation.backtest.models import MarketBar, MarketSnapshot
from asset_allocation.backtest.portfolio import Portfolio


def test_broker_tracks_position_state_and_avg_entry_price() -> None:
    portfolio = Portfolio(cash=1000.0)
    broker = SimulatedBroker(
        config=BrokerConfig(slippage_bps=0.0, spread_bps=0.0, commission=0.0),
        portfolio=portfolio,
    )

    d1 = date(2020, 1, 2)
    result1 = broker.execute_target_weights(
        MarketSnapshot(as_of=d1, bar_index=1, bars={"AAA": MarketBar(open=100.0, volume=1000.0)}),
        target_weights={"AAA": 1.0},
    )
    assert len(result1.fills) == 1
    state1 = broker.get_position_states()["AAA"]
    assert state1.shares == 10.0
    assert state1.avg_entry_price == 100.0
    assert state1.entry_date == d1
    assert state1.entry_bar_index == 1
    assert state1.last_fill_date == d1

    d2 = date(2020, 1, 3)
    result2 = broker.execute_target_weights(
        MarketSnapshot(as_of=d2, bar_index=2, bars={"AAA": MarketBar(open=110.0, volume=1000.0)}),
        target_weights={"AAA": 2.0},
    )
    assert len(result2.fills) == 1
    state2 = broker.get_position_states()["AAA"]
    assert state2.shares == 20.0
    assert state2.avg_entry_price == 105.0
    assert state2.entry_date == d1
    assert state2.entry_bar_index == 1
    assert state2.last_fill_date == d2

    d3 = date(2020, 1, 4)
    # Reduce to 10 shares at open=120 by setting target value to 10*120=1200.
    equity_open = portfolio.cash + state2.shares * 120.0
    target_weight = 1200.0 / equity_open
    result3 = broker.execute_target_weights(
        MarketSnapshot(as_of=d3, bar_index=3, bars={"AAA": MarketBar(open=120.0, volume=1000.0)}),
        target_weights={"AAA": target_weight},
    )
    assert len(result3.fills) == 1
    assert result3.fills[0].quantity == pytest.approx(-10.0, abs=1e-9)
    state3 = broker.get_position_states()["AAA"]
    assert state3.shares == pytest.approx(10.0, abs=1e-9)
    assert state3.avg_entry_price == 105.0
    assert state3.entry_date == d1
    assert state3.entry_bar_index == 1
    assert state3.last_fill_date == d3


def test_broker_enforces_rounding_and_participation_cap() -> None:
    cfg = BrokerConfig(
        slippage_bps=0.0,
        spread_bps=0.0,
        commission=0.0,
        allow_fractional_shares=False,
        lot_size=1,
        rounding_mode="toward_zero",
        min_trade_notional=0.0,
        min_trade_shares=0.0,
        on_missing_price="reject",
        max_participation_rate=0.1,
    )

    portfolio = Portfolio(cash=1000.0)
    broker = SimulatedBroker(config=cfg, portfolio=portfolio)

    d1 = date(2020, 1, 2)
    rounded = broker.execute_target_weights(
        MarketSnapshot(as_of=d1, bar_index=1, bars={"AAA": MarketBar(open=300.0, volume=1000.0)}),
        target_weights={"AAA": 0.1},  # target value 100 => 0.333 shares => rounds to 0
    )
    assert rounded.fills == []
    assert any(r.reason == "rounded_to_zero" for r in rounded.rejects)
    assert broker.get_position_states() == {}

    capped = broker.execute_target_weights(
        MarketSnapshot(as_of=d1, bar_index=1, bars={"AAA": MarketBar(open=100.0, volume=50.0)}),
        target_weights={"AAA": 1.0},  # target 10 shares, cap 5 shares at 10% of 50 volume
    )
    assert len(capped.fills) == 1
    assert capped.fills[0].quantity == 5.0
    assert any(r.reason == "participation_cap" for r in capped.rejects)


def test_broker_respects_min_trade_notional() -> None:
    portfolio = Portfolio(cash=1000.0)
    broker = SimulatedBroker(
        config=BrokerConfig(slippage_bps=0.0, spread_bps=0.0, commission=0.0, min_trade_notional=2000.0),
        portfolio=portfolio,
    )

    d1 = date(2020, 1, 2)
    result = broker.execute_target_weights(
        MarketSnapshot(as_of=d1, bar_index=1, bars={"AAA": MarketBar(open=100.0, volume=1000.0)}),
        target_weights={"AAA": 1.0},
    )
    assert result.fills == []
    assert any(r.reason == "min_trade_notional" for r in result.rejects)
    assert broker.get_position_states() == {}

