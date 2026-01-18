
import pandas as pd
import pytest
from datetime import date, timedelta
from asset_allocation.backtest.strategy import Strategy
from asset_allocation.backtest.models import PortfolioSnapshot

class MockStrategy(Strategy):
    """Simple concrete strategy to expose risk methods for testing."""
    def on_bar(self, as_of, prices, signals, portfolio):
        # Must call sync first
        self._sync_risk_state(as_of=as_of, prices=prices, portfolio=portfolio)
        
        exits = []
        if portfolio.positions:
            for sym in portfolio.positions:
                if self._check_risk_exits(as_of=as_of, symbol=sym, prices=prices):
                    exits.append(sym)
        return exits

@pytest.fixture
def sample_prices():
    dates = [date(2023, 1, 1) + timedelta(days=i) for i in range(10)]
    data = []
    for d in dates:
        # A flat market that sets a baseline
        data.append({"date": d, "symbol": "AAPL", "open": 100.0, "high": 105.0, "low": 95.0, "close": 100.0})
    return pd.DataFrame(data)

def test_stop_loss_long():
    """Verify Long Stop Loss triggers when low drops below threshold."""
    strat = MockStrategy(stop_loss_pct=0.10, use_low_for_stop=True)
    
    # Day 1: Entry
    d1 = date(2023, 1, 1)
    prices_d1 = pd.DataFrame([
        {"date": d1, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}
    ])
    port_d1 = PortfolioSnapshot(as_of=d1, cash=0, positions={"AAPL": 10}, equity=1000)
    
    strat.on_bar(d1, prices=prices_d1, signals=None, portfolio=port_d1)
    assert strat._entry_price["AAPL"] == 100.0

    # Day 2: Drop to 89 (Low) -> Should trigger 10% SL (threshold 90)
    d2 = date(2023, 1, 2)
    prices_d2 = pd.DataFrame([
        {"date": d2, "symbol": "AAPL", "open": 95.0, "high": 95.0, "low": 89.0, "close": 92.0}
    ])
    exits = strat.on_bar(d2, prices=prices_d2, signals=None, portfolio=port_d1) # Still holding
    assert "AAPL" in exits

def test_take_profit_long():
    """Verify Long Take Profit triggers when high exceeds threshold."""
    strat = MockStrategy(take_profit_pct=0.20) # Target 120
    
    d1 = date(2023, 1, 1)
    prices_d1 = pd.DataFrame([{"date": d1, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}])
    port_d1 = PortfolioSnapshot(as_of=d1, cash=0, positions={"AAPL": 10}, equity=1000)
    strat.on_bar(d1, prices=prices_d1, signals=None, portfolio=port_d1)

    # Day 2: Spike to 121 (High)
    d2 = date(2023, 1, 2)
    prices_d2 = pd.DataFrame([{"date": d2, "symbol": "AAPL", "open": 105.0, "high": 121.0, "low": 105.0, "close": 110.0}])
    
    exits = strat.on_bar(d2, prices=prices_d2, signals=None, portfolio=port_d1)
    assert "AAPL" in exits

def test_trailing_stop_long():
    """Verify Trailing Stop tracks HWM."""
    strat = MockStrategy(trailing_stop_pct=0.10) # 10% trail
    
    d1 = date(2023, 1, 1)
    prices_d1 = pd.DataFrame([{"date": d1, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}])
    port_d1 = PortfolioSnapshot(as_of=d1, cash=0, positions={"AAPL": 10}, equity=1000)
    strat.on_bar(d1, prices=prices_d1, signals=None, portfolio=port_d1)
    
    # Day 2: Market Rally -> HWM should update to 120
    # We set Low=110 so it doesn't trigger 10% trailing stop (120*0.9=108).
    d2 = date(2023, 1, 2)
    prices_d2 = pd.DataFrame([{"date": d2, "symbol": "AAPL", "open": 105.0, "high": 120.0, "low": 110.0, "close": 115.0}])
    exits = strat.on_bar(d2, prices=prices_d2, signals=None, portfolio=port_d1)
    assert "AAPL" not in exits
    assert strat._high_water_marks["AAPL"] == 120.0 # Verify HWM update

    # Day 3: Drop. Threshold is 120 * 0.9 = 108.
    # Case A: Low 110 -> Safe
    d3 = date(2023, 1, 3)
    prices_d3 = pd.DataFrame([{"date": d3, "symbol": "AAPL", "open": 115.0, "high": 116.0, "low": 110.0, "close": 112.0}])
    exits = strat.on_bar(d3, prices=prices_d3, signals=None, portfolio=port_d1)
    assert "AAPL" not in exits
    
    # Case B: Low 107 -> Exit
    d4 = date(2023, 1, 4)
    prices_d4 = pd.DataFrame([{"date": d4, "symbol": "AAPL", "open": 110.0, "high": 110.0, "low": 107.0, "close": 108.0}])
    exits = strat.on_bar(d4, prices=prices_d4, signals=None, portfolio=port_d1)
    assert "AAPL" in exits

def test_time_stop():
    """Verify Time Stop exits after N days."""
    strat = MockStrategy(time_stop_days=3) # Hold for 3 days max (exit on 3rd day, ie after 2 nights)
    
    d0 = date(2023, 1, 1) # Entry
    prices_d0 = pd.DataFrame([{"date": d0, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}])
    port = PortfolioSnapshot(as_of=d0, cash=0, positions={"AAPL": 10}, equity=1000)
    
    # day 0: Entry
    strat.on_bar(d0, prices=prices_d0, signals=None, portfolio=port)
    
    # Day 1: count=2 (d0, d1). 2 < 3. Safe.
    d1 = date(2023, 1, 2)
    row_d1 = {"date": d1, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}
    history_1 = pd.concat([prices_d0, pd.DataFrame([row_d1])], ignore_index=True)
    exits = strat.on_bar(d1, prices=history_1, signals=None, portfolio=port)
    assert "AAPL" not in exits

    # Day 2: count=3 (d0, d1, d2). 3 >= 3. Exit.
    d2 = date(2023, 1, 3)
    row_d2 = {"date": d2, "symbol": "AAPL", "open": 100.0, "high": 100.0, "low": 100.0, "close": 100.0}
    history_2 = pd.concat([history_1, pd.DataFrame([row_d2])], ignore_index=True)
    exits = strat.on_bar(d2, prices=history_2, signals=None, portfolio=port)
    assert "AAPL" in exits
