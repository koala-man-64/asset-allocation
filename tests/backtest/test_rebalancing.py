
import unittest
from datetime import date
import pandas as pd
from asset_allocation.backtest.config import (
    BacktestConfig, UniverseConfig, DataConfig, ComponentConfig, ConstraintsConfig, BrokerConfig, OutputConfig
)
from asset_allocation.backtest.engine import BacktestEngine
from asset_allocation.backtest.strategy import Strategy, StrategyDecision
from asset_allocation.backtest.sizer import EqualWeightSizer
from asset_allocation.backtest.constraints import Constraints
from asset_allocation.backtest.reporter import Reporter
from asset_allocation.backtest.models import PortfolioSnapshot

class MockStrategy(Strategy):
    """Always targets 50% allocation to 'AAPL' to force rebalancing if price moves."""
    def __init__(self, rebalance="daily"):
        super().__init__(rebalance=rebalance)

    def on_bar(self, as_of: date, prices: pd.DataFrame, signals, portfolio: PortfolioSnapshot):
        if not self.check_rebalance(as_of):
            return None
        return StrategyDecision(scores={"AAPL": 0.5}) # Target 50%

class TestRebalancing(unittest.TestCase):
    def setUp(self):
        # 10 days of prices: 2 weeks (Mon-Fri)
        self.dates = pd.date_range("2024-01-01", periods=14, freq="D") # Jan 1 is Monday
        self.dates = [d.date() for d in self.dates if d.weekday() < 5] # M-F only
        # 10 trading days: Jan 1-5, Jan 8-12
        
        data = []
        price = 100.0
        for d in self.dates:
            data.append({"date": d, "symbol": "AAPL", "open": price, "close": price})
            price *= 1.01 # Price increasing 1% daily -> forces 50% target to sell/buy to maintain ratio
            # If we hold 50% AAPL, and AAPL goes up, our weight becomes > 50%.
            # Next rebalance should SELL to get back to 50%.
        
        self.prices = pd.DataFrame(data)
        
        self.base_config = BacktestConfig(
            start_date=self.dates[0],
            end_date=self.dates[-1],
            universe=UniverseConfig(symbols=["AAPL"]),
            strategy=ComponentConfig(class_name="MockStrategy"),
            data=DataConfig(price_source="local"),
            initial_cash=100_000,
            broker=BrokerConfig(commission=0.0) # Zero comms to purely test rebalance logic
        )

    def run_engine(self, rebalance_freq):
        strategy = MockStrategy(rebalance=rebalance_freq)
        sizer = EqualWeightSizer() # Will just take the 0.5 score? 
        # Wait, EqualWeightSizer ignores scores values and does 1/N. 
        # If MockStrategy returns scores={'AAPL': 0.5}, EqualWeightSizer sees 1 asset -> 100% allocation.
        # We need a Sizer that respects the weights or use a custom Sizer.
        # Let's use a custom sizer in the test or modify MockStrategy to output 1.0 and assume BuyAndHold behavior?
        # If I use EqualWeightSizer, it targets 100%. 
        # If price goes up, weight stays 100%. No rebalance needed.
        # I need a strategy+sizer that targets Fixed Weight (e.g. 50%).
        
        # Override Sizer for test
        class FixedWeightSizer(EqualWeightSizer):
            def size(self, as_of, decision, prices, portfolio):
                from asset_allocation.backtest.sizer import TargetWeights
                return TargetWeights(weights=decision.scores) # Use raw scores as weights

        sizer = FixedWeightSizer()
        
        constraints = Constraints(ConstraintsConfig())
        from pathlib import Path
        reporter = Reporter(
            config=self.base_config,
            run_id="TEST",
            output_dir=Path("./test_results")
        )
        
        engine = BacktestEngine(
            config=self.base_config,
            prices=self.prices,
            signals=None,
            strategy=strategy,
            sizer=sizer,
            constraints=constraints,
            reporter=reporter
        )
        engine.run(run_id="TEST")
        return reporter

    def test_daily_rebalancing(self):
        """With daily rebalance, and price moving, should trade every day."""
        reporter = self.run_engine("daily")
        trades = reporter._trades
        # Day 1: Buy. Day 2-10: Rebalance trades.
        # Expect ~10 trades (or 9 rebalances).
        self.assertGreater(len(trades), 5)
        
    def test_weekly_rebalancing(self):
        """With weekly rebalance, should trade on Day 1 (Jan 1) and Day 6 (Jan 8)."""
        reporter = self.run_engine("weekly")
        trades = reporter._trades
        
        # Parse ISO strings to date objects
        trade_dates = sorted(list(set(date.fromisoformat(t["execution_date"]) for t in trades)))
        print(f"Weekly Trade Dates: {trade_dates}")
        
        # Expect trades on 2024-01-01 and 2024-01-08
        self.assertEqual(len(trade_dates), 2)
        self.assertEqual(trade_dates[0], date(2024, 1, 2)) # Execution is Next Open (Jan 2)
        self.assertEqual(trade_dates[1], date(2024, 1, 9)) # Execution is Next Open (Jan 9)
        
    def test_monthly_rebalancing(self):
        reporter = self.run_engine("monthly")
        trades = reporter._trades
        trade_dates = sorted(list(set(date.fromisoformat(t["execution_date"]) for t in trades)))
        
        # Should only trade once (Jan 1 -> Jan 2)
        self.assertEqual(len(trade_dates), 1)

if __name__ == "__main__":
    unittest.main()
