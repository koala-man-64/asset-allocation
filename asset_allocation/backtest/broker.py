from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Tuple

from asset_allocation.backtest.config import BrokerConfig
from asset_allocation.backtest.models import ExecutionCosts, TradeFill
from asset_allocation.backtest.portfolio import Portfolio


@dataclass
class SimulatedBroker:
    config: BrokerConfig
    portfolio: Portfolio

    def execute_target_weights(
        self,
        execution_date: date,
        *,
        target_weights: Dict[str, float],
        open_prices: Dict[str, float],
    ) -> Tuple[List[TradeFill], ExecutionCosts]:
        slippage_bps = float(self.config.slippage_bps)
        commission_rate = float(self.config.commission)

        # Compute equity at open using current positions valued at open.
        equity_open = self.portfolio.equity(open_prices)
        if equity_open <= 0:
            return [], ExecutionCosts()

        all_symbols = set(self.portfolio.positions.keys()) | set(target_weights.keys())
        fills: List[TradeFill] = []
        total_commission = 0.0
        total_slippage = 0.0

        for symbol in sorted(all_symbols):
            open_price = open_prices.get(symbol)
            if open_price is None or open_price <= 0:
                continue

            current_shares = self.portfolio.shares(symbol)
            current_value = current_shares * open_price
            target_weight = float(target_weights.get(symbol, 0.0))
            target_value = target_weight * equity_open
            delta_value = target_value - current_value
            quantity = delta_value / open_price

            if abs(quantity) < 1e-12:
                continue

            slip_multiplier = 1.0 + (slippage_bps / 10_000.0) * (1.0 if quantity > 0 else -1.0)
            fill_price = open_price * slip_multiplier
            notional = quantity * fill_price

            commission = abs(notional) * commission_rate
            slippage_cost = abs(quantity) * abs(fill_price - open_price)

            # Cash update; notional sign handles buy/sell.
            self.portfolio.cash -= notional
            self.portfolio.cash -= commission

            total_commission += commission
            total_slippage += slippage_cost

            self.portfolio.set_shares(symbol, current_shares + quantity)

            fills.append(
                TradeFill(
                    execution_date=execution_date,
                    symbol=symbol,
                    quantity=quantity,
                    price=fill_price,
                    notional=notional,
                    commission=commission,
                    slippage_cost=slippage_cost,
                    cash_after=self.portfolio.cash,
                )
            )

        return fills, ExecutionCosts(commission=total_commission, slippage_cost=total_slippage)

