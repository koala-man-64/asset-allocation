from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List

from asset_allocation.backtest.config import BrokerConfig
from asset_allocation.backtest.models import (
    ExecutionCosts,
    ExecutionReject,
    ExecutionResult,
    MarketSnapshot,
    PositionState,
    TradeFill,
)
from asset_allocation.backtest.portfolio import Portfolio


_EPS = 1e-12


def _shares_sign(shares: float) -> int:
    if shares > 0:
        return 1
    if shares < 0:
        return -1
    return 0


def _round_to_lot(quantity: float, *, lot_size: int, mode: str) -> float:
    if lot_size <= 1:
        if mode == "toward_zero":
            return float(math.trunc(quantity))
        if mode == "nearest":
            return float(round(quantity))
        if mode == "floor":
            return float(math.floor(quantity))
        if mode == "ceil":
            return float(math.ceil(quantity))
        return float(quantity)

    units = quantity / float(lot_size)
    if mode == "toward_zero":
        rounded_units = math.trunc(units)
    elif mode == "nearest":
        rounded_units = round(units)
    elif mode == "floor":
        rounded_units = math.floor(units)
    elif mode == "ceil":
        rounded_units = math.ceil(units)
    else:
        rounded_units = units
    return float(rounded_units) * float(lot_size)


@dataclass
class SimulatedBroker:
    config: BrokerConfig
    portfolio: Portfolio
    _position_states: Dict[str, PositionState] = field(default_factory=dict, init=False)

    def get_position_states(self) -> Dict[str, PositionState]:
        states: Dict[str, PositionState] = {}
        for symbol, shares in (self.portfolio.positions or {}).items():
            if abs(float(shares)) < _EPS:
                continue
            existing = self._position_states.get(symbol)
            if existing is None or abs(float(existing.shares) - float(shares)) > 1e-9:
                states[symbol] = PositionState(
                    symbol=symbol,
                    shares=float(shares),
                    avg_entry_price=existing.avg_entry_price if existing else None,
                    entry_date=existing.entry_date if existing else None,
                    entry_bar_index=existing.entry_bar_index if existing else None,
                    last_fill_date=existing.last_fill_date if existing else None,
                )
            else:
                states[symbol] = existing
        # Best-effort cleanup of stale state entries.
        stale = set(self._position_states.keys()) - set(states.keys())
        for symbol in stale:
            self._position_states.pop(symbol, None)
        return dict(states)

    def execute_target_weights(self, market: MarketSnapshot, *, target_weights: Dict[str, float]) -> ExecutionResult:
        slippage_bps = float(self.config.slippage_bps)
        spread_bps = float(self.config.spread_bps)
        commission_rate = float(self.config.commission)

        open_prices: Dict[str, float] = {}
        for symbol, bar in (market.bars or {}).items():
            if bar.open is None:
                continue
            open_prices[str(symbol)] = float(bar.open)

        equity_open = self.portfolio.equity(open_prices)
        if equity_open <= 0:
            return ExecutionResult(fills=[], costs=ExecutionCosts(), rejects=[])

        all_symbols = set(self.portfolio.positions.keys()) | set(target_weights.keys())
        fills: List[TradeFill] = []
        total_commission = 0.0
        total_slippage = 0.0
        rejects: List[ExecutionReject] = []

        lot_size = int(self.config.lot_size)
        rounding_mode = str(self.config.rounding_mode)
        allow_fractional = bool(self.config.allow_fractional_shares)
        min_trade_notional = float(self.config.min_trade_notional)
        min_trade_shares = float(self.config.min_trade_shares)
        on_missing_price = str(self.config.on_missing_price)
        max_participation_rate = self.config.max_participation_rate
        half_spread_bps = spread_bps / 2.0

        for symbol in sorted(all_symbols):
            bar = market.bars.get(symbol) if market.bars else None
            open_price = open_prices.get(symbol)
            if open_price is None or open_price <= 0:
                if on_missing_price == "reject":
                    rejects.append(
                        ExecutionReject(
                            execution_date=market.as_of,
                            symbol=symbol,
                            reason="missing_open_price",
                            requested_qty=0.0,
                            executed_qty=0.0,
                        )
                    )
                continue

            current_shares = self.portfolio.shares(symbol)
            current_value = current_shares * open_price
            target_weight = float(target_weights.get(symbol, 0.0))
            target_value = target_weight * equity_open
            delta_value = target_value - current_value
            requested_qty = delta_value / open_price
            if abs(requested_qty) < _EPS:
                continue

            quantity = float(requested_qty)
            if not allow_fractional or lot_size != 1:
                quantity = _round_to_lot(quantity, lot_size=max(1, lot_size), mode=rounding_mode)
                if abs(quantity) < _EPS:
                    rejects.append(
                        ExecutionReject(
                            execution_date=market.as_of,
                            symbol=symbol,
                            reason="rounded_to_zero",
                            requested_qty=float(requested_qty),
                            executed_qty=0.0,
                            requested_notional=float(requested_qty * open_price),
                            executed_notional=0.0,
                        )
                    )
                    continue

            executed_qty = float(quantity)
            if max_participation_rate is not None and bar is not None and bar.volume is not None and bar.volume > 0:
                cap_qty = float(max_participation_rate) * float(bar.volume)
                if cap_qty >= 0 and abs(executed_qty) > cap_qty + _EPS:
                    clipped = math.copysign(cap_qty, executed_qty)
                    rejects.append(
                        ExecutionReject(
                            execution_date=market.as_of,
                            symbol=symbol,
                            reason="participation_cap",
                            requested_qty=float(executed_qty),
                            executed_qty=float(clipped),
                            requested_notional=float(executed_qty * open_price),
                            executed_notional=float(clipped * open_price),
                        )
                    )
                    executed_qty = float(clipped)
                    if abs(executed_qty) < _EPS:
                        continue

            side = 1.0 if executed_qty > 0 else -1.0
            impact_bps = half_spread_bps + slippage_bps
            slip_multiplier = 1.0 + (impact_bps / 10_000.0) * side
            fill_price = open_price * slip_multiplier
            notional = executed_qty * fill_price

            commission = abs(notional) * commission_rate
            slippage_cost = abs(executed_qty) * abs(fill_price - open_price)

            if min_trade_shares > 0 and abs(executed_qty) < float(min_trade_shares) - _EPS:
                rejects.append(
                    ExecutionReject(
                        execution_date=market.as_of,
                        symbol=symbol,
                        reason="min_trade_shares",
                        requested_qty=float(requested_qty),
                        executed_qty=0.0,
                        requested_notional=float(requested_qty * open_price),
                        executed_notional=0.0,
                    )
                )
                continue

            if min_trade_notional > 0 and abs(notional) < float(min_trade_notional) - _EPS:
                rejects.append(
                    ExecutionReject(
                        execution_date=market.as_of,
                        symbol=symbol,
                        reason="min_trade_notional",
                        requested_qty=float(requested_qty),
                        executed_qty=0.0,
                        requested_notional=float(requested_qty * open_price),
                        executed_notional=0.0,
                    )
                )
                continue

            # Cash update; notional sign handles buy/sell.
            self.portfolio.cash -= notional
            self.portfolio.cash -= commission

            total_commission += commission
            total_slippage += slippage_cost

            self.portfolio.set_shares(symbol, current_shares + executed_qty)
            self._update_position_state(
                symbol=symbol,
                execution_date=market.as_of,
                bar_index=int(market.bar_index),
                previous_shares=float(current_shares),
                fill_qty=float(executed_qty),
                fill_price=float(fill_price),
            )

            fills.append(
                TradeFill(
                    execution_date=market.as_of,
                    symbol=symbol,
                    quantity=executed_qty,
                    price=fill_price,
                    notional=notional,
                    commission=commission,
                    slippage_cost=slippage_cost,
                    cash_after=self.portfolio.cash,
                )
            )

        return ExecutionResult(
            fills=fills,
            costs=ExecutionCosts(commission=total_commission, slippage_cost=total_slippage),
            rejects=rejects,
        )

    def _update_position_state(
        self,
        *,
        symbol: str,
        execution_date: date,
        bar_index: int,
        previous_shares: float,
        fill_qty: float,
        fill_price: float,
    ) -> None:
        next_shares = float(previous_shares) + float(fill_qty)
        if abs(next_shares) < _EPS:
            self._position_states.pop(symbol, None)
            return

        prev_side = _shares_sign(previous_shares)
        next_side = _shares_sign(next_shares)
        prev_state = self._position_states.get(symbol)

        if prev_state is None or prev_side == 0 or prev_side != next_side:
            self._position_states[symbol] = PositionState(
                symbol=symbol,
                shares=next_shares,
                avg_entry_price=float(fill_price),
                entry_date=execution_date,
                entry_bar_index=int(bar_index),
                last_fill_date=execution_date,
            )
            return

        avg_entry = prev_state.avg_entry_price
        entry_date = prev_state.entry_date
        entry_bar_index = prev_state.entry_bar_index

        if avg_entry is None or entry_date is None:
            self._position_states[symbol] = PositionState(
                symbol=symbol,
                shares=next_shares,
                avg_entry_price=float(fill_price),
                entry_date=execution_date,
                entry_bar_index=int(bar_index),
                last_fill_date=execution_date,
            )
            return

        if abs(next_shares) > abs(previous_shares) + _EPS:
            prev_abs = abs(float(previous_shares))
            add_abs = abs(float(fill_qty))
            next_abs = abs(float(next_shares))
            if next_abs > _EPS and add_abs > _EPS:
                avg_entry = (prev_abs * float(avg_entry) + add_abs * float(fill_price)) / next_abs

        self._position_states[symbol] = PositionState(
            symbol=symbol,
            shares=next_shares,
            avg_entry_price=float(avg_entry),
            entry_date=entry_date,
            entry_bar_index=entry_bar_index,
            last_fill_date=execution_date,
        )
