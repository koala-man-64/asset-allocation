from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from asset_allocation.backtest.broker import SimulatedBroker
from asset_allocation.backtest.config import BacktestConfig
from asset_allocation.backtest.constraints import Constraints
from asset_allocation.backtest.models import MarketBar, MarketSnapshot, PortfolioSnapshot
from asset_allocation.backtest.portfolio import Portfolio
from asset_allocation.backtest.reporter import Reporter
from asset_allocation.backtest.sizer import Sizer
from asset_allocation.backtest.strategy import Strategy


def _normalize_symbol(value: str) -> str:
    return str(value).strip()


def _to_date_series(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.date


def _maybe_float(value: object) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    if prices is None or prices.empty:
        raise ValueError("prices DataFrame is required and cannot be empty.")
    df = prices.copy()
    rename_map = {}
    for candidate, target in [
        ("Date", "date"),
        ("date", "date"),
        ("Symbol", "symbol"),
        ("symbol", "symbol"),
        ("Open", "open"),
        ("open", "open"),
        ("Close", "close"),
        ("close", "close"),
    ]:
        if candidate in df.columns:
            rename_map[candidate] = target
    df = df.rename(columns=rename_map)
    required = {"date", "symbol", "open", "close"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"prices missing required columns: {sorted(missing)}")
    df["date"] = _to_date_series(df["date"])
    df = df.dropna(subset=["date"])
    df["symbol"] = df["symbol"].astype(str).map(_normalize_symbol)
    df["open"] = pd.to_numeric(df["open"], errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["symbol", "open", "close"])
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
    return df


def _normalize_signals(signals: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if signals is None or signals.empty:
        return None
    df = signals.copy()
    rename_map = {}
    if "Date" in df.columns:
        rename_map["Date"] = "date"
    if "Symbol" in df.columns:
        rename_map["Symbol"] = "symbol"
    df = df.rename(columns=rename_map)
    if "date" not in df.columns or "symbol" not in df.columns:
        raise ValueError("signals must include 'date' and 'symbol' columns when provided.")
    df["date"] = _to_date_series(df["date"])
    df = df.dropna(subset=["date"])
    df["symbol"] = df["symbol"].astype(str).map(_normalize_symbol)
    df = df.sort_values(["date", "symbol"]).reset_index(drop=True)
    return df


@dataclass
class BacktestEngine:
    config: BacktestConfig
    prices: pd.DataFrame
    signals: Optional[pd.DataFrame]
    strategy: Strategy
    sizer: Sizer
    constraints: Constraints
    reporter: Reporter

    def run(self, *, run_id: str) -> None:
        prices = _normalize_prices(self.prices)
        signals = _normalize_signals(self.signals)

        start = self.config.start_date
        end = self.config.end_date
        universe = set(self.config.universe.symbols)

        prices = prices[prices["symbol"].isin(universe)]
        if prices.empty:
            raise ValueError("No price rows available for configured universe.")

        prices = prices[(prices["date"] >= start) & (prices["date"] <= end)]
        if prices.empty:
            raise ValueError("No price rows available in the requested date range.")

        dates: List[date] = sorted(prices["date"].unique().tolist())
        if len(dates) < 1:
            raise ValueError("No trading dates available.")

        price_index = prices.set_index(["date", "symbol"]).sort_index()
        signal_index = None
        if signals is not None:
            signals = signals[signals["symbol"].isin(universe)]
            signals = signals[(signals["date"] >= start) & (signals["date"] <= end)]
            signal_index = signals.set_index(["date", "symbol"]).sort_index()

        portfolio = Portfolio(cash=float(self.config.initial_cash))
        broker = SimulatedBroker(config=self.config.broker, portfolio=portfolio)

        pending_targets: Dict[str, float] = {}

        prev_equity = float(self.config.initial_cash)
        running_peak = prev_equity

        for i, current_date in enumerate(dates):
            try:
                day_prices = price_index.loc[current_date]  # type: ignore[index]
            except KeyError:
                day_prices = pd.DataFrame(columns=["open", "close"])

            close_prices = pd.to_numeric(day_prices["close"], errors="coerce").dropna().to_dict()
            market = MarketSnapshot(
                as_of=current_date,
                bar_index=i,
                bars={
                    str(symbol): MarketBar(
                        open=_maybe_float(row.get("open")),
                        high=_maybe_float(row.get("high")),
                        low=_maybe_float(row.get("low")),
                        close=_maybe_float(row.get("close")),
                        volume=_maybe_float(row.get("volume")),
                    )
                    for symbol, row in day_prices.iterrows()
                },
            )

            # Execute orders at open (except for the first bar, which has no prior close).
            fills = []
            costs = None
            if i > 0 and pending_targets:
                execution = broker.execute_target_weights(market, target_weights=pending_targets)
                fills = execution.fills
                costs = execution.costs
                self.reporter.record_trades(fills)

            # Mark-to-market at close and record daily metrics.
            equity = portfolio.equity(close_prices)
            long_value, short_value = portfolio.exposure_values(close_prices)
            gross_exposure = (long_value + abs(short_value)) / equity if equity else 0.0
            net_exposure = (long_value + short_value) / equity if equity else 0.0

            daily_return = (equity / prev_equity - 1.0) if i > 0 and prev_equity else 0.0
            cumulative_return = equity / float(self.config.initial_cash) - 1.0
            running_peak = max(running_peak, equity)
            drawdown = (equity / running_peak - 1.0) if running_peak else 0.0

            day_commission = float(costs.commission) if costs else 0.0
            day_slippage = float(costs.slippage_cost) if costs else 0.0
            day_n_trades = int(len(fills)) if fills else 0
            day_turnover = 0.0
            if fills and prev_equity:
                traded_value = sum(abs(f.notional) for f in fills)
                day_turnover = traded_value / prev_equity

            self.reporter.record_day(
                current_date,
                portfolio=portfolio,
                equity=equity,
                daily_return=daily_return,
                cumulative_return=cumulative_return,
                drawdown=drawdown,
                gross_exposure=gross_exposure,
                net_exposure=net_exposure,
                turnover=day_turnover,
                commission=day_commission,
                slippage_cost=day_slippage,
                n_trades=day_n_trades,
            )
            self.reporter.record_positions_snapshot(
                current_date,
                portfolio=portfolio,
                equity=equity,
                close_prices=close_prices,
            )

            prev_equity = equity

            # Generate targets at close for next open, except on the last bar.
            if i >= len(dates) - 1:
                break

            price_slice = prices[prices["date"] <= current_date].copy()
            signal_slice = None
            if signal_index is not None:
                try:
                    signal_slice = signal_index.loc[current_date].reset_index()  # type: ignore[index]
                except KeyError:
                    signal_slice = None

            snapshot = PortfolioSnapshot(
                as_of=current_date,
                cash=portfolio.cash,
                positions=dict(portfolio.positions),
                equity=equity,
                bar_index=i,
                position_states=broker.get_position_states(),
            )
            decision = self.strategy.on_bar(
                current_date,
                prices=price_slice,
                signals=signal_slice,
                portfolio=snapshot,
            )
            # If strategy returns None, it means "No Action" (e.g. no rebalance today).
            # We skip sizing and constraint application, preserving current positions.
            if decision is None:
                pending_targets = {}  # or better: self.constraints.apply(...) on EXISTING positions?
                # Actually, if we skip, pending_targets should be cleared or set to None?
                # In current loop logic:
                # `pending_targets` is a local dict defined outside loop: `pending_targets = {}`
                # If we don't update it, it KEEPS the value from previous iteration?
                # WAIIIT.
                # line 119: pending_targets: Dict[str, float] = {}
                # line 136: if i > 0 and pending_targets: broker.execute...
                # line 208: pending_targets = ...
                
                # If we skip line 208, pending_targets retains YESTERDAY'S target?
                # No, because pending_targets is executed at i (today's open).
                # The loop structure:
                # 1. Execute `pending_targets` (calculated yesterday close) at Today Open.
                # 2. Daily Metrics based on Today Close.
                # 3. Calculate `pending_targets` (for Tomorrow Open) based on Today Close.
                
                # So if on_bar returns None (No Rebalance at Today Close),
                # we want `pending_targets` for Tomorrow Open to be EMPTY (no trade).
                pending_targets = {}
                continue

            target = self.sizer.size(
                current_date,
                decision=decision,
                prices=price_slice,
                portfolio=snapshot,
            )
            constraint_result = self.constraints.apply(
                current_date,
                target.weights,
                portfolio=snapshot,
                close_prices=close_prices,
            )
            self.reporter.record_constraint_hits(constraint_result.hits)
            pending_targets = constraint_result.weights
