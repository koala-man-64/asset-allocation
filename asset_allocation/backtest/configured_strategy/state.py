from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Optional

import pandas as pd

from asset_allocation.backtest.models import PortfolioSnapshot
from asset_allocation.backtest.configured_strategy.utils import BarView, latest_bar


def _shares_side(shares: float) -> int:
    if shares > 0:
        return 1
    if shares < 0:
        return -1
    return 0


@dataclass
class PositionState:
    symbol: str
    side: int
    entry_date: date
    entry_price: float
    high_water_mark: float
    low_water_mark: float
    last_score: Optional[float] = None
    target_scale: float = 1.0
    # Generic per-symbol flags used by components (e.g., one-time partial exit).
    flags: Dict[str, bool] = field(default_factory=dict)
    cooldown_until_bar_index: Optional[int] = None
    dropped_since_bar_index: Optional[int] = None


class PositionStateStore:
    def __init__(self) -> None:
        self._states: Dict[str, PositionState] = {}

    def get(self, symbol: str) -> Optional[PositionState]:
        return self._states.get(str(symbol))

    def upsert(self, state: PositionState) -> None:
        self._states[str(state.symbol)] = state

    def delete(self, symbol: str) -> None:
        self._states.pop(str(symbol), None)

    def items(self) -> Dict[str, PositionState]:
        return dict(self._states)

    def sync(self, as_of: date, *, prices: pd.DataFrame, portfolio: PortfolioSnapshot) -> None:
        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        held_syms = set(held.keys())

        # Cleanup closed positions.
        for sym in list(self._states.keys()):
            if sym not in held_syms:
                self._states.pop(sym, None)

        for sym, shares in held.items():
            side = _shares_side(shares)
            if side == 0:
                continue

            broker_state = portfolio.position_states.get(sym) if portfolio.position_states else None
            entry_date = broker_state.entry_date if broker_state and broker_state.entry_date else as_of
            entry_price = broker_state.avg_entry_price if broker_state and broker_state.avg_entry_price else None

            bar: BarView = latest_bar(prices, as_of=as_of, symbol=sym)
            if entry_price is None:
                entry_price = bar.open if bar.open is not None else bar.close
            if entry_price is None:
                # If we cannot establish an entry price, keep existing state if any.
                if sym in self._states:
                    continue
                # Otherwise initialize with a safe placeholder; exits will no-op.
                entry_price = 0.0

            existing = self._states.get(sym)
            should_reset = False
            if existing is None:
                should_reset = True
            elif existing.side != side:
                should_reset = True
            elif existing.entry_date != entry_date:
                should_reset = True

            if should_reset:
                hwm = float(entry_price)
                lwm = float(entry_price)
                # New position or side flip: reset per-position flags and drop tracking.
                flags: Dict[str, bool] = {}
                dropped_since: Optional[int] = None
                self._states[sym] = PositionState(
                    symbol=str(sym),
                    side=int(side),
                    entry_date=entry_date,
                    entry_price=float(entry_price),
                    high_water_mark=hwm,
                    low_water_mark=lwm,
                    last_score=existing.last_score if existing else None,
                    target_scale=1.0,
                    flags=flags,
                    cooldown_until_bar_index=existing.cooldown_until_bar_index if existing else None,
                    dropped_since_bar_index=dropped_since,
                )
            else:
                # Update intraday watermarks for existing positions.
                state = existing
                if state is None:
                    continue

                # Prefer high/low, fallback to close, then entry.
                high_px = bar.high if bar.high is not None else bar.close
                low_px = bar.low if bar.low is not None else bar.close
                if high_px is not None:
                    state.high_water_mark = max(float(state.high_water_mark), float(high_px))
                if low_px is not None:
                    state.low_water_mark = min(float(state.low_water_mark), float(low_px))
