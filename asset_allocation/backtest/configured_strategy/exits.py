from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Literal, Optional, Protocol, Set, Tuple

import pandas as pd

from asset_allocation.backtest.models import PortfolioSnapshot
from asset_allocation.backtest.configured_strategy.state import PositionStateStore
from asset_allocation.backtest.configured_strategy.utils import BarView, latest_bar


@dataclass(frozen=True)
class ExitAction:
    exit: bool = False
    scale: Optional[float] = None
    reason: Optional[str] = None
    rule: Optional[str] = None


class ExitRule(Protocol):
    name: str

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        ...


def _trading_days_held(prices: pd.DataFrame, *, symbol: str, entry_date: date, as_of: date) -> int:
    if prices is None or prices.empty:
        return 0
    df = prices[(prices["symbol"].astype(str) == str(symbol)) & (prices["date"] >= entry_date) & (prices["date"] <= as_of)]
    if df.empty:
        return 0
    return int(df["date"].nunique())


class TrailingMaRule:
    name = "trailing_ma"

    def __init__(self, *, days: int, price_col: str = "close", side_aware: bool = True) -> None:
        self._days = int(days)
        self._price_col = str(price_col)
        self._side_aware = bool(side_aware)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or self._days <= 0:
            return None

        df = prices[prices["symbol"].astype(str) == str(symbol)].sort_values("date")
        if df.empty:
            return None

        col = self._price_col if self._price_col in df.columns else None
        if col is None:
            candidates = [c for c in df.columns if str(c).lower() == self._price_col.lower()]
            col = candidates[0] if candidates else None
        if col is None:
            return None
        close_series = pd.to_numeric(df[col], errors="coerce")
        close_series = close_series.dropna()
        if len(close_series) < self._days:
            return None
        ma = float(close_series.tail(self._days).mean())

        bar = latest_bar(prices, as_of=as_of, symbol=symbol)
        close_px = bar.close
        if close_px is None:
            return None

        if not self._side_aware:
            if close_px < ma:
                return ExitAction(exit=True, reason=f"close_below_ma({self._days})", rule=self.name)
            return None

        if st.side > 0 and close_px < ma:
            return ExitAction(exit=True, reason=f"long_close_below_ma({self._days})", rule=self.name)
        if st.side < 0 and close_px > ma:
            return ExitAction(exit=True, reason=f"short_close_above_ma({self._days})", rule=self.name)
        return None


class StopLossRule:
    name = "stop_loss"

    def __init__(
        self,
        *,
        pct: float,
        use_intraday_extremes: bool = True,
        price_col: str = "close",
    ) -> None:
        self._pct = float(pct)
        self._use_intraday_extremes = bool(use_intraday_extremes)
        self._price_col = str(price_col)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or not (0.0 < self._pct < 1.0) or st.entry_price <= 0:
            return None

        bar = latest_bar(prices, as_of=as_of, symbol=symbol)
        close_px = bar.close
        if close_px is None:
            return None

        trigger = close_px
        if self._use_intraday_extremes:
            if st.side > 0:
                if bar.low is not None:
                    trigger = bar.low
            else:
                if bar.high is not None:
                    trigger = bar.high

        if st.side > 0 and trigger <= st.entry_price * (1.0 - self._pct):
            return ExitAction(exit=True, reason=f"long_stop_loss({self._pct})", rule=self.name)
        if st.side < 0 and trigger >= st.entry_price * (1.0 + self._pct):
            return ExitAction(exit=True, reason=f"short_stop_loss({self._pct})", rule=self.name)
        return None


class TakeProfitRule:
    name = "take_profit"

    def __init__(self, *, pct: float, price_col: str = "close") -> None:
        self._pct = float(pct)
        self._price_col = str(price_col)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or not (0.0 < self._pct < 1.0) or st.entry_price <= 0:
            return None

        bar = latest_bar(prices, as_of=as_of, symbol=symbol)
        close_px = bar.close
        if close_px is None:
            return None

        trigger = close_px
        if st.side > 0:
            if bar.high is not None:
                trigger = bar.high
            if trigger >= st.entry_price * (1.0 + self._pct):
                return ExitAction(exit=True, reason=f"long_take_profit({self._pct})", rule=self.name)
        else:
            if bar.low is not None:
                trigger = bar.low
            if trigger <= st.entry_price * (1.0 - self._pct):
                return ExitAction(exit=True, reason=f"short_take_profit({self._pct})", rule=self.name)
        return None


class TrailingStopRule:
    name = "trailing_stop"

    def __init__(self, *, pct: float, uses_hwm_lwm: bool = True) -> None:
        self._pct = float(pct)
        self._uses_hwm_lwm = bool(uses_hwm_lwm)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or not (0.0 < self._pct < 1.0) or st.entry_price <= 0:
            return None

        bar = latest_bar(prices, as_of=as_of, symbol=symbol)
        close_px = bar.close
        if close_px is None:
            return None

        if st.side > 0:
            ref = st.high_water_mark if self._uses_hwm_lwm else st.entry_price
            trigger = bar.low if bar.low is not None else close_px
            if trigger <= ref * (1.0 - self._pct):
                return ExitAction(exit=True, reason=f"long_trailing_stop({self._pct})", rule=self.name)
        else:
            ref = st.low_water_mark if self._uses_hwm_lwm else st.entry_price
            trigger = bar.high if bar.high is not None else close_px
            if trigger >= ref * (1.0 + self._pct):
                return ExitAction(exit=True, reason=f"short_trailing_stop({self._pct})", rule=self.name)
        return None


class TimeStopRule:
    name = "time_stop"

    def __init__(self, *, days: int) -> None:
        self._days = int(days)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or self._days <= 0:
            return None

        held = _trading_days_held(prices, symbol=symbol, entry_date=st.entry_date, as_of=as_of)
        if held >= self._days:
            return ExitAction(exit=True, reason=f"time_stop({self._days})", rule=self.name)
        return None


class PartialExitAfterDaysRule:
    name = "partial_exit_after_days"

    def __init__(self, *, days: int, fraction: float, once: bool = True) -> None:
        self._days = int(days)
        self._fraction = float(fraction)
        self._once = bool(once)

    def evaluate(
        self,
        *,
        symbol: str,
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> Optional[ExitAction]:
        st = state_store.get(symbol)
        if st is None or self._days <= 0:
            return None

        if not (0.0 <= self._fraction <= 1.0):
            return None

        flag_key = f"{self.name}:{self._days}:{self._fraction}"
        if self._once and st.flags.get(flag_key):
            return None

        held = _trading_days_held(prices, symbol=symbol, entry_date=st.entry_date, as_of=as_of)
        if held < self._days:
            return None

        remaining = max(0.0, 1.0 - self._fraction)
        # Mark as applied; state store is mutable (shared).
        st.flags[flag_key] = True
        return ExitAction(scale=float(remaining), reason=f"partial_exit_after_days({self._days},{self._fraction})", rule=self.name)


@dataclass(frozen=True)
class ExitEngineResult:
    exit_symbols: Set[str]
    scale_updates: Dict[str, float]
    reasons: Dict[str, List[str]]


class ExitEngine:
    def __init__(
        self,
        *,
        precedence: Literal["exit_over_scale", "scale_over_exit", "first_match"] = "exit_over_scale",
        rules: Iterable[ExitRule],
    ) -> None:
        self._precedence = str(precedence)
        self._rules = list(rules)

    def evaluate(
        self,
        *,
        symbols: Iterable[str],
        as_of: date,
        prices: pd.DataFrame,
        portfolio: PortfolioSnapshot,
        state_store: PositionStateStore,
    ) -> ExitEngineResult:
        exit_symbols: Set[str] = set()
        scales: Dict[str, float] = {}
        reasons: Dict[str, List[str]] = {}

        for symbol in symbols:
            sym = str(symbol)
            sym_reasons: List[str] = []
            seen_exit = False
            seen_scale: Optional[float] = None

            for rule in self._rules:
                action = rule.evaluate(symbol=sym, as_of=as_of, prices=prices, portfolio=portfolio, state_store=state_store)
                if action is None:
                    continue
                if action.reason:
                    sym_reasons.append(action.reason)

                if action.exit:
                    seen_exit = True
                if action.scale is not None:
                    seen_scale = float(action.scale)

                if self._precedence == "first_match":
                    break

            if seen_exit and seen_scale is not None:
                if self._precedence == "scale_over_exit":
                    seen_exit = False
                else:
                    seen_scale = None

            if seen_exit:
                exit_symbols.add(sym)
            if seen_scale is not None:
                st = state_store.get(sym)
                if st is not None:
                    st.target_scale = min(float(st.target_scale), float(seen_scale))
                    scales[sym] = float(st.target_scale)
                else:
                    scales[sym] = float(seen_scale)
            if sym_reasons:
                reasons[sym] = sym_reasons

        return ExitEngineResult(exit_symbols=exit_symbols, scale_updates=scales, reasons=reasons)


def build_exit_engine(exits_cfg: dict) -> ExitEngine:
    precedence = str(exits_cfg.get("precedence") or "exit_over_scale")
    rules_cfg = exits_cfg.get("rules") or []
    if not isinstance(rules_cfg, list):
        raise ValueError("exits.rules must be a list.")

    rules: List[ExitRule] = []
    for raw in rules_cfg:
        if not isinstance(raw, dict):
            raise ValueError("exits.rules entries must be objects.")
        rule_type = str(raw.get("type") or "").strip()
        if rule_type == "trailing_ma":
            rules.append(
                TrailingMaRule(
                    days=int(raw.get("days", 0)),
                    price_col=str(raw.get("price_col") or "close"),
                    side_aware=bool(raw.get("side_aware", True)),
                )
            )
        elif rule_type == "stop_loss":
            rules.append(
                StopLossRule(
                    pct=float(raw.get("pct")),
                    use_intraday_extremes=bool(raw.get("use_intraday_extremes", True)),
                    price_col=str(raw.get("price_col") or "close"),
                )
            )
        elif rule_type == "take_profit":
            rules.append(TakeProfitRule(pct=float(raw.get("pct")), price_col=str(raw.get("price_col") or "close")))
        elif rule_type == "trailing_stop":
            rules.append(
                TrailingStopRule(
                    pct=float(raw.get("pct")),
                    uses_hwm_lwm=bool(raw.get("uses_hwm_lwm", True)),
                )
            )
        elif rule_type == "time_stop":
            rules.append(TimeStopRule(days=int(raw.get("days", 0))))
        elif rule_type == "partial_exit_after_days":
            rules.append(
                PartialExitAfterDaysRule(
                    days=int(raw.get("days", 0)),
                    fraction=float(raw.get("fraction", 0.0)),
                    once=bool(raw.get("once", True)),
                )
            )
        else:
            raise ValueError(f"Unknown exit rule type: {rule_type!r}")

    return ExitEngine(precedence=precedence, rules=rules)
