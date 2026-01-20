from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from typing import Dict, List, Optional

import pandas as pd

from asset_allocation.backtest.models import MarketSnapshot, PortfolioSnapshot


@dataclass(frozen=True)
class StrategyDecision:
    scores: Dict[str, float]
    # Optional per-symbol scaling applied by sizers (e.g., partial exits).
    # 1.0 = unchanged, 0.5 = half-sized relative to peers, 0.0 = treated as removed.
    scales: Dict[str, float] = field(default_factory=dict)


class Strategy(ABC):
    def __init__(
        self,
        *,
        rebalance: str | int = "daily",
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        time_stop_days: Optional[int] = None,
        trailing_ma_days: Optional[int] = None,
        use_low_for_stop: bool = True,
    ):
        self._rebalance = rebalance
        self._last_rebalance_date: Optional[date] = None

        # Standard Risk Parameters
        self._stop_loss_pct = float(stop_loss_pct) if stop_loss_pct is not None else None
        self._take_profit_pct = float(take_profit_pct) if take_profit_pct is not None else None
        self._trailing_stop_pct = float(trailing_stop_pct) if trailing_stop_pct is not None else None
        self._time_stop_days = int(time_stop_days) if time_stop_days is not None else None
        self._trailing_ma_days = int(trailing_ma_days) if trailing_ma_days is not None else None
        self._use_low_for_stop = bool(use_low_for_stop)

        # State Tracking
        self._entry_price: Dict[str, float] = {}
        self._entry_date: Dict[str, date] = {}
        self._entry_side: Dict[str, int] = {}
        self._high_water_marks: Dict[str, float] = {}
        self._low_water_marks: Dict[str, float] = {}

    def on_execution(self, *, market: MarketSnapshot) -> None:
        """
        Optional hook called by the engine immediately after executing targets at open(T).

        Most strategies are fully driven by close(T) decisions and broker-provided position state,
        so the default is a no-op. Composite strategies can override to keep sleeve state aligned.
        """
        return None

    def check_rebalance(self, current_date: date) -> bool:
        """
        Determines if rebalancing should occur on the current date.
        Updates _last_rebalance_date if True.
        """
        if self._rebalance == "daily":
            return True

        # First run always rebalances
        if self._last_rebalance_date is None:
            self._last_rebalance_date = current_date
            return True

        should_run = False
        if isinstance(self._rebalance, int):
            days_diff = (current_date - self._last_rebalance_date).days
            should_run = days_diff >= self._rebalance
        elif self._rebalance == "weekly":
            # Rebalance if week changed or > 7 days gap
            # isocalendar()[1] is week number
            should_run = (
                current_date.isocalendar()[1] != self._last_rebalance_date.isocalendar()[1]
                or (current_date - self._last_rebalance_date).days >= 7
            )
        elif self._rebalance == "monthly":
            should_run = current_date.month != self._last_rebalance_date.month
        elif self._rebalance == "quarterly":
            # Change in quarter ((month-1)//3 + 1)
            curr_q = (current_date.month - 1) // 3 + 1
            last_q = (self._last_rebalance_date.month - 1) // 3 + 1
            should_run = curr_q != last_q or current_date.year != self._last_rebalance_date.year
        elif self._rebalance == "annually":
            should_run = current_date.year != self._last_rebalance_date.year
        else:
            # Fallback for unknown strings -> treat as daily usually, or error?
            # For now, safe default is True to avoid locking up account
            return True

        if should_run:
            self._last_rebalance_date = current_date
            return True
        return False

    def _sync_risk_state(self, as_of: date, *, prices: pd.DataFrame, portfolio: PortfolioSnapshot) -> None:
        """
        Syncs internal risk state (entry prices, dates, HWM) with the current portfolio.
        Must be called at the start of on_bar().
        """
        held_symbols = set(portfolio.positions.keys()) if portfolio.positions else set()
        # Cleanup closed positions
        for sym in list(self._entry_date.keys()):
            if sym not in held_symbols or abs(float(portfolio.positions.get(sym, 0.0))) < 1e-12:
                self._entry_date.pop(sym, None)
                self._entry_price.pop(sym, None)
                self._entry_side.pop(sym, None)
                self._high_water_marks.pop(sym, None)
                self._low_water_marks.pop(sym, None)

        # Register new entries & Update HWM/LWM
        for sym, shares in (portfolio.positions or {}).items():
            if abs(shares) < 1e-12:
                continue
            
            side = 1 if shares > 0 else -1
            current_side = self._entry_side.get(sym)

            # Check if this is a new position or side flip
            state = portfolio.position_states.get(sym) if portfolio.position_states else None
            state_entry_date = state.entry_date if state else None
            state_entry_px = state.avg_entry_price if state else None

            if current_side != side or (state_entry_date is not None and self._entry_date.get(sym) != state_entry_date):
                # Prefer broker-provided entry price (executed at open), with a price fallback.
                entry_px = state_entry_px
                if entry_px is None:
                    open_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["open", "Open"])
                    close_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["close", "Close"])
                    entry_px = open_px if open_px is not None else close_px

                if entry_px is not None:
                    self._entry_date[sym] = state_entry_date or as_of
                    self._entry_price[sym] = float(entry_px)
                    self._entry_side[sym] = side
                    self._high_water_marks[sym] = float(entry_px)
                    self._low_water_marks[sym] = float(entry_px)
            else:
                # Update High/Low Water Marks for existing positions
                high_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["high", "High"])
                low_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["low", "Low"])

                if high_px is not None:
                    curr_hwm = self._high_water_marks.get(sym, -1e9)
                    self._high_water_marks[sym] = max(curr_hwm, high_px)
                
                if low_px is not None:
                    curr_lwm = self._low_water_marks.get(sym, 1e9)
                    self._low_water_marks[sym] = min(curr_lwm, low_px)

    def _check_risk_exits(self, *, as_of: date, symbol: str, prices: pd.DataFrame) -> bool:
        """
        Checks all risk management conditions (SL, TP, Trailing, Time, MA).
        Returns True if the position should be exited.
        """
        side = self._entry_side.get(symbol)
        if side is None:
            return False

        close_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["close", "Close"])
        if close_px is None:
            return False
        
        entry_px = self._entry_price.get(symbol)
        if entry_px is None:
            return False

        # 1. Trailing Moving Average Exit (Trend Following)
        if self._trailing_ma_days is not None:
            ma = _moving_average_close(prices, symbol=symbol, window=self._trailing_ma_days)
            if ma is not None:
                if side > 0 and close_px < ma:
                    return True
                if side < 0 and close_px > ma:
                    return True

        # 2. Time Stop
        if self._time_stop_days is not None:
            entry_date = self._entry_date.get(symbol)
            if entry_date:
                held_days = _trading_days_held(prices, symbol=symbol, entry_date=entry_date, as_of=as_of)
                if held_days >= self._time_stop_days:
                    return True

        # 3. Stop Loss (Fixed %)
        if self._stop_loss_pct is not None:
            trigger_px = close_px
            if side > 0:
                if self._use_low_for_stop:
                    low_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["low", "Low"])
                    if low_px is not None: trigger_px = low_px
                if trigger_px <= entry_px * (1.0 - self._stop_loss_pct):
                    return True
            else: # Short
                high_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["high", "High"])
                if high_px is not None: trigger_px = high_px
                if trigger_px >= entry_px * (1.0 + self._stop_loss_pct):
                    return True

        # 4. Take Profit (Fixed %)
        if self._take_profit_pct is not None:
            trigger_px = close_px
            if side > 0:
                high_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["high", "High"])
                if high_px is not None: trigger_px = high_px
                if trigger_px >= entry_px * (1.0 + self._take_profit_pct):
                    return True
            else: # Short
                low_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["low", "Low"])
                if low_px is not None: trigger_px = low_px
                if trigger_px <= entry_px * (1.0 - self._take_profit_pct):
                    return True

        # 5. Trailing Stop (%)
        if self._trailing_stop_pct is not None:
            if side > 0:
                hwm = self._high_water_marks.get(symbol, entry_px)
                trigger_px = close_px
                if self._use_low_for_stop:
                     low_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["low", "Low"])
                     if low_px is not None: trigger_px = low_px
                if trigger_px <= hwm * (1.0 - self._trailing_stop_pct):
                    return True
            else: # Short
                lwm = self._low_water_marks.get(symbol, entry_px)
                trigger_px = close_px
                high_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["high", "High"])
                if high_px is not None: trigger_px = high_px
                if trigger_px >= lwm * (1.0 + self._trailing_stop_pct):
                    return True

        return False

    @abstractmethod
    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        raise NotImplementedError


class BuyAndHoldStrategy(Strategy):
    """
    Toy strategy for Phase 1: long 100% in the first universe symbol after the first bar.
    """

    def __init__(self, *, symbol: str, rebalance: str | int = "daily"):
        super().__init__(rebalance=rebalance)
        self._symbol = symbol

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None
        return StrategyDecision(scores={self._symbol: 1.0})


class TopNSignalStrategy(Strategy):
    """
    Signal-driven strategy for Phase 2.

    Expects `signals` to include a numeric column (default: 'composite_percentile') and 'symbol'.
    Selects the Top-N symbols by that column on each decision date (close), to be executed at next open.
    """

    def __init__(
        self,
        *,
        signal_column: str = "composite_percentile",
        top_n: int = 10,
        min_signal: Optional[float] = None,
        higher_is_better: bool = True,
        rebalance: str | int = "daily",
    ):
        super().__init__(rebalance=rebalance)
        self._signal_column = str(signal_column)
        self._top_n = int(top_n)
        self._min_signal = float(min_signal) if min_signal is not None else None
        self._higher_is_better = bool(higher_is_better)

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None

        if signals is None or signals.empty:
            return StrategyDecision(scores={})

        if "symbol" not in signals.columns:
            raise ValueError("signals must include 'symbol'.")
        if self._signal_column not in signals.columns:
            raise ValueError(f"signals missing required column: {self._signal_column!r}")

        df = signals[["symbol", self._signal_column]].copy()
        df["symbol"] = df["symbol"].astype(str)
        df["signal"] = pd.to_numeric(df[self._signal_column], errors="coerce")
        df = df.dropna(subset=["signal"])
        if df.empty:
            return StrategyDecision(scores={})

        df = df.drop_duplicates(subset=["symbol"], keep="last")
        if self._min_signal is not None:
            df = df[df["signal"] >= self._min_signal]
        if df.empty:
            return StrategyDecision(scores={})

        df = df.sort_values("signal", ascending=not self._higher_is_better)
        df = df.head(max(0, self._top_n))
        return StrategyDecision(scores={row["symbol"]: float(row["signal"]) for _, row in df.iterrows()})


class LongShortTopNStrategy(Strategy):
    """
    Generic Platinum-driven long/short strategy using a single score column.

    - Signals are used as-is (no recomputation).
    - Targets are generated at close(T) and executed at open(T+1) by the engine.
    - Produces signed scores:
        - long candidates => positive scores
        - short candidates => negative scores

    Selection rules (per rebalance date):
      - If long_if_high=True: longs from highest scores, shorts from lowest scores.
      - If long_if_high=False: longs from lowest scores, shorts from highest scores.

    Exits are evaluated on daily bars using prices (close/T+0):
      - optional trailing moving average
      - optional stop loss
      - optional max holding period (trading days)

    Partial exits (v1) are expressed as per-symbol scaling via StrategyDecision.scales.
    """

    def __init__(
        self,
        *,
        signal_column: str,
        k_long: int = 0,
        k_short: int = 0,
        long_if_high: bool = True,
        min_abs_score: float = 0.0,
        trailing_ma_days: Optional[int] = None,
        stop_loss_pct: Optional[float] = None,
        use_low_for_stop: bool = True,
        partial_exit_days: Optional[int] = None,
        partial_exit_fraction: float = 0.5,
        max_hold_days: Optional[int] = None,
        rebalance: str | int = "daily",
    ):
        super().__init__(rebalance=rebalance)
        self._signal_column = str(signal_column)
        self._k_long = int(k_long)
        self._k_short = int(k_short)
        self._long_if_high = bool(long_if_high)
        self._min_abs_score = float(min_abs_score)
        self._trailing_ma_days = int(trailing_ma_days) if trailing_ma_days is not None else None
        self._stop_loss_pct = float(stop_loss_pct) if stop_loss_pct is not None else None
        self._use_low_for_stop = bool(use_low_for_stop)
        self._partial_exit_days = int(partial_exit_days) if partial_exit_days is not None else None
        self._partial_exit_fraction = float(partial_exit_fraction)
        self._max_hold_days = int(max_hold_days) if max_hold_days is not None else None

        self._entry_date: Dict[str, date] = {}
        self._entry_price: Dict[str, float] = {}
        self._entry_side: Dict[str, int] = {}
        self._scales: Dict[str, float] = {}
        self._last_score: Dict[str, float] = {}

    def _sync_positions(self, as_of: date, *, prices: pd.DataFrame, portfolio: PortfolioSnapshot) -> None:
        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}

        for sym in list(self._entry_date.keys()):
            if sym not in held:
                self._entry_date.pop(sym, None)
                self._entry_price.pop(sym, None)
                self._entry_side.pop(sym, None)
                self._scales.pop(sym, None)
                self._last_score.pop(sym, None)

        for sym, shares in held.items():
            side = 1 if shares > 0 else -1
            state = portfolio.position_states.get(sym) if portfolio.position_states else None
            state_entry_date = state.entry_date if state else None
            state_entry_px = state.avg_entry_price if state else None

            if self._entry_side.get(sym) != side or (state_entry_date is not None and self._entry_date.get(sym) != state_entry_date):
                entry_px = state_entry_px
                if entry_px is None:
                    open_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["open", "Open"])
                    close_px = _latest_bar_value(prices, as_of=as_of, symbol=sym, columns=["close", "Close"])
                    entry_px = open_px if open_px is not None else close_px

                if entry_px is not None:
                    self._entry_date[sym] = state_entry_date or as_of
                    self._entry_price[sym] = float(entry_px)
                    self._entry_side[sym] = side
                    self._scales[sym] = 1.0

    def _should_exit(self, *, as_of: date, symbol: str, side: int, prices: pd.DataFrame) -> bool:
        close_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["close", "Close"])
        if close_px is None:
            return False

        if self._trailing_ma_days is not None and self._trailing_ma_days > 0:
            ma = _moving_average_close(prices, symbol=symbol, window=self._trailing_ma_days)
            if ma is not None:
                if side > 0 and close_px < ma:
                    return True
                if side < 0 and close_px > ma:
                    return True

        if self._max_hold_days is not None and self._max_hold_days > 0:
            entry_date = self._entry_date.get(symbol)
            if entry_date is not None:
                held_days = _trading_days_held(prices, symbol=symbol, entry_date=entry_date, as_of=as_of)
                if held_days >= self._max_hold_days:
                    return True

        if self._stop_loss_pct is None:
            return False

        entry_px = self._entry_price.get(symbol)
        if entry_px is None:
            return False

        if side > 0:
            trigger_px = close_px
            if self._use_low_for_stop:
                low_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["low", "Low"])
                if low_px is not None:
                    trigger_px = low_px
            return trigger_px <= float(entry_px) * (1.0 - float(self._stop_loss_pct))

        trigger_px = close_px
        high_px = _latest_bar_value(prices, as_of=as_of, symbol=symbol, columns=["high", "High"])
        if high_px is not None:
            trigger_px = high_px
        return trigger_px >= float(entry_px) * (1.0 + float(self._stop_loss_pct))

    @staticmethod
    def _coerce_signal_map(signals: Optional[pd.DataFrame], *, signal_column: str) -> Dict[str, float]:
        if signals is None or signals.empty:
            return {}
        if "symbol" not in signals.columns:
            raise ValueError("signals must include 'symbol'.")
        if signal_column not in signals.columns:
            raise ValueError(f"signals missing required column: {signal_column!r}")

        df = signals[["symbol", signal_column]].copy()
        df["symbol"] = df["symbol"].astype(str)
        df["signal"] = pd.to_numeric(df[signal_column], errors="coerce")
        df = df.dropna(subset=["signal"]).drop_duplicates(subset=["symbol"], keep="last")
        if df.empty:
            return {}
        return {str(row["symbol"]): float(row["signal"]) for _, row in df.iterrows()}

    @staticmethod
    def _select_topn(
        signals: pd.DataFrame,
        *,
        k_long: int,
        k_short: int,
        long_if_high: bool,
        min_abs_score: float,
    ) -> Dict[str, float]:
        if signals is None or signals.empty:
            return {}

        df = signals.copy()
        df["symbol"] = df["symbol"].astype(str)
        df["signal"] = pd.to_numeric(df["signal"], errors="coerce")
        df = df.dropna(subset=["signal"]).drop_duplicates(subset=["symbol"], keep="last")
        if df.empty:
            return {}

        if min_abs_score > 0:
            df = df[df["signal"].abs() >= float(min_abs_score)]
            if df.empty:
                return {}

        # Select longs first, then shorts from remaining universe to prevent overlap.
        out: Dict[str, float] = {}

        if k_long > 0:
            long_sorted = df.sort_values("signal", ascending=not long_if_high)
            long_df = long_sorted.head(int(k_long))
            for _, row in long_df.iterrows():
                sym = str(row["symbol"])
                value = _safe_float(row["signal"])
                if value is None:
                    continue
                out[sym] = abs(float(value))
            df = df[~df["symbol"].isin(set(long_df["symbol"].tolist()))]

        if k_short > 0 and not df.empty:
            short_sorted = df.sort_values("signal", ascending=long_if_high)
            short_df = short_sorted.head(int(k_short))
            for _, row in short_df.iterrows():
                sym = str(row["symbol"])
                value = _safe_float(row["signal"])
                if value is None:
                    continue
                out[sym] = -abs(float(value))

        return out

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        self._sync_positions(as_of, prices=prices, portfolio=portfolio)

        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        kept: Dict[str, float] = {}
        scales: Dict[str, float] = {}
        changed_non_rebalance = False

        signal_map = self._coerce_signal_map(signals, signal_column=self._signal_column)

        for sym, shares in held.items():
            side = 1 if shares > 0 else -1
            if self._should_exit(as_of=as_of, symbol=sym, side=side, prices=prices):
                changed_non_rebalance = True
                continue

            # Refresh held score from today's signal if available (keeps ranking current at rebalance).
            sig = signal_map.get(sym)
            if sig is not None:
                score = abs(float(sig)) * (1.0 if side > 0 else -1.0)
            else:
                score = self._last_score.get(sym)
                if score is None:
                    score = float(side)
                else:
                    score = float(score)
                    if side > 0 and score <= 0:
                        score = abs(score) or 1.0
                    if side < 0 and score >= 0:
                        score = -(abs(score) or 1.0)
            kept[sym] = float(score)

            scale = float(self._scales.get(sym, 1.0))
            if self._partial_exit_days is not None and scale >= 0.999:
                entry_date = self._entry_date.get(sym)
                if entry_date is not None:
                    held_days = _trading_days_held(prices, symbol=sym, entry_date=entry_date, as_of=as_of)
                    if held_days >= int(self._partial_exit_days):
                        remaining = max(0.0, 1.0 - float(self._partial_exit_fraction))
                        if abs(remaining - scale) > 1e-12:
                            scale = remaining
                            self._scales[sym] = scale
                            changed_non_rebalance = True
            if abs(scale - 1.0) > 1e-12:
                scales[sym] = scale

        is_rebalance = self.check_rebalance(as_of)
        if not is_rebalance and not changed_non_rebalance:
            return None

        candidates: Dict[str, float] = {}
        if is_rebalance and signal_map:
            df = pd.DataFrame({"symbol": list(signal_map.keys()), "signal": list(signal_map.values())})
            candidates = self._select_topn(
                df,
                k_long=max(0, self._k_long),
                k_short=max(0, self._k_short),
                long_if_high=self._long_if_high,
                min_abs_score=self._min_abs_score,
            )

        merged = dict(kept)
        merged.update(candidates)
        for sym, score in merged.items():
            self._last_score[sym] = float(score)

        return StrategyDecision(scores=merged, scales=scales)


class StaticUniverseStrategy(Strategy):
    """
    Allocates equal signals relative to all symbols in the provided list.
    Sizer will normalize weights (e.g. EqualWeightSizer will make them 1/N).
    """

    def __init__(self, *, symbols: List[str], rebalance: str | int = "daily"):
        super().__init__(rebalance=rebalance)
        self._symbols = symbols

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        if not self.check_rebalance(as_of):
            return None
        return StrategyDecision(scores={s: 1.0 for s in self._symbols})


def _safe_float(value: object) -> Optional[float]:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = list(df.columns)
    lower_map = {str(c).lower(): str(c) for c in cols}
    for candidate in candidates:
        if candidate in cols:
            return candidate
        mapped = lower_map.get(str(candidate).lower())
        if mapped:
            return mapped
    return None


def _latest_bar_value(prices: pd.DataFrame, *, as_of: date, symbol: str, columns: List[str]) -> Optional[float]:
    if prices is None or prices.empty:
        return None
    df = prices[(prices["symbol"].astype(str) == str(symbol)) & (prices["date"] == as_of)]
    if df.empty:
        return None
    col = _find_column(df, columns)
    if not col:
        return None
    value = df.iloc[-1][col]
    return _safe_float(value)


def _moving_average_close(prices: pd.DataFrame, *, symbol: str, window: int) -> Optional[float]:
    if prices is None or prices.empty:
        return None
    df = prices[prices["symbol"].astype(str) == str(symbol)]
    if df.empty:
        return None
    close_col = _find_column(df, ["close", "Close"])
    if not close_col:
        return None
    series = pd.to_numeric(df[close_col], errors="coerce").dropna()
    if len(series) < int(window) or int(window) <= 0:
        return None
    return float(series.tail(int(window)).mean())


def _trading_days_held(prices: pd.DataFrame, *, symbol: str, entry_date: date, as_of: date) -> int:
    if prices is None or prices.empty:
        return 0
    df = prices[(prices["symbol"].astype(str) == str(symbol)) & (prices["date"] >= entry_date) & (prices["date"] <= as_of)]
    if df.empty:
        return 0
    return int(df["date"].nunique())


class BreakoutStrategy(Strategy):
    """
    Breakout strategy (Platinum-first).

    - Entries: uses Platinum `breakout_score` (and optional `breakdown_score` for shorts).
    - Exits: trailing MA and optional stop-loss, evaluated on daily bars.
    - Partial exits: optional size reduction after N trading days held.
    """

    def __init__(
        self,
        *,
        breakout_score_column: str = "breakout_score",
        breakdown_score_column: Optional[str] = "breakdown_score",
        enable_shorts: bool = True,
        short_from_breakout: bool = False,
        allow_price_fallback: bool = False,
        fallback_lookback_days: int = 20,
        min_abs_score: float = 0.0,
        trailing_ma_days: int = 10,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        time_stop_days: Optional[int] = None,
        use_low_for_stop: bool = True,
        partial_exit_days: Optional[int] = 4,
        partial_exit_fraction: float = 0.5,
        rebalance: str | int = "daily",
    ):
        super().__init__(
            rebalance=rebalance,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            trailing_stop_pct=trailing_stop_pct,
            time_stop_days=time_stop_days,
            trailing_ma_days=trailing_ma_days,
            use_low_for_stop=use_low_for_stop,
        )
        self._breakout_col = str(breakout_score_column)
        self._breakdown_col = str(breakdown_score_column) if breakdown_score_column else None
        self._enable_shorts = bool(enable_shorts)
        self._short_from_breakout = bool(short_from_breakout)
        self._allow_price_fallback = bool(allow_price_fallback)
        self._fallback_lookback_days = int(fallback_lookback_days)
        self._min_abs_score = float(min_abs_score)
        self._partial_exit_days = int(partial_exit_days) if partial_exit_days is not None else None
        self._partial_exit_fraction = float(partial_exit_fraction)

        self._scales: Dict[str, float] = {}
        self._last_score: Dict[str, float] = {}

    def _fallback_scores_from_prices(self, *, prices: pd.DataFrame, as_of: date) -> Dict[str, float]:
        """
        Minimal interim fallback when Platinum breakout columns are unavailable.

        This is intentionally simple and should be replaced by Platinum-provided breakout fields.
        """
        if prices is None or prices.empty:
            return {}
        close_col = _find_column(prices, ["close", "Close"])
        if not close_col:
            return {}

        lookback = max(2, int(self._fallback_lookback_days))
        df = prices[prices["date"] <= as_of][["date", "symbol", close_col]].copy()
        df["symbol"] = df["symbol"].astype(str)
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df = df.dropna(subset=["close"])
        if df.empty:
            return {}

        out: Dict[str, float] = {}
        for sym, group in df.groupby("symbol", sort=False):
            series = group.sort_values("date")["close"]
            if len(series) < lookback + 1:
                continue
            close_today = float(series.iloc[-1])
            prev = series.iloc[-(lookback + 1) : -1]
            prev_high = float(prev.max())
            prev_low = float(prev.min())
            if prev_high <= 0 or prev_low <= 0 or close_today <= 0:
                continue

            breakout_strength = max(0.0, close_today / prev_high - 1.0)
            breakdown_strength = max(0.0, prev_low / close_today - 1.0)

            if breakout_strength > 0:
                out[str(sym)] = float(breakout_strength)
                continue
            if self._enable_shorts and breakdown_strength > 0:
                out[str(sym)] = -float(breakdown_strength)
        return out

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        self._sync_risk_state(as_of, prices=prices, portfolio=portfolio)

        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        kept: Dict[str, float] = {}
        scales: Dict[str, float] = {}
        changed_non_rebalance = False

        for sym, shares in held.items():
            # Check Risk Exits (SL, TP, TSL, etc) from Base Class
            if self._check_risk_exits(as_of=as_of, symbol=sym, prices=prices):
                changed_non_rebalance = True
                continue

            side = 1 if shares > 0 else -1
            score = self._last_score.get(sym)
            if score is None:
                score = float(side)
            else:
                score = float(score)
                if side > 0 and score <= 0:
                    score = abs(score) or 1.0
                if side < 0 and score >= 0:
                    score = -(abs(score) or 1.0)
            kept[sym] = score

            # Partial Exits logic (kept here as it scales position rather than exiting)
            scale = float(self._scales.get(sym, 1.0))
            if self._partial_exit_days is not None and scale >= 0.999:
                entry_date = self._entry_date.get(sym)
                if entry_date is not None:
                    held_days = _trading_days_held(prices, symbol=sym, entry_date=entry_date, as_of=as_of)
                    if held_days >= int(self._partial_exit_days):
                        remaining = max(0.0, 1.0 - float(self._partial_exit_fraction))
                        if abs(remaining - scale) > 1e-12:
                            scale = remaining
                            self._scales[sym] = scale
                            changed_non_rebalance = True
            if abs(scale - 1.0) > 1e-12:
                scales[sym] = scale

        is_rebalance = self.check_rebalance(as_of)
        if not is_rebalance and not changed_non_rebalance:
            return None

        candidates: Dict[str, float] = {}
        if signals is not None and not signals.empty:
            if "symbol" not in signals.columns:
                raise ValueError("signals must include 'symbol'.")
            df = signals.copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df.drop_duplicates(subset=["symbol"], keep="last")

            if self._breakout_col not in df.columns:
                if self._allow_price_fallback:
                    candidates = self._fallback_scores_from_prices(prices=prices, as_of=as_of)
                    df = pd.DataFrame({"symbol": list(candidates.keys()), "score": list(candidates.values())})
                    df["symbol"] = df["symbol"].astype(str)
                    df["breakout"] = pd.to_numeric(df["score"], errors="coerce")
                    df["breakdown"] = pd.NA
                else:
                    raise ValueError(f"signals missing required column: {self._breakout_col!r}")
            else:
                df["breakout"] = pd.to_numeric(df[self._breakout_col], errors="coerce")
                if self._breakdown_col and self._breakdown_col in df.columns:
                    df["breakdown"] = pd.to_numeric(df[self._breakdown_col], errors="coerce")
                else:
                    df["breakdown"] = pd.NA

            for _, row in df.iterrows():
                sym = str(row["symbol"])
                breakout = _safe_float(row["breakout"])
                breakdown = _safe_float(row["breakdown"])
                best: Optional[float] = None
                if breakout is not None:
                    best = breakout
                if self._enable_shorts:
                    short_score = None
                    if breakdown is not None:
                        short_score = -abs(breakdown)
                    elif self._short_from_breakout and breakout is not None:
                        short_score = -abs(breakout)
                    if short_score is not None and (best is None or abs(short_score) > abs(best)):
                        best = short_score

                if best is None:
                    continue
                if abs(float(best)) < float(self._min_abs_score):
                    continue
                candidates[sym] = float(best)

        # Merge held + candidates (candidates may update held scores on rebalance dates).
        merged = dict(kept)
        merged.update(candidates)
        for sym, score in merged.items():
            self._last_score[sym] = float(score)

        return StrategyDecision(scores=merged, scales=scales)


class EpisodicPivotStrategy(Strategy):
    """
    Episodic Pivot (EP) strategy (Platinum-first).

    Entries are driven by Platinum `ep_score` (or a deterministic combination of raw EP fields).
    Exits use a trailing moving average and optional stop-loss using daily bars.
    """

    def __init__(
        self,
        *,
        ep_score_column: str = "ep_score",
        min_ep_score: float = 0.0,
        enable_shorts: bool = False,
        trailing_ma_days: int = 20,
        stop_loss_pct: Optional[float] = None,
        take_profit_pct: Optional[float] = None,
        trailing_stop_pct: Optional[float] = None,
        time_stop_days: Optional[int] = None,
        use_low_for_stop: bool = True,
        allow_raw_fields: bool = True,
        rebalance: str | int = "daily",
    ):
        super().__init__(
            rebalance=rebalance,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            trailing_stop_pct=trailing_stop_pct,
            time_stop_days=time_stop_days,
            trailing_ma_days=trailing_ma_days,
            use_low_for_stop=use_low_for_stop,
        )
        self._ep_col = str(ep_score_column)
        self._min_ep_score = float(min_ep_score)
        self._enable_shorts = bool(enable_shorts)
        self._allow_raw_fields = bool(allow_raw_fields)

        self._last_score: Dict[str, float] = {}

    def on_bar(
        self,
        as_of: date,
        *,
        prices: pd.DataFrame,
        signals: Optional[pd.DataFrame],
        portfolio: PortfolioSnapshot,
    ) -> Optional[StrategyDecision]:
        self._sync_risk_state(as_of, prices=prices, portfolio=portfolio)

        held = {s: float(sh) for s, sh in (portfolio.positions or {}).items() if abs(float(sh)) >= 1e-12}
        kept: Dict[str, float] = {}
        changed_non_rebalance = False

        for sym, shares in held.items():
            if self._check_risk_exits(as_of=as_of, symbol=sym, prices=prices):
                changed_non_rebalance = True
                continue
            
            side = 1 if shares > 0 else -1
            score = self._last_score.get(sym)
            if score is None:
                score = float(side)
            else:
                score = float(score)
                if side > 0 and score <= 0:
                    score = abs(score) or 1.0
                if side < 0 and score >= 0:
                    score = -(abs(score) or 1.0)
            kept[sym] = score

        is_rebalance = self.check_rebalance(as_of)
        if not is_rebalance and not changed_non_rebalance:
            return None

        candidates: Dict[str, float] = {}
        if signals is not None and not signals.empty:
            if "symbol" not in signals.columns:
                raise ValueError("signals must include 'symbol'.")
            df = signals.copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df.drop_duplicates(subset=["symbol"], keep="last")

            if self._ep_col in df.columns:
                df["ep_score"] = pd.to_numeric(df[self._ep_col], errors="coerce")
            elif self._allow_raw_fields:
                gap = _find_column(df, ["gap_pct", "gap", "gapPercent", "gap_percent"])
                vol = _find_column(df, ["vol_ratio", "volume_ratio", "volRatio"])
                if not gap or not vol:
                    raise ValueError(f"signals missing required column: {self._ep_col!r}")
                df["gap_pct"] = pd.to_numeric(df[gap], errors="coerce")
                df["vol_ratio"] = pd.to_numeric(df[vol], errors="coerce")
                # Optional catalyst fields (treated as additive boosters if present).
                for src, dst in [
                    ("earnings_surprise", "earnings_surprise"),
                    ("rev_yoy", "rev_yoy"),
                    ("eps_yoy", "eps_yoy"),
                ]:
                    col = _find_column(df, [src])
                    df[dst] = pd.to_numeric(df[col], errors="coerce") if col else 0.0
                df["ep_score"] = (
                    df["gap_pct"].fillna(0.0)
                    + 0.5 * df["vol_ratio"].fillna(0.0)
                    + 0.1 * df["earnings_surprise"].fillna(0.0)
                    + 0.05 * df["rev_yoy"].fillna(0.0)
                    + 0.05 * df["eps_yoy"].fillna(0.0)
                )
            else:
                raise ValueError(f"signals missing required column: {self._ep_col!r}")

            for _, row in df.iterrows():
                sym = str(row["symbol"])
                score = _safe_float(row["ep_score"])
                if score is None:
                    continue
                if float(score) < float(self._min_ep_score) and not self._enable_shorts:
                    continue
                if self._enable_shorts and float(score) < -float(self._min_ep_score):
                    candidates[sym] = float(score)  # negative score => short
                elif float(score) >= float(self._min_ep_score):
                    candidates[sym] = float(score)  # positive => long

        merged = dict(kept)
        merged.update(candidates)
        for sym, score in merged.items():
            self._last_score[sym] = float(score)

        return StrategyDecision(scores=merged)
