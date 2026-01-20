from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal, Optional

from asset_allocation.backtest.models import PortfolioSnapshot


Weekday = Literal["mon", "tue", "wed", "thu", "fri"]
Freq = Literal["daily", "weekly", "monthly", "quarterly", "annually", "every_n_days"]


def _weekday_num(value: Weekday) -> int:
    mapping = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4}
    return mapping[str(value)]


@dataclass
class RebalanceSchedule:
    freq: Freq = "daily"
    every_n_days: Optional[int] = None
    weekday: Optional[Weekday] = None
    day_of_month: Optional[int] = None
    allow_nontrading_shift: Literal["prev", "next", "none"] = "prev"

    _last_rebalance_date: Optional[date] = None
    _last_rebalance_bar_index: Optional[int] = None

    def should_rebalance(self, as_of: date, *, portfolio: PortfolioSnapshot, commit: bool = True) -> bool:
        if self.freq == "daily":
            return True

        bar_index = int(portfolio.bar_index) if portfolio.bar_index is not None else None

        # First call always rebalances.
        if self._last_rebalance_date is None:
            if commit:
                self._last_rebalance_date = as_of
                self._last_rebalance_bar_index = bar_index
            return True

        if self.freq == "every_n_days":
            n = int(self.every_n_days or 0)
            if n <= 0:
                raise ValueError("rebalance.every_n_days must be > 0 when freq=every_n_days.")
            if bar_index is not None and self._last_rebalance_bar_index is not None:
                if (bar_index - int(self._last_rebalance_bar_index)) >= n:
                    if commit:
                        self._last_rebalance_date = as_of
                        self._last_rebalance_bar_index = bar_index
                    return True
                return False
            # Fallback to calendar days (best-effort).
            if (as_of - self._last_rebalance_date).days >= n:
                if commit:
                    self._last_rebalance_date = as_of
                    self._last_rebalance_bar_index = bar_index
                return True
            return False

        if self.freq == "weekly":
            if self.weekday:
                if as_of.weekday() == _weekday_num(self.weekday):
                    if as_of != self._last_rebalance_date:
                        if commit:
                            self._last_rebalance_date = as_of
                            self._last_rebalance_bar_index = bar_index
                        return True
                return False

            if as_of.isocalendar()[1] != self._last_rebalance_date.isocalendar()[1] or (as_of - self._last_rebalance_date).days >= 7:
                if commit:
                    self._last_rebalance_date = as_of
                    self._last_rebalance_bar_index = bar_index
                return True
            return False

        if self.freq == "monthly":
            if self.day_of_month:
                dom = int(self.day_of_month)
                if not (1 <= dom <= 31):
                    raise ValueError("rebalance.day_of_month must be in [1, 31].")
                # Best-effort: rebalance on the first trading day on/after dom.
                if as_of.month != self._last_rebalance_date.month and as_of.day >= dom:
                    if commit:
                        self._last_rebalance_date = as_of
                        self._last_rebalance_bar_index = bar_index
                    return True
                return False

            if as_of.month != self._last_rebalance_date.month or as_of.year != self._last_rebalance_date.year:
                if commit:
                    self._last_rebalance_date = as_of
                    self._last_rebalance_bar_index = bar_index
                return True
            return False

        if self.freq == "quarterly":
            curr_q = (as_of.month - 1) // 3 + 1
            last_q = (self._last_rebalance_date.month - 1) // 3 + 1 if self._last_rebalance_date else None
            if last_q is None or curr_q != last_q or as_of.year != self._last_rebalance_date.year:
                if commit:
                    self._last_rebalance_date = as_of
                    self._last_rebalance_bar_index = bar_index
                return True
            return False

        if self.freq == "annually":
            if as_of.year != self._last_rebalance_date.year:
                if commit:
                    self._last_rebalance_date = as_of
                    self._last_rebalance_bar_index = bar_index
                return True
            return False

        raise ValueError(f"Unknown rebalance.freq: {self.freq!r}")


def build_rebalance_schedule(cfg: dict) -> RebalanceSchedule:
    if not isinstance(cfg, dict):
        raise ValueError("rebalance must be an object.")
    freq = str(cfg.get("freq") or "daily").strip()
    schedule = RebalanceSchedule(
        freq=freq,  # type: ignore[arg-type]
        every_n_days=int(cfg.get("every_n_days")) if cfg.get("every_n_days") is not None else None,
        weekday=str(cfg.get("weekday")) if cfg.get("weekday") is not None else None,  # type: ignore[arg-type]
        day_of_month=int(cfg.get("day_of_month")) if cfg.get("day_of_month") is not None else None,
        allow_nontrading_shift=str(cfg.get("allow_nontrading_shift") or "prev"),  # type: ignore[arg-type]
    )
    return schedule
