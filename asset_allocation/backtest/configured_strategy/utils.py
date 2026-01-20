from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Optional, Sequence

import pandas as pd


def safe_float(value: object) -> float | None:
    try:
        out = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def find_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    cols = list(df.columns)
    lower_map = {str(c).lower(): str(c) for c in cols}
    for candidate in candidates:
        if candidate in cols:
            return candidate
        mapped = lower_map.get(str(candidate).lower())
        if mapped:
            return mapped
    return None


@dataclass(frozen=True)
class BarView:
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None


def latest_bar(prices: pd.DataFrame, *, as_of: date, symbol: str) -> BarView:
    if prices is None or prices.empty:
        return BarView()

    df = prices[(prices["symbol"].astype(str) == str(symbol)) & (prices["date"] == as_of)]
    if df.empty:
        return BarView()

    row = df.iloc[-1]
    return BarView(
        open=safe_float(row.get(find_column(df, ["open", "Open"]) or "")),
        high=safe_float(row.get(find_column(df, ["high", "High"]) or "")),
        low=safe_float(row.get(find_column(df, ["low", "Low"]) or "")),
        close=safe_float(row.get(find_column(df, ["close", "Close"]) or "")),
        volume=safe_float(row.get(find_column(df, ["volume", "Volume"]) or "")),
    )


def ensure_price_columns(prices: pd.DataFrame, *, required: Iterable[str]) -> None:
    missing: list[str] = []
    for col in required:
        found = find_column(prices, [col])
        if not found:
            missing.append(str(col))
    if missing:
        raise ValueError(f"prices missing required columns: {missing}")


def ensure_signal_columns(signals: pd.DataFrame, *, required: Iterable[str]) -> None:
    missing: list[str] = []
    for col in required:
        found = find_column(signals, [col])
        if not found:
            missing.append(str(col))
    if missing:
        raise ValueError(f"signals missing required columns: {missing}")

