from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Protocol

import pandas as pd

from backtest.configured_strategy.utils import ensure_signal_columns, find_column
from backtest.configured_strategy.scoring import PriceBreakoutScore


class SignalProvider(Protocol):
    def load_today(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: Optional[pd.DataFrame],
        eligible_symbols: list[str],
    ) -> pd.DataFrame:
        ...


class EngineSignalsProvider:
    def __init__(self, *, columns: Optional[list[str]] = None) -> None:
        self._columns = list(columns) if columns else None

    def load_today(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: Optional[pd.DataFrame],
        eligible_symbols: list[str],
    ) -> pd.DataFrame:
        if signals_today is None or signals_today.empty:
            return pd.DataFrame(columns=["symbol"])

        df = signals_today.copy()
        if "symbol" not in df.columns:
            sym_col = find_column(df, ["symbol", "Symbol"])
            if not sym_col:
                raise ValueError("signals_today missing 'symbol' column.")
            df = df.rename(columns={sym_col: "symbol"})
        df["symbol"] = df["symbol"].astype(str)
        df = df[df["symbol"].isin(set(eligible_symbols))]

        if self._columns:
            keep = ["symbol"]
            for col in self._columns:
                found = find_column(df, [col])
                if not found:
                    raise ValueError(f"signals_today missing required column: {col!r}")
                keep.append(found)
            df = df[keep].copy()
        return df.drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


class ComputedFromPricesProvider:
    def __init__(self, *, columns: list[str]) -> None:
        self._columns = list(columns)

    def load_today(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: Optional[pd.DataFrame],
        eligible_symbols: list[str],
    ) -> pd.DataFrame:
        df = pd.DataFrame({"symbol": list(eligible_symbols)})
        for col in self._columns:
            if col in {"breakout_score", "breakdown_score"}:
                mode = "breakout" if col == "breakout_score" else "breakdown"
                model = PriceBreakoutScore(lookback=20, mode=mode)
                scores = model.score(as_of=as_of, prices=prices, signals_today=pd.DataFrame(), eligible_symbols=eligible_symbols)
                df[col] = df["symbol"].map(lambda s: float(scores.get(str(s), 0.0)))
            else:
                df[col] = 0.0
        return df


class HybridSignalsProvider:
    def __init__(
        self,
        *,
        columns: list[str],
        allowed_missing_columns: list[str],
        fallback_enabled: bool = True,
    ) -> None:
        self._columns = list(columns)
        self._allowed_missing = set(str(c) for c in (allowed_missing_columns or []))
        self._fallback_enabled = bool(fallback_enabled)

    def load_today(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: Optional[pd.DataFrame],
        eligible_symbols: list[str],
    ) -> pd.DataFrame:
        engine = EngineSignalsProvider(columns=None)
        base = engine.load_today(as_of=as_of, prices=prices, signals_today=signals_today, eligible_symbols=eligible_symbols)
        if base.empty:
            base = pd.DataFrame({"symbol": list(eligible_symbols)})

        base_cols = set(str(c) for c in base.columns)
        missing = [c for c in self._columns if c not in base_cols and find_column(base, [c]) is None]
        if missing and not self._fallback_enabled:
            raise ValueError(f"signals_today missing required columns: {missing}")

        for col in self._columns:
            found = find_column(base, [col])
            if found:
                if found != col:
                    base = base.rename(columns={found: col})
                continue
            if col in missing and col not in self._allowed_missing:
                raise ValueError(f"signals_today missing required column: {col!r}")
            # Fallback compute for allowed missing columns.
            if col in {"breakout_score", "breakdown_score"}:
                mode = "breakout" if col == "breakout_score" else "breakdown"
                model = PriceBreakoutScore(lookback=20, mode=mode)
                scores = model.score(as_of=as_of, prices=prices, signals_today=pd.DataFrame(), eligible_symbols=eligible_symbols)
                base[col] = base["symbol"].map(lambda s: float(scores.get(str(s), 0.0)))
            else:
                base[col] = 0.0

        keep = ["symbol"] + [str(c) for c in self._columns]
        return base[keep].drop_duplicates(subset=["symbol"], keep="last").reset_index(drop=True)


def build_signal_provider(cfg: dict) -> SignalProvider:
    if not isinstance(cfg, dict):
        raise ValueError("signals must be an object.")
    provider = str(cfg.get("provider") or "platinum_signals_daily").strip()
    columns = cfg.get("columns") or []
    if not isinstance(columns, list):
        raise ValueError("signals.columns must be a list.")

    if provider == "platinum_signals_daily":
        return EngineSignalsProvider(columns=[str(c) for c in columns] if columns else None)
    if provider == "computed_from_prices":
        return ComputedFromPricesProvider(columns=[str(c) for c in columns])
    if provider == "hybrid":
        fb = dict(cfg.get("fallback") or {})
        return HybridSignalsProvider(
            columns=[str(c) for c in columns],
            allowed_missing_columns=[str(c) for c in (fb.get("allowed_missing_columns") or [])],
            fallback_enabled=bool(fb.get("enabled", True)),
        )
    raise ValueError(f"Unknown signals.provider: {provider!r}")

