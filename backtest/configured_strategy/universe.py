from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Iterable, List, Optional

import pandas as pd

from backtest.configured_strategy.utils import ensure_price_columns, ensure_signal_columns, find_column


@dataclass(frozen=True)
class UniverseResult:
    eligible_symbols: List[str]


def _unique_symbols(df: pd.DataFrame) -> list[str]:
    if df is None or df.empty:
        return []
    if "symbol" not in df.columns:
        col = find_column(df, ["symbol", "Symbol"])
        if not col:
            return []
        series = df[col]
    else:
        series = df["symbol"]
    return sorted({str(s) for s in series.dropna().astype(str).tolist() if str(s).strip()})


def build_universe(
    *,
    as_of: date,
    prices: pd.DataFrame,
    signals_today: Optional[pd.DataFrame],
    cfg: dict,
) -> UniverseResult:
    if not isinstance(cfg, dict):
        raise ValueError("universe must be an object.")

    source = str(cfg.get("source") or "signals").strip()
    require_columns = cfg.get("require_columns") or []
    if not isinstance(require_columns, list):
        raise ValueError("universe.require_columns must be a list.")

    if require_columns:
        ensure_price_columns(prices, required=require_columns)

    if source == "signals":
        base = _unique_symbols(signals_today) if signals_today is not None else []
    elif source == "prices":
        day = prices[prices["date"] == as_of]
        base = _unique_symbols(day)
    elif source == "intersection":
        day = prices[prices["date"] == as_of]
        base_prices = set(_unique_symbols(day))
        base_signals = set(_unique_symbols(signals_today) if signals_today is not None else [])
        base = sorted(base_prices & base_signals)
    else:
        raise ValueError(f"Unknown universe.source: {source!r}")

    filters = cfg.get("filters") or []
    if not isinstance(filters, list):
        raise ValueError("universe.filters must be a list.")

    eligible = set(base)

    for flt in filters:
        if not isinstance(flt, dict):
            raise ValueError("universe.filters entries must be objects.")
        ftype = str(flt.get("type") or "").strip()

        if ftype == "whitelist":
            symbols = {str(s) for s in (flt.get("symbols") or [])}
            eligible &= symbols
            continue

        if ftype == "blacklist":
            symbols = {str(s) for s in (flt.get("symbols") or [])}
            eligible -= symbols
            continue

        if ftype == "min_price":
            col = str(flt.get("column") or "close")
            value = float(flt.get("value"))
            day = prices[prices["date"] == as_of].copy()
            day["symbol"] = day["symbol"].astype(str)
            px_col = find_column(day, [col])
            if not px_col:
                raise ValueError(f"prices missing required column for min_price: {col!r}")
            day["px"] = pd.to_numeric(day[px_col], errors="coerce")
            ok = set(day[(day["symbol"].isin(eligible)) & (day["px"] >= value)]["symbol"].astype(str).tolist())
            eligible &= ok
            continue

        if ftype in {"min_dollar_volume", "min_adv"}:
            lookback = int(flt.get("lookback", 20))
            if lookback <= 0:
                continue
            volume_col = str(flt.get("volume_col") or "volume")
            vol_col = find_column(prices, [volume_col])
            if not vol_col:
                raise ValueError(f"prices missing required volume column: {volume_col!r}")

            if ftype == "min_dollar_volume":
                price_col = str(flt.get("price_col") or "close")
                px_col = find_column(prices, [price_col])
                if not px_col:
                    raise ValueError(f"prices missing required price column: {price_col!r}")
                value = float(flt.get("value"))
                df = prices[prices["date"] <= as_of][["date", "symbol", px_col, vol_col]].copy()
                df["symbol"] = df["symbol"].astype(str)
                df = df[df["symbol"].isin(eligible)]
                df["px"] = pd.to_numeric(df[px_col], errors="coerce")
                df["vol"] = pd.to_numeric(df[vol_col], errors="coerce")
                df = df.dropna(subset=["px", "vol"])
                df["dollar_vol"] = df["px"] * df["vol"]
                ok: set[str] = set()
                for sym, group in df.groupby("symbol", sort=False):
                    tail = group.sort_values("date")["dollar_vol"].tail(lookback)
                    if len(tail) < lookback:
                        continue
                    if float(tail.mean()) >= value:
                        ok.add(str(sym))
                eligible &= ok
                continue

            # min_adv
            value = float(flt.get("value"))
            df = prices[prices["date"] <= as_of][["date", "symbol", vol_col]].copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df[df["symbol"].isin(eligible)]
            df["vol"] = pd.to_numeric(df[vol_col], errors="coerce")
            df = df.dropna(subset=["vol"])
            ok = set()
            for sym, group in df.groupby("symbol", sort=False):
                tail = group.sort_values("date")["vol"].tail(lookback)
                if len(tail) < lookback:
                    continue
                if float(tail.mean()) >= value:
                    ok.add(str(sym))
            eligible &= ok
            continue

        if ftype == "require_signal_columns":
            if signals_today is None or signals_today.empty:
                eligible = set()
                continue
            columns = flt.get("columns") or []
            if not isinstance(columns, list) or not columns:
                continue
            ensure_signal_columns(signals_today, required=columns)
            df = signals_today.copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df[df["symbol"].isin(eligible)]
            for col in columns:
                c = find_column(df, [str(col)])
                if c:
                    df = df.dropna(subset=[c])
            eligible &= set(df["symbol"].astype(str).tolist())
            continue

        if ftype == "no_missing_prices":
            window = int(flt.get("window", 10))
            if window <= 1:
                continue
            df = prices[prices["date"] <= as_of][["date", "symbol"]].copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df[df["symbol"].isin(eligible)]
            ok = set()
            for sym, group in df.groupby("symbol", sort=False):
                if int(group["date"].nunique()) >= window:
                    ok.add(str(sym))
            eligible &= ok
            continue

        if ftype == "max_volatility":
            lookback = int(flt.get("lookback", 20))
            value = float(flt.get("value"))
            returns_col = str(flt.get("returns_col") or "returns_1d")
            col = find_column(prices, [returns_col])

            df = prices[prices["date"] <= as_of].copy()
            df["symbol"] = df["symbol"].astype(str)
            df = df[df["symbol"].isin(eligible)]
            if df.empty:
                eligible = set()
                continue

            ok = set()
            if col and col in df.columns:
                df["ret"] = pd.to_numeric(df[col], errors="coerce")
                df = df.dropna(subset=["ret"])
                for sym, group in df.groupby("symbol", sort=False):
                    tail = group.sort_values("date")["ret"].tail(lookback)
                    if len(tail) < lookback:
                        continue
                    if float(tail.std(ddof=0)) <= value:
                        ok.add(str(sym))
            else:
                close_col = find_column(df, ["close", "Close"])
                if not close_col:
                    raise ValueError("prices missing close column for volatility computation.")
                df["close"] = pd.to_numeric(df[close_col], errors="coerce")
                df = df.dropna(subset=["close"])
                for sym, group in df.groupby("symbol", sort=False):
                    g = group.sort_values("date")["close"]
                    ret = g.pct_change().dropna().tail(lookback)
                    if len(ret) < lookback:
                        continue
                    if float(ret.std(ddof=0)) <= value:
                        ok.add(str(sym))
            eligible &= ok
            continue

        if ftype == "exchange_in":
            # Not currently supported in the backtest data contract; ignore safely.
            continue

        raise ValueError(f"Unknown universe filter type: {ftype!r}")

    return UniverseResult(eligible_symbols=sorted(eligible))

