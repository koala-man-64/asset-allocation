from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Literal, Optional, Protocol

import numpy as np
import pandas as pd

from asset_allocation.backtest.configured_strategy.utils import find_column, safe_float


class ScoringModel(Protocol):
    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        ...


def _fillna(series: pd.Series, how: str) -> pd.Series:
    mode = str(how or "drop")
    if mode == "drop":
        return series
    if mode == "zero":
        return series.fillna(0.0)
    if mode == "mean":
        mean = float(series.mean()) if series.notna().any() else 0.0
        return series.fillna(mean)
    raise ValueError(f"Unknown fillna mode: {how!r}")


def _normalize(series: pd.Series, method: str) -> pd.Series:
    mode = str(method or "none")
    if mode == "none":
        return series
    if mode == "zscore":
        mean = float(series.mean()) if series.notna().any() else 0.0
        std = float(series.std(ddof=0)) if series.notna().any() else 0.0
        if std <= 0:
            return series * 0.0
        return (series - mean) / std
    if mode == "rank_percentile":
        ranked = series.rank(pct=True, method="average")
        return ranked
    raise ValueError(f"Unknown normalize method: {method!r}")


class ColumnScore:
    def __init__(self, *, column: str, higher_is_better: bool = True, fillna: str = "drop") -> None:
        self._column = str(column)
        self._higher_is_better = bool(higher_is_better)
        self._fillna = str(fillna)

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        if signals_today is None or signals_today.empty:
            return pd.Series(dtype="float64")
        col = find_column(signals_today, [self._column])
        if not col:
            raise ValueError(f"signals_today missing required column: {self._column!r}")
        df = signals_today.copy()
        df["symbol"] = df["symbol"].astype(str)
        df = df[df["symbol"].isin(set(eligible_symbols))]
        df = df.drop_duplicates(subset=["symbol"], keep="last")
        series = pd.to_numeric(df[col], errors="coerce")
        series.index = df["symbol"]
        series = _fillna(series, self._fillna)
        if self._fillna == "drop":
            series = series.dropna()
        if not self._higher_is_better:
            series = -series
        return series.astype("float64")


class NegateColumnScore:
    def __init__(self, *, column: str, fillna: str = "drop") -> None:
        self._inner = ColumnScore(column=column, higher_is_better=True, fillna=fillna)

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        out = self._inner.score(as_of=as_of, prices=prices, signals_today=signals_today, eligible_symbols=eligible_symbols)
        return -out


class WeightedSumScore:
    def __init__(self, *, terms: list[dict], normalize: str = "none", fillna: str = "drop") -> None:
        self._terms = list(terms)
        self._normalize = str(normalize or "none")
        self._fillna = str(fillna or "drop")

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        if not self._terms:
            return pd.Series(dtype="float64")
        total: Optional[pd.Series] = None
        for term in self._terms:
            if not isinstance(term, dict):
                raise ValueError("scoring.terms entries must be objects.")
            col = str(term.get("column") or "")
            w = float(term.get("weight", 1.0))
            series = ColumnScore(column=col, fillna=self._fillna).score(
                as_of=as_of,
                prices=prices,
                signals_today=signals_today,
                eligible_symbols=eligible_symbols,
            )
            series = series * w
            total = series if total is None else total.add(series, fill_value=np.nan if self._fillna == "drop" else 0.0)

        if total is None:
            return pd.Series(dtype="float64")
        total = _fillna(total, self._fillna)
        if self._fillna == "drop":
            total = total.dropna()
        total = _normalize(total, self._normalize)
        return total.astype("float64")


class MaxOfScore:
    def __init__(self, *, scores: list[dict], fillna: str = "drop") -> None:
        self._specs = list(scores)
        self._fillna = str(fillna or "drop")

    def _build_child(self, spec: dict) -> ScoringModel:
        stype = str(spec.get("type") or "").strip()
        if stype == "column":
            return ColumnScore(
                column=str(spec.get("column") or ""),
                higher_is_better=bool(spec.get("higher_is_better", True)),
                fillna=str(spec.get("fillna") or self._fillna),
            )
        if stype == "negate_column":
            return NegateColumnScore(column=str(spec.get("column") or ""), fillna=str(spec.get("fillna") or self._fillna))
        raise ValueError(f"Unknown max_of sub-score type: {stype!r}")

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        if not self._specs:
            return pd.Series(dtype="float64")

        frames: list[pd.Series] = []
        for spec in self._specs:
            if not isinstance(spec, dict):
                raise ValueError("scoring.scores entries must be objects.")
            model = self._build_child(spec)
            frames.append(model.score(as_of=as_of, prices=prices, signals_today=signals_today, eligible_symbols=eligible_symbols))

        if not frames:
            return pd.Series(dtype="float64")

        aligned = pd.concat(frames, axis=1)
        # Choose the score with maximum absolute magnitude (useful for breakout vs breakdown).
        absmax_col = aligned.abs().idxmax(axis=1)
        out = pd.Series(index=aligned.index, dtype="float64")
        for sym, col in absmax_col.items():
            out.loc[sym] = float(aligned.loc[sym, col])

        out = _fillna(out, self._fillna)
        if self._fillna == "drop":
            out = out.dropna()
        return out.astype("float64")


class RankTransformScore:
    def __init__(self, *, base: dict, method: str = "percentile", ascending: bool = False) -> None:
        self._base = dict(base)
        self._method = str(method or "percentile")
        self._ascending = bool(ascending)

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        base_model = build_scoring_model(self._base)
        raw = base_model.score(as_of=as_of, prices=prices, signals_today=signals_today, eligible_symbols=eligible_symbols)
        if raw.empty:
            return raw
        if self._method == "percentile":
            return raw.rank(pct=True, ascending=self._ascending).astype("float64")
        if self._method == "dense_rank":
            return raw.rank(method="dense", ascending=self._ascending).astype("float64")
        raise ValueError(f"Unknown rank_transform method: {self._method!r}")


class PriceBreakoutScore:
    def __init__(
        self,
        *,
        lookback: int,
        mode: Literal["breakout", "breakdown"],
        price_col: str = "close",
        scale: Literal["none", "pct"] = "pct",
    ) -> None:
        self._lookback = int(lookback)
        self._mode = str(mode)
        self._price_col = str(price_col)
        self._scale = str(scale)

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        if prices is None or prices.empty:
            return pd.Series(dtype="float64")

        lookback = max(2, self._lookback)
        out: Dict[str, float] = {}

        df = prices[prices["date"] <= as_of].copy()
        df["symbol"] = df["symbol"].astype(str)
        df = df[df["symbol"].isin(set(eligible_symbols))]
        if df.empty:
            return pd.Series(dtype="float64")

        # Find price column (case-insensitive).
        col = self._price_col if self._price_col in df.columns else None
        if col is None:
            candidates = [c for c in df.columns if str(c).lower() == self._price_col.lower()]
            col = candidates[0] if candidates else None
        if col is None:
            raise ValueError(f"prices missing required column: {self._price_col!r}")

        df["px"] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["px"])
        if df.empty:
            return pd.Series(dtype="float64")

        for sym, group in df.groupby("symbol", sort=False):
            g = group.sort_values("date")
            series = g["px"]
            if len(series) < lookback + 1:
                continue
            px_today = float(series.iloc[-1])
            window = series.iloc[-(lookback + 1) : -1]
            prev_high = float(window.max())
            prev_low = float(window.min())
            if prev_high <= 0 or prev_low <= 0 or px_today <= 0:
                continue

            if self._mode == "breakout":
                strength = max(0.0, px_today / prev_high - 1.0) if self._scale == "pct" else max(0.0, px_today - prev_high)
                if strength > 0:
                    out[str(sym)] = float(strength)
            else:
                strength = max(0.0, prev_low / px_today - 1.0) if self._scale == "pct" else max(0.0, prev_low - px_today)
                if strength > 0:
                    out[str(sym)] = -float(strength)

        return pd.Series(out, dtype="float64")


class EpisodicPivotHeuristicScore:
    def __init__(self, *, inputs: dict, params: dict) -> None:
        self._gap_col = str(inputs.get("gap_col") or "gap_pct")
        self._vol_ratio_col = str(inputs.get("vol_ratio_col") or "vol_ratio")
        self._min_gap = float(params.get("min_gap", 0.0))
        self._min_vol_ratio = float(params.get("min_vol_ratio", 0.0))

    def score(
        self,
        *,
        as_of: date,
        prices: pd.DataFrame,
        signals_today: pd.DataFrame,
        eligible_symbols: list[str],
    ) -> pd.Series:
        if signals_today is None or signals_today.empty:
            return pd.Series(dtype="float64")

        df = signals_today.copy()
        df["symbol"] = df["symbol"].astype(str)
        df = df[df["symbol"].isin(set(eligible_symbols))]
        df = df.drop_duplicates(subset=["symbol"], keep="last")

        gap = find_column(df, [self._gap_col])
        vol = find_column(df, [self._vol_ratio_col])
        if not gap or not vol:
            raise ValueError("signals_today missing required EP raw fields.")

        df["gap"] = pd.to_numeric(df[gap], errors="coerce")
        df["vol_ratio"] = pd.to_numeric(df[vol], errors="coerce")
        df = df.dropna(subset=["gap", "vol_ratio"])
        if df.empty:
            return pd.Series(dtype="float64")

        df = df[(df["gap"] >= self._min_gap) & (df["vol_ratio"] >= self._min_vol_ratio)]
        if df.empty:
            return pd.Series(dtype="float64")

        score = df["gap"] + 0.5 * df["vol_ratio"]
        score.index = df["symbol"]
        return score.astype("float64")


def build_scoring_model(scoring_cfg: dict) -> ScoringModel:
    if not isinstance(scoring_cfg, dict):
        raise ValueError("scoring must be an object.")
    stype = str(scoring_cfg.get("type") or "").strip()
    if stype == "column":
        return ColumnScore(
            column=str(scoring_cfg.get("column") or ""),
            higher_is_better=bool(scoring_cfg.get("higher_is_better", True)),
            fillna=str(scoring_cfg.get("fillna") or "drop"),
        )
    if stype == "weighted_sum":
        return WeightedSumScore(
            terms=list(scoring_cfg.get("terms") or []),
            normalize=str(scoring_cfg.get("normalize") or "none"),
            fillna=str(scoring_cfg.get("fillna") or "drop"),
        )
    if stype == "max_of":
        return MaxOfScore(scores=list(scoring_cfg.get("scores") or []), fillna=str(scoring_cfg.get("fillna") or "drop"))
    if stype == "rank_transform":
        return RankTransformScore(
            base=dict(scoring_cfg.get("base") or {}),
            method=str(scoring_cfg.get("method") or "percentile"),
            ascending=bool(scoring_cfg.get("ascending", False)),
        )
    if stype == "price_breakout":
        return PriceBreakoutScore(
            lookback=int(scoring_cfg.get("lookback", 20)),
            mode=str(scoring_cfg.get("mode") or "breakout"),  # type: ignore[arg-type]
            price_col=str(scoring_cfg.get("price_col") or "close"),
            scale=str(scoring_cfg.get("scale") or "pct"),  # type: ignore[arg-type]
        )
    if stype == "episodic_pivot_heuristic":
        return EpisodicPivotHeuristicScore(
            inputs=dict(scoring_cfg.get("inputs") or {}),
            params=dict(scoring_cfg.get("params") or {}),
        )
    raise ValueError(f"Unknown scoring.type: {stype!r}")

