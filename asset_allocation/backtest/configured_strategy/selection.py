from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Literal, Optional, Protocol

import pandas as pd


class Selector(Protocol):
    def select(self, *, scores: pd.Series) -> Dict[str, float]:
        ...


class TopNSelector:
    def __init__(
        self,
        *,
        n: int,
        side: Literal["long", "short"],
        min_score: Optional[float] = 0.0,
        higher_is_better: bool = True,
    ) -> None:
        self._n = int(n)
        self._side = str(side)
        self._min_score = float(min_score) if min_score is not None else None
        self._higher_is_better = bool(higher_is_better)

    def select(self, *, scores: pd.Series) -> Dict[str, float]:
        if scores is None or scores.empty or self._n <= 0:
            return {}

        series = pd.to_numeric(scores, errors="coerce").dropna()
        if series.empty:
            return {}

        if self._side == "long":
            if self._min_score is not None:
                series = series[series >= self._min_score]
            series = series.sort_values(ascending=not self._higher_is_better)
            series = series.head(self._n)
            return {str(sym): float(val) for sym, val in series.items()}

        # short
        if self._min_score is not None:
            series = series[series <= -abs(self._min_score)] if self._min_score >= 0 else series
        series = series.sort_values(ascending=self._higher_is_better)
        series = series.head(self._n)
        return {str(sym): -abs(float(val)) for sym, val in series.items()}


class LongShortTopNSelector:
    def __init__(
        self,
        *,
        long_n: int,
        short_n: int,
        min_abs_score: float = 0.0,
        score_abs: bool = True,
        allow_short: bool = True,
        long_if_high: bool = True,
    ) -> None:
        self._long_n = int(long_n)
        self._short_n = int(short_n)
        self._min_abs_score = float(min_abs_score)
        self._score_abs = bool(score_abs)
        self._allow_short = bool(allow_short)
        self._long_if_high = bool(long_if_high)

    def select(self, *, scores: pd.Series) -> Dict[str, float]:
        if scores is None or scores.empty:
            return {}
        series = pd.to_numeric(scores, errors="coerce").dropna()
        if series.empty:
            return {}

        if self._min_abs_score > 0:
            series = series[series.abs() >= self._min_abs_score]
        if series.empty:
            return {}

        out: Dict[str, float] = {}

        has_pos = bool((series > 0).any())
        has_neg = bool((series < 0).any())
        signed_mode = self._allow_short and has_pos and has_neg

        if signed_mode:
            longs = series[series > 0]
            shorts = series[series < 0]

            if self._long_n > 0 and not longs.empty:
                key = longs.abs() if self._score_abs else longs
                ranked = key.sort_values(ascending=False)
                for sym, _ in ranked.head(self._long_n).items():
                    out[str(sym)] = abs(float(series.loc[sym]))

            if self._short_n > 0 and not shorts.empty:
                key = shorts.abs() if self._score_abs else shorts
                ranked = key.sort_values(ascending=False if self._score_abs else True)
                for sym, _ in ranked.head(self._short_n).items():
                    out[str(sym)] = -abs(float(series.loc[sym]))
            return out

        # Unsigned mode: derive long/short sides from extremes of the same score column.
        if self._long_n > 0:
            ranked = series.sort_values(ascending=not self._long_if_high)
            for sym, val in ranked.head(self._long_n).items():
                out[str(sym)] = abs(float(val))

        if self._allow_short and self._short_n > 0:
            remaining = series.drop(labels=list(out.keys()), errors="ignore")
            ranked = remaining.sort_values(ascending=self._long_if_high)
            for sym, val in ranked.head(self._short_n).items():
                out[str(sym)] = -abs(float(val))

        return out


class ThresholdSelector:
    def __init__(self, *, long_if_gte: float, short_if_lte: float, allow_short: bool = True) -> None:
        self._long = float(long_if_gte)
        self._short = float(short_if_lte)
        self._allow_short = bool(allow_short)

    def select(self, *, scores: pd.Series) -> Dict[str, float]:
        if scores is None or scores.empty:
            return {}
        series = pd.to_numeric(scores, errors="coerce").dropna()
        out: Dict[str, float] = {}
        if series.empty:
            return out

        for sym, val in series.items():
            v = float(val)
            if v >= self._long:
                out[str(sym)] = abs(v)
            elif self._allow_short and v <= self._short:
                out[str(sym)] = -abs(v)
        return out


class QuantilesSelector:
    def __init__(self, *, long_q: float, short_q: float, allow_short: bool = True) -> None:
        self._long_q = float(long_q)
        self._short_q = float(short_q)
        self._allow_short = bool(allow_short)

    def select(self, *, scores: pd.Series) -> Dict[str, float]:
        if scores is None or scores.empty:
            return {}
        series = pd.to_numeric(scores, errors="coerce").dropna()
        if series.empty:
            return {}

        long_thr = float(series.quantile(self._long_q))
        short_thr = float(series.quantile(self._short_q))

        out: Dict[str, float] = {}
        for sym, val in series.items():
            v = float(val)
            if v >= long_thr:
                out[str(sym)] = abs(v)
            elif self._allow_short and v <= short_thr:
                out[str(sym)] = -abs(v)
        return out


def build_selector(selection_cfg: dict) -> Selector:
    if not isinstance(selection_cfg, dict):
        raise ValueError("selection must be an object.")
    stype = str(selection_cfg.get("type") or "").strip()
    if stype == "topn":
        cfg = dict(selection_cfg.get("topn") or {})
        min_score_raw = cfg.get("min_score", 0.0)
        min_score = float(min_score_raw) if min_score_raw is not None else None
        return TopNSelector(
            n=int(cfg.get("n", 0)),
            side=str(cfg.get("side") or "long"),  # type: ignore[arg-type]
            min_score=min_score,
            higher_is_better=bool(cfg.get("higher_is_better", True)),
        )
    if stype == "long_short_topn":
        cfg = dict(selection_cfg.get("long_short_topn") or {})
        return LongShortTopNSelector(
            long_n=int(cfg.get("long_n", 0)),
            short_n=int(cfg.get("short_n", 0)),
            min_abs_score=float(cfg.get("min_abs_score", 0.0)),
            score_abs=bool(cfg.get("score_abs", True)),
            allow_short=bool(cfg.get("allow_short", True)),
            long_if_high=bool(cfg.get("long_if_high", True)),
        )
    if stype == "threshold":
        cfg = dict(selection_cfg.get("threshold") or {})
        return ThresholdSelector(
            long_if_gte=float(cfg.get("long_if_gte")),
            short_if_lte=float(cfg.get("short_if_lte")),
            allow_short=bool(cfg.get("allow_short", True)),
        )
    if stype == "quantiles":
        cfg = dict(selection_cfg.get("quantiles") or {})
        return QuantilesSelector(
            long_q=float(cfg.get("long_q")),
            short_q=float(cfg.get("short_q")),
            allow_short=bool(cfg.get("allow_short", True)),
        )
    raise ValueError(f"Unknown selection.type: {stype!r}")
