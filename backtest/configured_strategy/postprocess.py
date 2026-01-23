from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional, Tuple

import numpy as np
import pandas as pd

from backtest.configured_strategy.state import PositionStateStore


@dataclass(frozen=True)
class PostprocessResult:
    scores: Dict[str, float]
    scales: Dict[str, float]


def _clamp(scores: Dict[str, float], *, min_v: float, max_v: float) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for sym, val in (scores or {}).items():
        v = float(val)
        out[str(sym)] = max(float(min_v), min(float(max_v), v))
    return out


def _normalize(scores: Dict[str, float], *, method: str) -> Dict[str, float]:
    if not scores:
        return {}
    series = pd.Series(scores, dtype="float64").dropna()
    if series.empty:
        return {}

    mode = str(method)
    if mode == "zscore":
        mean = float(series.mean())
        std = float(series.std(ddof=0))
        if std <= 0:
            return {str(sym): 0.0 for sym in series.index.tolist()}
        z = (series - mean) / std
        return {str(sym): float(v) for sym, v in z.items()}

    if mode == "minmax":
        lo = float(series.min())
        hi = float(series.max())
        if hi <= lo:
            return {str(sym): 0.0 for sym in series.index.tolist()}
        mm = (series - lo) / (hi - lo)
        return {str(sym): float(v) for sym, v in mm.items()}

    if mode == "rank_percentile":
        pct = series.rank(pct=True, method="average")
        # Center to [-1, 1] so median ~= 0.
        centered = pct * 2.0 - 1.0
        return {str(sym): float(v) for sym, v in centered.items()}

    raise ValueError(f"Unknown normalize method: {method!r}")


def _sticky_scores(
    scores: Dict[str, float],
    *,
    alpha: float,
    state_store: PositionStateStore,
) -> Dict[str, float]:
    a = float(alpha)
    if not (0.0 <= a <= 1.0):
        raise ValueError("sticky_scores.alpha must be in [0, 1].")
    out: Dict[str, float] = {}
    for sym, val in (scores or {}).items():
        st = state_store.get(sym)
        prev = float(st.last_score) if st and st.last_score is not None else None
        if prev is None:
            out[str(sym)] = float(val)
        else:
            out[str(sym)] = a * prev + (1.0 - a) * float(val)
    return out


def apply_postprocess(
    *,
    steps_cfg: list,
    scores: Dict[str, float],
    scales: Dict[str, float],
    state_store: PositionStateStore,
) -> PostprocessResult:
    out_scores = dict(scores or {})
    out_scales = dict(scales or {})

    for step in steps_cfg or []:
        if not isinstance(step, dict):
            raise ValueError("postprocess.steps entries must be objects.")
        stype = str(step.get("type") or "").strip()
        if stype == "clamp":
            out_scores = _clamp(out_scores, min_v=float(step.get("min", -np.inf)), max_v=float(step.get("max", np.inf)))
            continue
        if stype == "normalize":
            out_scores = _normalize(out_scores, method=str(step.get("method") or "zscore"))
            continue
        if stype == "sticky_scores":
            enabled = bool(step.get("enabled", True))
            if enabled:
                out_scores = _sticky_scores(out_scores, alpha=float(step.get("alpha", 0.7)), state_store=state_store)
            continue
        if stype == "enforce_min_candidates":
            # Handled in orchestrator where it can decide skip/error semantics.
            continue
        raise ValueError(f"Unknown postprocess step type: {stype!r}")

    return PostprocessResult(scores=out_scores, scales=out_scales)

