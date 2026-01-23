from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Literal, Optional


NormalizeMode = Literal["gross", "net", "none"]
BlendMethod = Literal["weighted_sum"]


def _clean_weights(weights: Dict[str, float], *, eps: float = 1e-12) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for sym, w in (weights or {}).items():
        try:
            wf = float(w)
        except (TypeError, ValueError):
            continue
        if math.isnan(wf) or math.isinf(wf) or abs(wf) < eps:
            continue
        out[str(sym)] = wf
    return out


def normalize_alphas(alphas: Iterable[float], *, eps: float = 1e-12) -> List[float]:
    values = []
    for a in alphas:
        af = float(a)
        if math.isnan(af) or math.isinf(af):
            raise ValueError("blend leg weights must be finite floats.")
        values.append(af)
    total = sum(values)
    if total <= eps:
        raise ValueError("blend leg weights must sum to > 0.")
    return [v / total for v in values]


def normalize_exposure(
    weights: Dict[str, float],
    *,
    mode: NormalizeMode,
    target_gross: float = 1.0,
    target_net: float = 1.0,
    eps: float = 1e-12,
) -> Dict[str, float]:
    cleaned = _clean_weights(weights, eps=eps)
    if not cleaned or mode == "none":
        return cleaned

    if mode == "gross":
        gross = sum(abs(w) for w in cleaned.values())
        if gross <= eps:
            return {}
        tgt = float(target_gross)
        if math.isnan(tgt) or math.isinf(tgt) or tgt < 0:
            raise ValueError("target_gross must be a finite float >= 0.")
        if tgt == 0:
            return {}
        scale = tgt / gross
        return _clean_weights({s: w * scale for s, w in cleaned.items()}, eps=eps)

    if mode == "net":
        net = sum(cleaned.values())
        if abs(net) <= eps:
            return {}
        tgt = float(target_net)
        if math.isnan(tgt) or math.isinf(tgt):
            raise ValueError("target_net must be a finite float.")
        if abs(tgt) <= eps:
            return {}
        scale = tgt / net
        return _clean_weights({s: w * scale for s, w in cleaned.items()}, eps=eps)

    raise ValueError(f"Unknown normalize mode: {mode!r}")


def weighted_sum(leg_weights: List[Dict[str, float]], *, alphas: List[float], eps: float = 1e-12) -> Dict[str, float]:
    if len(leg_weights) != len(alphas):
        raise ValueError("leg_weights and alphas must have the same length.")
    symbols: set[str] = set()
    cleaned_legs: list[dict[str, float]] = []
    for w in leg_weights:
        cleaned = _clean_weights(w, eps=eps)
        cleaned_legs.append(cleaned)
        symbols |= set(cleaned.keys())

    out: Dict[str, float] = {}
    for sym in symbols:
        total = 0.0
        for a, w in zip(alphas, cleaned_legs):
            total += float(a) * float(w.get(sym, 0.0))
        if abs(total) >= eps:
            out[str(sym)] = float(total)
    return out


@dataclass(frozen=True)
class BlendConfig:
    method: BlendMethod = "weighted_sum"
    normalize_final: NormalizeMode = "none"
    target_gross: Optional[float] = None
    target_net: Optional[float] = None
    allow_overlap: bool = True

