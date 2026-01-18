from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional

from asset_allocation.backtest.config import ConstraintsConfig
from asset_allocation.backtest.models import PortfolioSnapshot


@dataclass(frozen=True)
class ConstraintHit:
    as_of: str
    constraint: str
    symbol: Optional[str]
    before: Optional[float]
    after: Optional[float]
    details: Dict[str, Any]


@dataclass(frozen=True)
class ConstraintResult:
    weights: Dict[str, float]
    hits: List[ConstraintHit]

    def to_jsonable(self) -> Dict[str, Any]:
        return {
            "weights": self.weights,
            "hits": [
                {
                    "as_of": h.as_of,
                    "constraint": h.constraint,
                    "symbol": h.symbol,
                    "before": h.before,
                    "after": h.after,
                    "details": h.details,
                }
                for h in self.hits
            ],
        }


@dataclass
class Constraints:
    config: ConstraintsConfig

    def apply(
        self,
        as_of: date,
        target_weights: Dict[str, float],
        *,
        portfolio: Optional[PortfolioSnapshot] = None,
        close_prices: Optional[Dict[str, float]] = None,
    ) -> ConstraintResult:
        hits: List[ConstraintHit] = []

        def _hit(
            constraint: str,
            *,
            symbol: Optional[str] = None,
            before: Optional[float] = None,
            after: Optional[float] = None,
            **details: Any,
        ) -> None:
            hits.append(
                ConstraintHit(
                    as_of=as_of.isoformat(),
                    constraint=constraint,
                    symbol=symbol,
                    before=before,
                    after=after,
                    details=details,
                )
            )

        weights: Dict[str, float] = {}
        for symbol, weight in (target_weights or {}).items():
            w = float(weight)
            if not self.config.allow_short and w < 0:
                _hit("shorts_disallowed", symbol=str(symbol), before=w, after=0.0)
                w = 0.0
            if w == 0.0:
                continue
            cap = float(self.config.max_position_size)
            if abs(w) > cap:
                clipped = cap if w > 0 else -cap
                _hit("position_cap", symbol=str(symbol), before=w, after=clipped, cap=cap)
                w = clipped
            weights[str(symbol)] = float(w)

        if not weights:
            return ConstraintResult(weights={}, hits=hits)

        # Max gross leverage (sum of absolute weights).
        gross = sum(abs(w) for w in weights.values())
        max_lev = float(self.config.max_leverage)
        if gross > max_lev and gross > 0:
            scale = max_lev / gross
            _hit("max_leverage", before=gross, after=max_lev, scale=scale)
            weights = {s: w * scale for s, w in weights.items() if abs(w * scale) >= 1e-12}

        # Net exposure constraints:
        # - If net_exposure_min/max are set, enforce a band.
        # - Otherwise, use max_net_exposure (absolute cap) if provided.
        net_min = self.config.net_exposure_min
        net_max = self.config.net_exposure_max

        if weights and (net_min is not None or net_max is not None):
            net = sum(weights.values())
            long_sum = sum(w for w in weights.values() if w > 0)
            short_sum = sum(w for w in weights.values() if w < 0)  # negative

            if net_min is not None and net < float(net_min):
                # Increase net by reducing short exposure (safe: never increases gross).
                if short_sum < 0:
                    desired = float(net_min)
                    factor = (desired - long_sum) / short_sum  # short_sum < 0
                    factor = max(0.0, min(1.0, float(factor)))
                    new_weights = {s: (w * factor if w < 0 else w) for s, w in weights.items()}
                    new_net = sum(new_weights.values())
                    _hit("net_exposure_min", before=net, after=new_net, min_net=desired, factor=factor)
                    weights = {s: w for s, w in new_weights.items() if abs(w) >= 1e-12}
                else:
                    _hit("net_exposure_min_unachievable", before=net, after=net, min_net=float(net_min))

            if net_max is not None and weights:
                net = sum(weights.values())
                long_sum = sum(w for w in weights.values() if w > 0)
                short_sum = sum(w for w in weights.values() if w < 0)
                if net > float(net_max):
                    # Decrease net by reducing long exposure (safe: never increases gross).
                    if long_sum > 0:
                        desired = float(net_max)
                        factor = (desired - short_sum) / long_sum
                        factor = max(0.0, min(1.0, float(factor)))
                        new_weights = {s: (w * factor if w > 0 else w) for s, w in weights.items()}
                        new_net = sum(new_weights.values())
                        _hit("net_exposure_max", before=net, after=new_net, max_net=desired, factor=factor)
                        weights = {s: w for s, w in new_weights.items() if abs(w) >= 1e-12}
                    else:
                        _hit("net_exposure_max_unachievable", before=net, after=net, max_net=float(net_max))

        # Optional net exposure cap (absolute), preserved for backwards compatibility.
        if net_min is None and net_max is None and self.config.max_net_exposure is not None and weights:
            net = sum(weights.values())
            cap = float(self.config.max_net_exposure)
            if abs(net) > cap and abs(net) > 0:
                scale = cap / abs(net)
                _hit("max_net_exposure", before=net, after=cap if net > 0 else -cap, scale=scale)
                weights = {s: w * scale for s, w in weights.items() if abs(w * scale) >= 1e-12}

        # Optional turnover cap (approximate, using close prices and equity at close).
        if (self.config.min_weight_change is not None or self.config.max_turnover is not None) and weights and portfolio and close_prices:
            equity = float(portfolio.equity)
            if equity > 0:
                current_weights: Dict[str, float] = {}
                for sym, shares in (portfolio.positions or {}).items():
                    px = close_prices.get(sym)
                    if px is None:
                        continue
                    current_weights[str(sym)] = (float(shares) * float(px)) / equity

                # Optional drift threshold: suppress tiny per-symbol weight changes.
                if self.config.min_weight_change is not None and weights:
                    threshold = float(self.config.min_weight_change)
                    if threshold > 0:
                        all_symbols = set(current_weights.keys()) | set(weights.keys())
                        clipped = 0
                        adjusted: Dict[str, float] = {}
                        for sym in all_symbols:
                            cur = float(current_weights.get(sym, 0.0))
                            tgt = float(weights.get(sym, 0.0))
                            if abs(tgt - cur) < threshold:
                                if abs(tgt - cur) >= 1e-12:
                                    clipped += 1
                                new = cur
                            else:
                                new = tgt
                            if abs(new) >= 1e-12:
                                adjusted[sym] = float(new)
                        if clipped:
                            _hit("min_weight_change", threshold=threshold, clipped=clipped)
                        weights = adjusted

                if self.config.max_turnover is not None and weights:
                    all_symbols = set(current_weights.keys()) | set(weights.keys())
                    turnover = 0.0
                    deltas: Dict[str, float] = {}
                    for sym in all_symbols:
                        cur = float(current_weights.get(sym, 0.0))
                        tgt = float(weights.get(sym, 0.0))
                        delta = tgt - cur
                        deltas[sym] = delta
                        turnover += abs(delta)

                    cap = float(self.config.max_turnover)
                    if turnover > cap and turnover > 0:
                        factor = cap / turnover
                        _hit("max_turnover", before=turnover, after=cap, factor=factor)
                        constrained: Dict[str, float] = {}
                        for sym, delta in deltas.items():
                            cur = float(current_weights.get(sym, 0.0))
                            new = cur + factor * float(delta)
                            if abs(new) >= 1e-12:
                                constrained[sym] = float(new)
                        weights = constrained

        return ConstraintResult(weights=weights, hits=hits)


def serialize_constraint_hits(hits: List[ConstraintHit]) -> str:
    return json.dumps(
        [
            {
                "as_of": h.as_of,
                "constraint": h.constraint,
                "symbol": h.symbol,
                "before": h.before,
                "after": h.after,
                "details": h.details,
            }
            for h in hits
        ],
        indent=2,
        sort_keys=True,
    )
