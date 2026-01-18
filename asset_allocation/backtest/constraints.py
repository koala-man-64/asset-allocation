from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict

from asset_allocation.backtest.config import ConstraintsConfig


@dataclass
class Constraints:
    config: ConstraintsConfig

    def apply(self, as_of: date, target_weights: Dict[str, float]) -> Dict[str, float]:
        weights: Dict[str, float] = {}
        for symbol, weight in target_weights.items():
            w = float(weight)
            if not self.config.allow_short and w < 0:
                w = 0.0
            if w == 0.0:
                continue
            cap = float(self.config.max_position_size)
            if abs(w) > cap:
                w = cap if w > 0 else -cap
            weights[symbol] = w

        gross = sum(abs(w) for w in weights.values())
        if gross <= 0:
            return {}

        max_lev = float(self.config.max_leverage)
        if gross > max_lev:
            scale = max_lev / gross
            weights = {s: w * scale for s, w in weights.items() if w * scale != 0.0}
        return weights

