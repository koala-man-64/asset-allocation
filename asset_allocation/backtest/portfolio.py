from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Tuple


@dataclass
class Portfolio:
    cash: float
    positions: Dict[str, float] = field(default_factory=dict)  # symbol -> shares

    def shares(self, symbol: str) -> float:
        return float(self.positions.get(symbol, 0.0))

    def set_shares(self, symbol: str, shares: float) -> None:
        if abs(shares) < 1e-12:
            self.positions.pop(symbol, None)
        else:
            self.positions[symbol] = float(shares)

    def symbols(self) -> Iterable[str]:
        return self.positions.keys()

    def equity(self, prices: Dict[str, float]) -> float:
        value = float(self.cash)
        for symbol, shares in self.positions.items():
            price = prices.get(symbol)
            if price is None:
                continue
            value += float(shares) * float(price)
        return float(value)

    def exposure_values(self, prices: Dict[str, float]) -> Tuple[float, float]:
        long_value = 0.0
        short_value = 0.0
        for symbol, shares in self.positions.items():
            price = prices.get(symbol)
            if price is None:
                continue
            position_value = float(shares) * float(price)
            if position_value >= 0:
                long_value += position_value
            else:
                short_value += position_value
        return long_value, short_value

