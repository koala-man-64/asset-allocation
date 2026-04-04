from __future__ import annotations

from tasks.common import market_symbols as _market_symbols


def __getattr__(name: str):
    return getattr(_market_symbols, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_market_symbols)))
