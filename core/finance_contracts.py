from __future__ import annotations

from tasks.common import finance_contracts as _finance_contracts


def __getattr__(name: str):
    return getattr(_finance_contracts, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_finance_contracts)))
