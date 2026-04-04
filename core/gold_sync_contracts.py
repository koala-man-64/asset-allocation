from __future__ import annotations

from tasks.common import postgres_gold_sync as _postgres_gold_sync


def __getattr__(name: str):
    return getattr(_postgres_gold_sync, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_postgres_gold_sync)))
