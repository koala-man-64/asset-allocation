from __future__ import annotations

from tasks.common import bronze_bucketing as _bronze_bucketing


def __getattr__(name: str):
    return getattr(_bronze_bucketing, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_bronze_bucketing)))
