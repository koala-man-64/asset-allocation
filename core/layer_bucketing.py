from __future__ import annotations

from tasks.common import layer_bucketing as _layer_bucketing


def __getattr__(name: str):
    return getattr(_layer_bucketing, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_layer_bucketing)))
