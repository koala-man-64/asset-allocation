from __future__ import annotations

from core import config as _cfg

AZURE_CONTAINER_EARNINGS = _cfg.AZURE_CONTAINER_EARNINGS


def __getattr__(name: str):
    return getattr(_cfg, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_cfg)))
