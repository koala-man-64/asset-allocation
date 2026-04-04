from __future__ import annotations

from tasks.common import domain_artifacts as _domain_artifacts


def __getattr__(name: str):
    return getattr(_domain_artifacts, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_domain_artifacts)))
