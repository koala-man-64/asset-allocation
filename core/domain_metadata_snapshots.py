from __future__ import annotations

from tasks.common import domain_metadata_snapshots as _domain_metadata_snapshots


def __getattr__(name: str):
    return getattr(_domain_metadata_snapshots, name)


def __dir__() -> list[str]:
    return sorted(set(globals().keys()) | set(dir(_domain_metadata_snapshots)))
