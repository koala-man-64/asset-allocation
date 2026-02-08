"""Configuration for Massive REST/WebSocket/Flat-File access."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


def _strip_or_none(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_float(name: str, default: float) -> float:
    raw = _strip_or_none(os.environ.get(name))
    if raw is None:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = (_strip_or_none(os.environ.get(name)) or "").lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "t", "yes", "y", "on"}


@dataclass(frozen=True)
class MassiveConfig:
    """Runtime configuration for Massive integration.

    Notes
    -----
    * **REST base URL** defaults to ``https://api.massive.com`` (the current SDK
      default). The previous base (``https://api.polygon.io``) is still supported
      by Massive, but should be treated as legacy.
    * We do not include the official Massive SDK as a hard dependency in this
      repo artifact, because this environment can't resolve new packages.
      The module will use the SDK if present; otherwise it falls back to a small
      direct-HTTP client for the endpoints we need.
    """

    api_key: str
    base_url: str = "https://api.massive.com"
    timeout_seconds: float = 30.0

    # Prefer the official SDK when it is installed.
    prefer_official_sdk: bool = True

    # WebSocket
    websocket_subscriptions_default: tuple[str, ...] = ()

    # Flat files (S3-compatible)
    flatfiles_endpoint_url: str = "https://files.massive.com"
    flatfiles_bucket: str = "flatfiles"

    @staticmethod
    def from_env(*, require_api_key: bool = True) -> "MassiveConfig":
        api_key = _strip_or_none(os.environ.get("MASSIVE_API_KEY"))
        if require_api_key and not api_key:
            raise ValueError("MASSIVE_API_KEY is required.")

        base_url = _strip_or_none(os.environ.get("MASSIVE_BASE_URL")) or "https://api.massive.com"
        timeout_seconds = _env_float("MASSIVE_TIMEOUT_SECONDS", 30.0)
        prefer_sdk = _env_bool("MASSIVE_PREFER_OFFICIAL_SDK", True)

        flat_endpoint = _strip_or_none(os.environ.get("MASSIVE_FLATFILES_ENDPOINT_URL")) or "https://files.massive.com"
        flat_bucket = _strip_or_none(os.environ.get("MASSIVE_FLATFILES_BUCKET")) or "flatfiles"

        subs_raw = _strip_or_none(os.environ.get("MASSIVE_WS_SUBSCRIPTIONS"))
        subs: tuple[str, ...] = ()
        if subs_raw:
            subs = tuple(s.strip() for s in subs_raw.split(",") if s.strip())

        # When require_api_key=False, api_key may be None. Keep typing strict by
        # normalizing to empty string (it will fail later if used).
        return MassiveConfig(
            api_key=str(api_key or ""),
            base_url=str(base_url),
            timeout_seconds=float(timeout_seconds),
            prefer_official_sdk=bool(prefer_sdk),
            websocket_subscriptions_default=subs,
            flatfiles_endpoint_url=str(flat_endpoint),
            flatfiles_bucket=str(flat_bucket),
        )
