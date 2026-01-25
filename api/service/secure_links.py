from __future__ import annotations

import base64
import hashlib
import os
from typing import Optional
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken


class LinkTokenError(RuntimeError):
    pass


def _require_https_url(url: str) -> str:
    text = str(url or "").strip()
    if not text:
        raise LinkTokenError("URL is required.")
    parsed = urlparse(text)
    if parsed.scheme.lower() != "https":
        raise LinkTokenError("Only https:// URLs are supported.")
    if not parsed.hostname:
        raise LinkTokenError("URL hostname is required.")
    return text


def _allowed_hosts() -> list[str]:
    raw = os.environ.get("SYSTEM_HEALTH_LINK_ALLOWED_HOSTS", "")
    return [part.strip().lower() for part in raw.split(",") if part.strip()]


def _host_allowed(host: str) -> bool:
    allowed = _allowed_hosts()
    if not allowed:
        return True
    candidate = (host or "").strip().lower()
    if not candidate:
        return False
    for entry in allowed:
        if entry.startswith("*.") and candidate.endswith(entry[1:]):
            return True
        if candidate == entry:
            return True
    return False


def _ttl_seconds() -> Optional[int]:
    raw = os.environ.get("SYSTEM_HEALTH_LINK_TOKEN_TTL_SECONDS", "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise LinkTokenError(f"Invalid SYSTEM_HEALTH_LINK_TOKEN_TTL_SECONDS={raw!r}") from exc
    if value <= 0:
        return None
    return value


def _fernet() -> Optional[Fernet]:
    secret = os.environ.get("SYSTEM_HEALTH_LINK_TOKEN_SECRET", "").strip()
    if not secret:
        return None
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)


def build_link_token(url: str) -> Optional[str]:
    """
    Returns a stable link token for resolving by the API, or None when disabled.

    Disabled when SYSTEM_HEALTH_LINK_TOKEN_SECRET is unset.
    """
    f = _fernet()
    if f is None:
        return None
    normalized = _require_https_url(url)
    host = urlparse(normalized).hostname or ""
    if not _host_allowed(host):
        raise LinkTokenError(f"Host not allowed: {host}")
    return f.encrypt(normalized.encode("utf-8")).decode("utf-8")


def resolve_link_token(token: str) -> str:
    f = _fernet()
    if f is None:
        raise LinkTokenError("Link tokens are disabled.")
    raw = str(token or "").strip()
    if not raw:
        raise LinkTokenError("Token is required.")
    ttl = _ttl_seconds()
    try:
        decrypted = f.decrypt(raw.encode("utf-8"), ttl=ttl) if ttl is not None else f.decrypt(raw.encode("utf-8"))
    except InvalidToken as exc:
        raise LinkTokenError("Invalid or expired link token.") from exc
    url = decrypted.decode("utf-8", errors="replace")
    normalized = _require_https_url(url)
    host = urlparse(normalized).hostname or ""
    if not _host_allowed(host):
        raise LinkTokenError(f"Host not allowed: {host}")
    return normalized

