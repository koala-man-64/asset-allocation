from __future__ import annotations

import base64
import hashlib
import json
import os
from functools import lru_cache
from typing import Optional, Set
from urllib.parse import urlparse

from cryptography.fernet import Fernet, InvalidToken


DEFAULT_LINK_ALLOWLIST = {"portal.azure.com"}
DEFAULT_LINK_TTL_SECONDS = 900


class LinkTokenError(ValueError):
    pass


def _normalize_fernet_key(secret: str) -> bytes:
    text = secret.strip()
    if not text:
        raise LinkTokenError("Link token secret is empty.")

    try:
        decoded = base64.urlsafe_b64decode(text)
        if len(decoded) == 32:
            return text.encode("utf-8")
    except Exception:
        pass

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)


@lru_cache(maxsize=1)
def _get_fernet() -> Optional[Fernet]:
    secret = os.environ.get("SYSTEM_HEALTH_LINK_TOKEN_SECRET", "").strip()
    if not secret:
        return None
    key = _normalize_fernet_key(secret)
    return Fernet(key)


def _get_allowlist() -> Set[str]:
    raw = os.environ.get("SYSTEM_HEALTH_LINK_ALLOWLIST", "").strip()
    allowlist = {item.strip().lower() for item in raw.split(",") if item.strip()}
    return allowlist or set(DEFAULT_LINK_ALLOWLIST)


def _get_ttl_seconds() -> int:
    raw = os.environ.get("SYSTEM_HEALTH_LINK_TOKEN_TTL_SECONDS", "").strip()
    if not raw:
        return DEFAULT_LINK_TTL_SECONDS
    try:
        ttl = int(raw)
    except ValueError as exc:
        raise LinkTokenError(
            f"Invalid SYSTEM_HEALTH_LINK_TOKEN_TTL_SECONDS={raw!r} (expected integer seconds)."
        ) from exc
    if ttl < 1 or ttl > 24 * 3600:
        raise LinkTokenError("SYSTEM_HEALTH_LINK_TOKEN_TTL_SECONDS must be in [1, 86400].")
    return ttl


def _validate_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme.lower() != "https":
        raise LinkTokenError("Link URL must use https.")
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        raise LinkTokenError("Link URL must include a hostname.")
    allowlist = _get_allowlist()
    if hostname not in allowlist:
        raise LinkTokenError(f"Link host {hostname!r} is not allowlisted.")
    return url


def build_link_token(url: str) -> Optional[str]:
    fernet = _get_fernet()
    if fernet is None:
        return None
    _get_ttl_seconds()
    safe_url = _validate_url(url)
    payload = json.dumps({"url": safe_url}, separators=(",", ":")).encode("utf-8")
    return fernet.encrypt(payload).decode("utf-8")


def resolve_link_token(token: str) -> str:
    fernet = _get_fernet()
    if fernet is None:
        raise LinkTokenError("Link tokens are not configured.")
    ttl_seconds = _get_ttl_seconds()
    try:
        payload = fernet.decrypt(token.encode("utf-8"), ttl=ttl_seconds)
    except InvalidToken as exc:
        raise LinkTokenError("Invalid or expired link token.") from exc
    try:
        data = json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise LinkTokenError("Link token payload is invalid.") from exc
    url = str(data.get("url") or "").strip()
    if not url:
        raise LinkTokenError("Link token missing url.")
    return _validate_url(url)
