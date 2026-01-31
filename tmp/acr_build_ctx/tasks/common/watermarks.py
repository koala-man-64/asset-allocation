from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from core import core as mdc


def _is_enabled() -> bool:
    return getattr(mdc, "common_storage_client", None) is not None


def _watermark_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/{cleaned}.json"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def build_blob_signature(blob: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        "etag": blob.get("etag"),
        "last_modified": _iso(blob.get("last_modified")),
    }


def signature_matches(prior: Dict[str, Any], current: Dict[str, Optional[str]]) -> bool:
    if not prior or not current:
        return False

    current_etag = current.get("etag")
    prior_etag = prior.get("etag")
    if current_etag and prior_etag and current_etag == prior_etag:
        return True

    current_lm = current.get("last_modified")
    prior_lm = prior.get("last_modified")
    if current_lm and prior_lm and current_lm == prior_lm:
        return True

    return False


def load_watermarks(key: str) -> Optional[Dict[str, Any]]:
    if not _is_enabled():
        mdc.write_warning("Watermarks disabled (common storage not initialized).")
        return None

    payload = mdc.get_common_json_content(_watermark_path(key)) or {}
    if isinstance(payload, dict) and isinstance(payload.get("items"), dict):
        return payload["items"]
    if isinstance(payload, dict):
        return payload
    return {}


def save_watermarks(key: str, items: Dict[str, Any]) -> None:
    if not _is_enabled():
        mdc.write_warning("Skipping watermark save (common storage not initialized).")
        return

    payload = {
        "version": 1,
        "updated_at": _iso(datetime.now(timezone.utc)),
        "items": items,
    }
    try:
        mdc.save_common_json_content(payload, _watermark_path(key))
    except Exception as exc:
        mdc.write_warning(f"Failed to save watermarks: {exc}")


def check_blob_unchanged(blob: Dict[str, Any], prior: Optional[Dict[str, Any]]) -> Tuple[bool, Dict[str, Optional[str]]]:
    signature = build_blob_signature(blob)
    if not prior:
        return False, signature
    return signature_matches(prior, signature), signature
