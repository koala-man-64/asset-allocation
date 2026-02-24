from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from core import core as mdc


def _is_enabled() -> bool:
    return getattr(mdc, "common_storage_client", None) is not None


def _require_enabled(action: str) -> None:
    if _is_enabled():
        return
    message = f"{action} failed: common storage client is not initialized."
    mdc.write_error(message)
    raise RuntimeError(message)


def _watermark_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/{cleaned}.json"


def _run_checkpoint_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/runs/{cleaned}.json"


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _parse_iso(raw: Any) -> Optional[datetime]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def blob_last_modified_utc(blob: Dict[str, Any]) -> Optional[datetime]:
    raw = blob.get("last_modified")
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=timezone.utc)
        return raw.astimezone(timezone.utc)
    return _parse_iso(raw)


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
    if current_etag and prior_etag:
        return current_etag == prior_etag

    current_lm = current.get("last_modified")
    prior_lm = prior.get("last_modified")
    if current_lm and prior_lm:
        return current_lm == prior_lm

    return False


def load_watermarks(key: str) -> Dict[str, Any]:
    _require_enabled("Watermark load")

    payload = mdc.get_common_json_content(_watermark_path(key)) or {}
    if isinstance(payload, dict) and isinstance(payload.get("items"), dict):
        return payload["items"]
    if isinstance(payload, dict):
        return payload
    return {}


def save_watermarks(key: str, items: Dict[str, Any]) -> None:
    _require_enabled("Watermark save")

    payload = {
        "version": 1,
        "updated_at": _iso(datetime.now(timezone.utc)),
        "items": items,
    }
    try:
        mdc.save_common_json_content(payload, _watermark_path(key))
    except Exception as exc:
        message = f"Failed to save watermarks: {exc}"
        mdc.write_error(message)
        raise RuntimeError(message) from exc


def load_last_success(key: str) -> Optional[datetime]:
    _require_enabled("Run checkpoint load")

    payload = mdc.get_common_json_content(_run_checkpoint_path(key))
    if not isinstance(payload, dict):
        return None

    for candidate_key in ("last_success", "last_success_at", "updated_at"):
        parsed = _parse_iso(payload.get(candidate_key))
        if parsed is not None:
            return parsed
    return None


def save_last_success(key: str, *, when: Optional[datetime] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
    _require_enabled("Run checkpoint save")

    last_success = when or datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "version": 1,
        "last_success": _iso(last_success),
        "updated_at": _iso(datetime.now(timezone.utc)),
    }
    if metadata:
        payload["metadata"] = metadata
    try:
        mdc.save_common_json_content(payload, _run_checkpoint_path(key))
    except Exception as exc:
        message = f"Failed to save run checkpoint: {exc}"
        mdc.write_error(message)
        raise RuntimeError(message) from exc


def should_process_blob_since_last_success(
    blob: Dict[str, Any],
    *,
    prior_signature: Optional[Dict[str, Any]],
    last_success_at: Optional[datetime],
    force_reprocess: bool = False,
) -> bool:
    if force_reprocess:
        return True

    if not prior_signature:
        return True

    current_signature = build_blob_signature(blob)
    if not signature_matches(prior_signature, current_signature):
        return True

    if last_success_at is None:
        return False

    blob_last_modified = blob_last_modified_utc(blob)
    if blob_last_modified is None:
        return False

    checkpoint = (
        last_success_at.replace(tzinfo=timezone.utc)
        if last_success_at.tzinfo is None
        else last_success_at.astimezone(timezone.utc)
    )
    return blob_last_modified > checkpoint


def check_blob_unchanged(blob: Dict[str, Any], prior: Optional[Dict[str, Any]]) -> Tuple[bool, Dict[str, Optional[str]]]:
    signature = build_blob_signature(blob)
    if not prior:
        return False, signature
    return signature_matches(prior, signature), signature
