from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from core import core as mdc


_MANIFEST_VERSION = 1
_ROOT_PREFIX = "system/run-manifests"
_BRONZE_FINANCE_PREFIX = f"{_ROOT_PREFIX}/bronze_finance"
_SILVER_FINANCE_PREFIX = f"{_ROOT_PREFIX}/silver_finance"


def _is_truthy(raw: Optional[str], *, default: bool) -> bool:
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def manifests_enabled() -> bool:
    return _is_truthy(os.environ.get("FINANCE_RUN_MANIFESTS_ENABLED"), default=True)


def silver_manifest_consumption_enabled() -> bool:
    return _is_truthy(os.environ.get("SILVER_FINANCE_USE_BRONZE_MANIFEST"), default=True)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


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


def _run_id(prefix: str) -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    token = uuid.uuid4().hex[:8]
    return f"{prefix}-{now}-{token}"


def _normalize_blob_entry(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    name = str(raw.get("name", "")).strip()
    if not name:
        return None

    entry: Dict[str, Any] = {"name": name}
    etag = raw.get("etag")
    if etag is not None:
        entry["etag"] = str(etag)

    lm = _parse_iso(raw.get("last_modified"))
    if lm is not None:
        entry["last_modified"] = _iso(lm)

    size = raw.get("size")
    if isinstance(size, int):
        entry["size"] = int(size)

    return entry


def _require_common_storage(action: str) -> bool:
    if getattr(mdc, "common_storage_client", None) is None:
        mdc.write_warning(f"Skipping {action}: common storage client is not initialized.")
        return False
    return True


def create_bronze_finance_manifest(
    *,
    producer_job_name: str,
    listed_blobs: List[Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not manifests_enabled():
        return None
    if not _require_common_storage("bronze finance manifest write"):
        return None

    normalized: List[Dict[str, Any]] = []
    for item in listed_blobs:
        if not isinstance(item, dict):
            continue
        parsed = _normalize_blob_entry(item)
        if parsed is None:
            continue
        normalized.append(parsed)
    normalized.sort(key=lambda item: item["name"])

    run_id = _run_id("bronze-finance")
    produced_at = datetime.now(timezone.utc)
    manifest = {
        "version": _MANIFEST_VERSION,
        "manifestType": "bronze-finance",
        "runId": run_id,
        "producerJobName": str(producer_job_name or "").strip(),
        "producedAt": _iso(produced_at),
        "blobPrefix": "finance-data/",
        "blobCount": len(normalized),
        "blobs": normalized,
        "metadata": dict(metadata or {}),
    }
    manifest_path = f"{_BRONZE_FINANCE_PREFIX}/{run_id}.json"
    latest_path = f"{_BRONZE_FINANCE_PREFIX}/latest.json"
    latest_payload = {
        "version": _MANIFEST_VERSION,
        "runId": run_id,
        "manifestPath": manifest_path,
        "updatedAt": _iso(produced_at),
        "blobCount": len(normalized),
    }
    try:
        mdc.save_common_json_content(manifest, manifest_path)
        mdc.save_common_json_content(latest_payload, latest_path)
        return {"runId": run_id, "manifestPath": manifest_path, "blobCount": len(normalized)}
    except Exception as exc:
        mdc.write_warning(f"Failed to persist bronze finance manifest: {exc}")
        return None


def load_latest_bronze_finance_manifest() -> Optional[Dict[str, Any]]:
    if not manifests_enabled():
        return None
    if not _require_common_storage("bronze finance manifest read"):
        return None

    latest_path = f"{_BRONZE_FINANCE_PREFIX}/latest.json"
    latest = mdc.get_common_json_content(latest_path)
    if not isinstance(latest, dict):
        return None

    manifest_path = str(latest.get("manifestPath") or "").strip()
    if not manifest_path:
        return None

    manifest = mdc.get_common_json_content(manifest_path)
    if not isinstance(manifest, dict):
        return None
    manifest = dict(manifest)
    manifest.setdefault("manifestPath", manifest_path)
    return manifest


def silver_finance_ack_exists(run_id: str) -> bool:
    if not run_id:
        return False
    if not _require_common_storage("silver finance manifest ack read"):
        return False

    path = f"{_SILVER_FINANCE_PREFIX}/{run_id}.json"
    existing = mdc.get_common_json_content(path)
    return isinstance(existing, dict)


def write_silver_finance_ack(
    *,
    run_id: str,
    manifest_path: str,
    status: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if not manifests_enabled():
        return None
    if not run_id:
        return None
    if not _require_common_storage("silver finance manifest ack write"):
        return None

    payload = {
        "version": _MANIFEST_VERSION,
        "manifestType": "silver-finance-ack",
        "runId": str(run_id).strip(),
        "manifestPath": str(manifest_path or "").strip(),
        "status": str(status or "").strip().lower() or "unknown",
        "recordedAt": _iso(datetime.now(timezone.utc)),
        "metadata": dict(metadata or {}),
    }
    ack_path = f"{_SILVER_FINANCE_PREFIX}/{run_id}.json"
    try:
        mdc.save_common_json_content(payload, ack_path)
        return ack_path
    except Exception as exc:
        mdc.write_warning(f"Failed to persist silver finance manifest ack for runId={run_id}: {exc}")
        return None


def manifest_blobs(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    raw = manifest.get("blobs")
    if not isinstance(raw, list):
        return out
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        parsed: Dict[str, Any] = {"name": name}
        etag = item.get("etag")
        if etag is not None:
            parsed["etag"] = str(etag)
        lm = _parse_iso(item.get("last_modified"))
        if lm is not None:
            parsed["last_modified"] = lm
        size = item.get("size")
        if isinstance(size, int):
            parsed["size"] = int(size)
        out.append(parsed)
    out.sort(key=lambda item: item["name"])
    return out
