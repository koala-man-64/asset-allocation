import logging
import json
import os
import re
import threading
import time
import uuid
import hashlib
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Literal, Tuple, TypeVar, Sequence

import httpx
from anyio import from_thread
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from api.service.dependencies import (
    get_alert_state_store,
    get_auth_manager,
    get_settings,
    get_system_health_cache,
    validate_auth,
)
from api.service.realtime import manager as realtime_manager
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.lineage import get_lineage_snapshot
from monitoring.domain_metadata import collect_domain_metadata
from monitoring.log_analytics import AzureLogAnalyticsClient
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache
from core import config as cfg
from core import core as mdc
from core.blob_storage import BlobStorageClient
from core.debug_symbols import read_debug_symbols_state, update_debug_symbols_state
from core.core import get_symbol_sync_state
from core.delta_core import load_delta
from core.delta_core import get_delta_schema_columns
from core.pipeline import DataPaths
from core.postgres import PostgresError
from core.runtime_config import (
    DEFAULT_ENV_OVERRIDE_KEYS,
    delete_runtime_config,
    list_runtime_config,
    normalize_env_override,
    upsert_runtime_config,
)
from core.purge_rules import (
    PurgeRule,
    claim_purge_rule_for_run,
    complete_purge_rule_execution,
    create_purge_rule,
    delete_purge_rule as delete_purge_rule_row,
    get_purge_rule,
    is_percent_operator,
    list_due_purge_rules,
    list_purge_rules,
    normalize_purge_rule_operator,
    supported_purge_rule_operators,
    update_purge_rule,
)

logger = logging.getLogger("asset-allocation.api.system")

router = APIRouter()


REALTIME_TOPIC_BACKTESTS = "backtests"
REALTIME_TOPIC_SYSTEM_HEALTH = "system-health"
REALTIME_TOPIC_JOBS = "jobs"
REALTIME_TOPIC_CONTAINER_APPS = "container-apps"
REALTIME_TOPIC_ALERTS = "alerts"
REALTIME_TOPIC_RUNTIME_CONFIG = "runtime-config"
REALTIME_TOPIC_DEBUG_SYMBOLS = "debug-symbols"

_PURGE_OPERATIONS: Dict[str, Dict[str, Any]] = {}
_PURGE_OPERATIONS_LOCK = threading.Lock()
_PURGE_BLACKLIST_UPDATE_LOCK = threading.Lock()
_PURGE_RULE_AUDIT_INTERVAL_MINUTES = 60 * 24 * 365
_DEFAULT_PURGE_SYMBOL_MAX_WORKERS = 8
_MAX_PURGE_SYMBOL_MAX_WORKERS = 32
_DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS = 8
_MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS = 32
_DEFAULT_PURGE_SCOPE_MAX_WORKERS = 8
_MAX_PURGE_SCOPE_MAX_WORKERS = 32
_DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS = 8
_MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS = 32
_DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS = 3
_MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS = 3
_T = TypeVar("_T")


def _emit_realtime(topic: str, event_type: str, payload: Optional[Dict[str, Any]] = None) -> None:
    """
    Emit websocket events from sync FastAPI endpoints.

    Endpoints in this module are mostly sync (`def`) and run in AnyIO worker threads.
    `from_thread.run` bridges to the app event loop so connected websocket clients receive updates.
    """
    message = {
        "type": event_type,
        "payload": payload or {},
        "emittedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        from_thread.run(realtime_manager.broadcast, topic, message)
    except RuntimeError:
        logger.debug(
            "Realtime emit skipped (no AnyIO worker context): topic=%s type=%s",
            topic,
            event_type,
        )
    except Exception:
        logger.exception("Realtime emit failed: topic=%s type=%s", topic, event_type)


def _extract_arm_error_message(response: httpx.Response) -> str:
    """
    Best-effort extraction of a human-friendly error message from ARM responses.

    Some ARM endpoints return a JSON string like:
      "Reason: Bad Request. Body: {\"error\":\"...\",\"success\":false}"
    """

    def _from_mapping(payload: Dict[str, Any]) -> str:
        err = payload.get("error")
        if isinstance(err, dict):
            message = err.get("message") or err.get("Message") or err.get("detail") or err.get("details")
            if isinstance(message, str) and message.strip():
                return message.strip()
            code = err.get("code") or err.get("Code")
            if isinstance(code, str) and code.strip():
                return code.strip()
            return json.dumps(err, ensure_ascii=False)
        if isinstance(err, str) and err.strip():
            return err.strip()
        message = payload.get("message") or payload.get("Message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        return json.dumps(payload, ensure_ascii=False)

    def _from_text(text: str) -> str:
        cleaned = (text or "").strip()
        if not cleaned:
            return ""
        # If the payload includes an embedded JSON body fragment, prefer it.
        match = re.search(r"Body:\\s*(\\{.*\\})\\s*$", cleaned)
        if match:
            fragment = match.group(1)
            try:
                nested = json.loads(fragment)
            except json.JSONDecodeError:
                return cleaned
            if isinstance(nested, dict):
                return _from_mapping(nested)
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
            return fragment
        return cleaned

    try:
        payload = response.json()
    except Exception:
        return _from_text(response.text)

    if isinstance(payload, dict):
        return _from_mapping(payload)
    if isinstance(payload, str):
        return _from_text(payload)
    return _from_text(response.text)


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _get_actor(request: Request) -> Optional[str]:
    settings = get_settings(request)
    if settings.auth_mode == "none":
        return None
    auth = get_auth_manager(request)
    ctx = auth.authenticate_headers(dict(request.headers))
    if ctx.subject:
        return ctx.subject
    for key in ("preferred_username", "email", "upn"):
        value = ctx.claims.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _job_control_context(request: Request) -> Dict[str, str]:
    actor = _get_actor(request)
    request_id = request.headers.get("x-request-id")
    context: Dict[str, str] = {}
    if actor:
        context["actor"] = actor
    if request_id:
        context["requestId"] = request_id.strip()
    return context


def _split_csv(raw: Optional[str]) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _normalize_container_app_name(value: str) -> str:
    return str(value or "").strip().lower().replace("_", "-")


def _container_app_allowlist() -> Tuple[str, str, List[str]]:
    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""
    app_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS")
    app_allowlist = _split_csv(app_names_raw)
    return subscription_id, resource_group, app_allowlist


def _container_app_health_url_overrides() -> Dict[str, str]:
    raw = (os.environ.get("SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON") or "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        logger.warning("Invalid SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON; expected JSON object.")
        return {}
    if not isinstance(payload, dict):
        logger.warning("Invalid SYSTEM_HEALTH_CONTAINERAPP_HEALTH_URLS_JSON type; expected JSON object.")
        return {}

    out: Dict[str, str] = {}
    for key, value in payload.items():
        name = _normalize_container_app_name(str(key or ""))
        url = str(value or "").strip()
        if not name or not url:
            continue
        out[name] = url
    return out


def _container_app_default_health_path(app_name: str) -> str:
    lowered = _normalize_container_app_name(app_name)
    if "api" in lowered:
        return "/healthz"
    return "/"


def _resolve_container_app_health_url(
    app_name: str,
    *,
    ingress_fqdn: Optional[str],
    overrides: Dict[str, str],
) -> Optional[str]:
    override = overrides.get(_normalize_container_app_name(app_name))
    if override:
        if override.startswith(("http://", "https://")):
            return override
        if override.startswith("/") and ingress_fqdn:
            path = override if override.startswith("/") else f"/{override}"
            return f"https://{ingress_fqdn}{path}"
        return override

    if not ingress_fqdn:
        return None
    path = _container_app_default_health_path(app_name)
    if not path.startswith("/"):
        path = f"/{path}"
    return f"https://{ingress_fqdn}{path}"


def _probe_container_app_health(url: str, *, timeout_seconds: float) -> Dict[str, Any]:
    checked_at = datetime.now(timezone.utc).isoformat()
    try:
        with httpx.Client(timeout=max(0.5, float(timeout_seconds)), follow_redirects=True, trust_env=False) as client:
            response = client.get(url)
        status_code = int(response.status_code)
        if 200 <= status_code < 400:
            status = "healthy"
        elif 400 <= status_code < 500:
            status = "warning"
        else:
            status = "error"
        return {
            "status": status,
            "url": url,
            "httpStatus": status_code,
            "checkedAt": checked_at,
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "error",
            "url": url,
            "httpStatus": None,
            "checkedAt": checked_at,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _resource_status_from_provisioning_state(value: str) -> str:
    state = str(value or "").strip().lower()
    if state == "succeeded":
        return "healthy"
    if state in {"failed", "canceled", "cancelled"}:
        return "error"
    if state in {"creating", "updating", "deleting", "inprogress"}:
        return "warning"
    if not state:
        return "unknown"
    return "warning"


def _worse_status(a: str, b: str) -> str:
    order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return b if order.get(b, 0) > order.get(a, 0) else a


def _extract_container_app_properties(payload: Dict[str, Any]) -> Dict[str, Any]:
    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    configuration = props.get("configuration") if isinstance(props.get("configuration"), dict) else {}
    ingress = configuration.get("ingress") if isinstance(configuration.get("ingress"), dict) else {}

    provisioning_state = str(props.get("provisioningState") or "").strip() or None
    running_state = (
        str(props.get("runningStatus") or "").strip()
        or str(props.get("runningState") or "").strip()
        or None
    )
    latest_ready_revision = str(props.get("latestReadyRevisionName") or "").strip() or None
    ingress_fqdn = str(ingress.get("fqdn") or "").strip() or None
    resource_id = str(payload.get("id") or "").strip() or None

    return {
        "provisioningState": provisioning_state,
        "runningState": running_state,
        "latestReadyRevisionName": latest_ready_revision,
        "ingressFqdn": ingress_fqdn,
        "azureId": resource_id,
    }


@router.get("/health")
def system_health(request: Request, refresh: bool = Query(False)) -> JSONResponse:
    request_id = request.headers.get("x-request-id", "")
    logger.info(
        "System health request: refresh=%s path=%s host=%s fwd=%s request_id=%s",
        refresh,
        request.url.path,
        request.headers.get("host", ""),
        request.headers.get("x-forwarded-for", ""),
        request_id,
    )
    validate_auth(request)
    settings = get_settings(request)

    include_ids = False
    if settings.auth_mode != "none":
        raw_env = os.environ.get("SYSTEM_HEALTH_VERBOSE_IDS")
        raw = raw_env.strip().lower() if raw_env else ""
        include_ids = raw in {"1", "true", "t", "yes", "y", "on"}

    cache: TtlCache[Dict[str, Any]] = get_system_health_cache(request)

    def _refresh() -> Dict[str, Any]:
        return collect_system_health_snapshot(include_resource_ids=include_ids)

    try:
        result = cache.get(_refresh, force_refresh=bool(refresh))
    except Exception as exc:
        logger.exception("System health cache refresh failed.")
        raise HTTPException(status_code=503, detail=f"System health unavailable: {exc}") from exc

    payload: Dict[str, Any] = dict(result.value or {})
    raw_alerts = payload.get("alerts")
    if isinstance(raw_alerts, list):
        payload["alerts"] = [dict(item) if isinstance(item, dict) else item for item in raw_alerts]

    alert_store = get_alert_state_store(request)
    if alert_store and isinstance(payload.get("alerts"), list):
        alert_ids: list[str] = []
        for alert in payload["alerts"]:
            if not isinstance(alert, dict):
                continue
            alert_id = str(alert.get("id") or "").strip()
            if alert_id:
                alert_ids.append(alert_id)
        try:
            states = alert_store.get_states(alert_ids)
        except Exception:
            logger.exception("Failed to load alert lifecycle states; returning stateless alerts.")
            states = {}

        for alert in payload["alerts"]:
            if not isinstance(alert, dict):
                continue
            alert_id = str(alert.get("id") or "").strip()
            if not alert_id:
                continue
            state = states.get(alert_id)
            if not state:
                continue

            alert["acknowledged"] = bool(state.acknowledged_at)
            alert["acknowledgedAt"] = _iso(state.acknowledged_at)
            alert["acknowledgedBy"] = state.acknowledged_by
            alert["snoozedUntil"] = _iso(state.snoozed_until)
            alert["resolvedAt"] = _iso(state.resolved_at)
            alert["resolvedBy"] = state.resolved_by
    elif alert_store is None:
        logger.info("System health alert store not configured (alerts will be unacknowledgeable).")

    logger.info(
        "System health payload ready: cache_hit=%s refresh_error=%s layers=%s alerts=%s resources=%s recent_jobs=%s",
        result.cache_hit,
        bool(result.refresh_error),
        len(payload.get("dataLayers") or []),
        len(payload.get("alerts") or []),
        len(payload.get("resources") or []),
        len(payload.get("recentJobs") or []),
    )
    recent_runs_preview: list[str] = []
    for run in (payload.get("recentJobs") or [])[:10]:
        if not isinstance(run, dict):
            continue
        job_name = str(run.get("jobName") or "").strip() or "?"
        status = str(run.get("status") or "").strip() or "unknown"
        start_time = str(run.get("startTime") or "").strip() or "n/a"
        recent_runs_preview.append(f"{job_name}:{status}@{start_time}")
    if recent_runs_preview:
        logger.info("System health recentJobs preview: %s", " | ".join(recent_runs_preview))
    elif payload.get("recentJobs") == []:
        logger.warning("System health recentJobs is empty.")

    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-System-Health-Cache": "hit" if result.cache_hit else "miss",
    }
    if result.refresh_error:
        headers["X-System-Health-Cache-Degraded"] = "1"
        # Backward-compatible legacy signal.
        headers["X-System-Health-Stale"] = "1"
    return JSONResponse(payload, headers=headers)



class SymbolSyncStateResponse(BaseModel):
    id: int
    last_refreshed_at: Optional[str] = None
    last_refreshed_sources: Optional[Dict[str, Any]] = None
    last_refresh_error: Optional[str] = None


@router.get("/symbol-sync-state", response_model=SymbolSyncStateResponse)
def get_symbol_sync_state_endpoint(request: Request) -> JSONResponse:
    validate_auth(request)
    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")
    
    try:
        state = get_symbol_sync_state(dsn)
    except Exception as exc:
        logger.exception("Failed to load symbol sync state.")
        raise HTTPException(status_code=500, detail=f"Failed to load symbol sync state: {exc}") from exc
        
    if not state:
        return JSONResponse(
            {
                "id": 1,
                "last_refreshed_at": None,
                "last_refreshed_sources": None,
                "last_refresh_error": None,
            },
            headers={"Cache-Control": "no-store"},
        )
    
    return JSONResponse(
        {
            "id": state["id"],
            "last_refreshed_at": _iso(state["last_refreshed_at"]),
            "last_refreshed_sources": state["last_refreshed_sources"],
            "last_refresh_error": state["last_refresh_error"],
        },
        headers={"Cache-Control": "no-store"},
    )


class DomainDateRange(BaseModel):
    min: Optional[str] = None
    max: Optional[str] = None
    column: Optional[str] = None
    source: Optional[Literal["partition", "stats"]] = None


class DomainMetadataResponse(BaseModel):
    layer: str
    domain: str
    container: str
    type: Literal["blob", "delta"]
    computedAt: str
    folderLastModified: Optional[str] = None
    cachedAt: Optional[str] = None
    cacheSource: Optional[Literal["snapshot", "live-refresh"]] = None
    symbolCount: Optional[int] = None
    financeSubfolderSymbolCounts: Optional[Dict[str, int]] = None
    dateRange: Optional[DomainDateRange] = None
    totalRows: Optional[int] = None
    fileCount: Optional[int] = None
    totalBytes: Optional[int] = None
    deltaVersion: Optional[int] = None
    tablePath: Optional[str] = None
    prefix: Optional[str] = None
    blacklistedSymbolCount: Optional[int] = None
    coverageStatus: Optional[str] = None
    asOfCutoff: Optional[str] = None
    lagSymbolCount: Optional[int] = None
    coverageReportPath: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class DomainMetadataSnapshotResponse(BaseModel):
    version: int = 1
    updatedAt: Optional[str] = None
    entries: Dict[str, DomainMetadataResponse] = Field(default_factory=dict)
    warnings: List[str] = Field(default_factory=list)


_DOMAIN_METADATA_CACHE_FILE_DEFAULT = "metadata/domain-metadata.json"
_DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS = 30.0
_DOMAIN_METADATA_DOCUMENT_CACHE_LOCK = threading.Lock()
_DOMAIN_METADATA_DOCUMENT_CACHE: Optional[Dict[str, Any]] = None
_DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0
_DOMAIN_METADATA_UI_CACHE_FILE_DEFAULT = "metadata/ui-cache/domain-metadata-snapshot.json"


def _domain_metadata_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_METADATA_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_METADATA_CACHE_FILE_DEFAULT


def _domain_metadata_ui_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_METADATA_UI_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_METADATA_UI_CACHE_FILE_DEFAULT


def _domain_metadata_snapshot_cache_ttl_seconds() -> float:
    raw = (os.environ.get("DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS") or "").strip()
    if not raw:
        return _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS
    try:
        ttl = float(raw)
    except ValueError:
        logger.warning(
            "Invalid DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS=%r. Using default=%s.",
            raw,
            _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS,
        )
        return _DEFAULT_DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS
    if ttl < 0:
        return 0.0
    return ttl


def _cache_domain_metadata_document(payload: Dict[str, Any]) -> None:
    ttl = _domain_metadata_snapshot_cache_ttl_seconds()
    with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
        global _DOMAIN_METADATA_DOCUMENT_CACHE
        global _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
        if ttl <= 0:
            _DOMAIN_METADATA_DOCUMENT_CACHE = None
            _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0
            return
        _DOMAIN_METADATA_DOCUMENT_CACHE = deepcopy(payload)
        _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = time.monotonic() + ttl


def _invalidate_domain_metadata_document_cache() -> None:
    with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
        global _DOMAIN_METADATA_DOCUMENT_CACHE
        global _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
        _DOMAIN_METADATA_DOCUMENT_CACHE = None
        _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT = 0.0


def _domain_metadata_cache_key(layer: str, domain: str) -> str:
    normalized_layer = _normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = _normalize_domain(domain) or str(domain or "").strip().lower()
    return f"{normalized_layer}/{normalized_domain}"


def _default_domain_metadata_document() -> Dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}}


def _load_domain_metadata_document(force_refresh: bool = False) -> Dict[str, Any]:
    if not force_refresh:
        now = time.monotonic()
        with _DOMAIN_METADATA_DOCUMENT_CACHE_LOCK:
            cached = _DOMAIN_METADATA_DOCUMENT_CACHE
            expires_at = _DOMAIN_METADATA_DOCUMENT_CACHE_EXPIRES_AT
            if isinstance(cached, dict) and now < expires_at:
                return deepcopy(cached)

    path = _domain_metadata_cache_path()
    payload = mdc.get_common_json_content(path)
    if not isinstance(payload, dict):
        payload = _default_domain_metadata_document()

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    _cache_domain_metadata_document(payload)
    return payload


def _read_cached_domain_metadata_snapshot(layer: str, domain: str) -> Optional[Dict[str, Any]]:
    key = _domain_metadata_cache_key(layer, domain)
    payload = _load_domain_metadata_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return None

    raw_entry = entries.get(key)
    if not isinstance(raw_entry, dict):
        return None

    raw_metadata = raw_entry.get("metadata")
    if isinstance(raw_metadata, dict):
        metadata = dict(raw_metadata)
    else:
        # Backward compatibility if older cache files stored payload directly.
        legacy_payload = raw_entry.get("payload")
        metadata = dict(legacy_payload) if isinstance(legacy_payload, dict) else {}

    if not metadata:
        return None

    normalized_layer = _normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = _normalize_domain(domain) or str(domain or "").strip().lower()
    metadata["layer"] = normalized_layer
    metadata["domain"] = normalized_domain

    cached_at = raw_entry.get("cachedAt")
    if not isinstance(cached_at, str) or not cached_at.strip():
        legacy_cached = metadata.get("cachedAt") or raw_entry.get("updatedAt") or payload.get("updatedAt")
        cached_at = str(legacy_cached).strip() if isinstance(legacy_cached, str) else ""
    if cached_at:
        metadata["cachedAt"] = cached_at
    metadata["cacheSource"] = "snapshot"
    return metadata


def _write_cached_domain_metadata_snapshot(layer: str, domain: str, metadata: Dict[str, Any]) -> str:
    if not isinstance(metadata, dict):
        raise ValueError("metadata payload must be a JSON object.")

    normalized_layer = _normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = _normalize_domain(domain) or str(domain or "").strip().lower()
    key = _domain_metadata_cache_key(normalized_layer, normalized_domain)

    payload = _load_domain_metadata_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        payload["entries"] = entries

    now = _utc_timestamp()
    metadata_payload = dict(metadata)
    metadata_payload["layer"] = normalized_layer
    metadata_payload["domain"] = normalized_domain
    metadata_payload["cachedAt"] = now
    metadata_payload["cacheSource"] = "snapshot"

    previous_entry = entries.get(key)
    history: List[Dict[str, Any]] = []
    if isinstance(previous_entry, dict):
        previous_history = previous_entry.get("history")
        if isinstance(previous_history, list):
            for item in previous_history[-199:]:
                if isinstance(item, dict):
                    history.append(dict(item))

    history.append(
        {
            "timestamp": now,
            "symbolCount": metadata_payload.get("symbolCount"),
            "fileCount": metadata_payload.get("fileCount"),
            "totalRows": metadata_payload.get("totalRows"),
            "totalBytes": metadata_payload.get("totalBytes"),
            "deltaVersion": metadata_payload.get("deltaVersion"),
        }
    )

    entries[key] = {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "cachedAt": now,
        "metadata": metadata_payload,
        "history": history[-200:],
    }
    payload["version"] = 1
    payload["updatedAt"] = now
    mdc.save_common_json_content(payload, _domain_metadata_cache_path())
    _cache_domain_metadata_document(payload)
    return now


def _extract_cached_domain_metadata_snapshots(payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}

    extracted: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_entry in entries.items():
        if not isinstance(raw_entry, dict):
            continue

        raw_metadata = raw_entry.get("metadata")
        if isinstance(raw_metadata, dict):
            metadata = dict(raw_metadata)
        else:
            legacy_payload = raw_entry.get("payload")
            metadata = dict(legacy_payload) if isinstance(legacy_payload, dict) else {}
        if not metadata:
            continue

        layer = _normalize_layer(str(metadata.get("layer") or raw_entry.get("layer") or ""))
        domain = _normalize_domain(str(metadata.get("domain") or raw_entry.get("domain") or ""))
        if not layer or not domain:
            if isinstance(raw_key, str) and "/" in raw_key:
                prefix, suffix = raw_key.split("/", 1)
                layer = layer or _normalize_layer(prefix)
                domain = domain or _normalize_domain(suffix)
        if not layer or not domain:
            continue

        key = _domain_metadata_cache_key(layer, domain)
        metadata["layer"] = layer
        metadata["domain"] = domain

        cached_at = raw_entry.get("cachedAt")
        if not isinstance(cached_at, str) or not cached_at.strip():
            legacy_cached = metadata.get("cachedAt") or raw_entry.get("updatedAt") or payload.get("updatedAt")
            cached_at = str(legacy_cached).strip() if isinstance(legacy_cached, str) else ""
        if cached_at:
            metadata["cachedAt"] = cached_at
        metadata["cacheSource"] = "snapshot"
        extracted[key] = metadata

    return extracted


def _parse_domain_metadata_filter(
    raw: Optional[str],
    *,
    param_name: str,
    normalizer: Callable[[str], Optional[str]],
    allowed_values: Optional[set[str]] = None,
) -> Optional[set[str]]:
    text = (raw or "").strip()
    if not text:
        return None
    items = _split_csv(text)
    if not items:
        return set()
    normalized: set[str] = set()
    for item in items:
        value = normalizer(item)
        if not value:
            raise HTTPException(
                status_code=400,
                detail=f"{param_name} contains unsupported value: {item!r}.",
            )
        if allowed_values is not None and value not in allowed_values:
            raise HTTPException(
                status_code=400,
                detail=f"{param_name} contains unsupported value: {item!r}.",
            )
        normalized.add(value)
    return normalized


@router.get("/domain-metadata", response_model=DomainMetadataResponse)
def domain_metadata(
    request: Request,
    layer: str = Query(..., description="Medallion layer key (bronze|silver|gold|platinum)"),
    domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
    refresh: bool = Query(default=False, description="When true, recompute metadata and update the snapshot cache."),
    cache_only: bool = Query(
        default=False,
        alias="cacheOnly",
        description="When true, return only cached snapshot data and never compute live metadata.",
    ),
) -> JSONResponse:
    validate_auth(request)
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if not normalized_layer:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")

    if not refresh:
        try:
            cached_payload = _read_cached_domain_metadata_snapshot(normalized_layer, normalized_domain)
        except Exception as exc:
            logger.warning(
                "Domain metadata snapshot read failed. layer=%s domain=%s err=%s",
                normalized_layer,
                normalized_domain,
                exc,
            )
            cached_payload = None
        if cached_payload is not None:
            headers: Dict[str, str] = {
                "Cache-Control": "no-store",
                "X-Domain-Metadata-Source": "snapshot",
            }
            cached_at = cached_payload.get("cachedAt")
            if isinstance(cached_at, str) and cached_at.strip():
                headers["X-Domain-Metadata-Cached-At"] = cached_at
            return JSONResponse(cached_payload, headers=headers)
        if cache_only:
            missing_message = (
                "No cached domain metadata snapshot found for "
                f"layer={normalized_layer} domain={normalized_domain}."
            )
            now = _utc_timestamp()
            # Cache-only mode must not trigger a live compute. Return a typed placeholder payload
            # so UI consumers avoid noisy 404 spam while still receiving an explicit miss signal.
            placeholder_payload: Dict[str, Any] = {
                "layer": normalized_layer,
                "domain": normalized_domain,
                "container": "",
                "type": "blob",
                "computedAt": now,
                "folderLastModified": None,
                "cachedAt": None,
                "cacheSource": "snapshot",
                "symbolCount": None,
                "dateRange": None,
                "totalRows": None,
                "fileCount": None,
                "totalBytes": None,
                "deltaVersion": None,
                "tablePath": None,
                "prefix": None,
                "blacklistedSymbolCount": None,
                "coverageStatus": None,
                "asOfCutoff": None,
                "lagSymbolCount": None,
                "coverageReportPath": None,
                "warnings": [missing_message],
            }
            return JSONResponse(
                placeholder_payload,
                headers={
                    "Cache-Control": "no-store",
                    "X-Domain-Metadata-Source": "snapshot-miss",
                    "X-Domain-Metadata-Cache-Miss": "1",
                },
            )

    try:
        payload = collect_domain_metadata(layer=normalized_layer, domain=normalized_domain)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Domain metadata collection failed: layer=%s domain=%s", normalized_layer, normalized_domain)
        raise HTTPException(status_code=503, detail=f"Domain metadata unavailable: {exc}") from exc

    payload_out = dict(payload)
    payload_out["cacheSource"] = "live-refresh"
    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-Domain-Metadata-Source": payload_out["cacheSource"],
    }
    try:
        cached_at = _write_cached_domain_metadata_snapshot(normalized_layer, normalized_domain, payload_out)
        payload_out["cachedAt"] = cached_at
        headers["X-Domain-Metadata-Cached-At"] = cached_at
    except Exception as exc:
        logger.warning(
            "Domain metadata snapshot write failed. layer=%s domain=%s err=%s",
            normalized_layer,
            normalized_domain,
            exc,
        )
        warnings = payload_out.setdefault("warnings", [])
        if isinstance(warnings, list):
            warnings.append(f"Snapshot cache write failed: {exc}")

    return JSONResponse(payload_out, headers=headers)


@router.get("/domain-metadata/snapshot", response_model=DomainMetadataSnapshotResponse)
def domain_metadata_snapshot(
    request: Request,
    layers: Optional[str] = Query(
        default=None,
        description="Optional comma-separated layer filter (e.g. bronze,silver,gold).",
    ),
    domains: Optional[str] = Query(
        default=None,
        description="Optional comma-separated domain filter (e.g. market,finance,earnings,price-target).",
    ),
    cache_only: bool = Query(
        default=True,
        alias="cacheOnly",
        description="When true, return only cached snapshot entries and do not compute missing metadata.",
    ),
    refresh: bool = Query(
        default=False,
        description="When true, bypass the in-process snapshot document cache before reading persisted metadata.",
    ),
) -> JSONResponse:
    validate_auth(request)
    layer_filter = _parse_domain_metadata_filter(
        layers,
        param_name="layers",
        normalizer=lambda value: _normalize_layer(value),
        allowed_values=set(_LAYER_CONTAINER_ENV.keys()),
    )
    domain_filter = _parse_domain_metadata_filter(
        domains,
        param_name="domains",
        normalizer=lambda value: _normalize_domain(value),
    )

    try:
        snapshot_doc = _load_domain_metadata_document(force_refresh=bool(refresh))
    except Exception as exc:
        logger.warning("Domain metadata snapshot load failed: %s", exc)
        snapshot_doc = _default_domain_metadata_document()
        _invalidate_domain_metadata_document_cache()

    all_entries = _extract_cached_domain_metadata_snapshots(snapshot_doc)
    filtered_entries: Dict[str, Dict[str, Any]] = {}
    warnings: List[str] = []

    for key, metadata in all_entries.items():
        layer = _normalize_layer(str(metadata.get("layer") or ""))
        domain = _normalize_domain(str(metadata.get("domain") or ""))
        if not layer or not domain:
            continue
        if layer_filter is not None and layer not in layer_filter:
            continue
        if domain_filter is not None and domain not in domain_filter:
            continue
        filtered_entries[key] = metadata

    # Optional warm-fill for known layer/domain combinations only.
    if not cache_only and layer_filter and domain_filter:
        for layer in sorted(layer_filter):
            for domain in sorted(domain_filter):
                key = _domain_metadata_cache_key(layer, domain)
                if key in filtered_entries:
                    continue
                try:
                    payload = collect_domain_metadata(layer=layer, domain=domain)
                    payload_out = dict(payload)
                    payload_out["cacheSource"] = "live-refresh"
                    cached_at = _write_cached_domain_metadata_snapshot(layer, domain, payload_out)
                    payload_out["cachedAt"] = cached_at
                    payload_out["cacheSource"] = "snapshot"
                    filtered_entries[key] = payload_out
                except Exception as exc:
                    warnings.append(f"Unable to compute metadata for {key}: {exc}")

        # Refresh snapshot metadata after optional warm-fill writes.
        try:
            snapshot_doc = _load_domain_metadata_document(force_refresh=True)
        except Exception:
            pass

    response_payload: Dict[str, Any] = {
        "version": int(snapshot_doc.get("version") or 1),
        "updatedAt": snapshot_doc.get("updatedAt"),
        "entries": filtered_entries,
        "warnings": warnings,
    }
    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-Domain-Metadata-Source": "snapshot-batch",
        "X-Domain-Metadata-Entry-Count": str(len(filtered_entries)),
    }
    updated_at = response_payload.get("updatedAt")
    if isinstance(updated_at, str) and updated_at.strip():
        headers["X-Domain-Metadata-Updated-At"] = updated_at
        headers["Last-Modified"] = updated_at
    etag_basis = {
        "updatedAt": response_payload.get("updatedAt"),
        "keys": sorted(filtered_entries.keys()),
    }
    etag = f'W/"{hashlib.sha256(json.dumps(etag_basis, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:24]}"'
    headers["ETag"] = etag
    if (request.headers.get("if-none-match") or "").strip() == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(response_payload, headers=headers)


@router.get("/domain-metadata/snapshot/cache", response_model=DomainMetadataSnapshotResponse)
def get_domain_metadata_snapshot_cache(request: Request) -> JSONResponse:
    validate_auth(request)
    warnings: List[str] = []
    cache_hit = False
    payload: Dict[str, Any] = {}

    try:
        raw = mdc.get_common_json_content(_domain_metadata_ui_cache_path())
    except Exception as exc:
        logger.warning("Failed to read persisted UI domain metadata cache: %s", exc)
        raw = None
        warnings.append(f"Read failed: {exc}")

    if isinstance(raw, dict):
        try:
            parsed = DomainMetadataSnapshotResponse(**raw)
            cache_hit = True
            payload = parsed.model_dump() if hasattr(parsed, "model_dump") else parsed.dict()
        except Exception as exc:
            logger.warning("Persisted UI cache payload was invalid. Returning empty snapshot. err=%s", exc)
            warnings.append(f"Invalid cache payload ignored: {exc}")

    if not payload:
        payload = {
            "version": 1,
            "updatedAt": None,
            "entries": {},
            "warnings": warnings or ["No persisted UI domain metadata snapshot found."],
        }
    elif warnings:
        payload["warnings"] = [*list(payload.get("warnings") or []), *warnings]

    return JSONResponse(
        payload,
        headers={
            "Cache-Control": "no-store",
            "X-Domain-Metadata-UI-Cache": "hit" if cache_hit else "miss",
            "X-Domain-Metadata-Entry-Count": str(len(payload.get("entries") or {})),
        },
    )


@router.put("/domain-metadata/snapshot/cache", response_model=DomainMetadataSnapshotResponse)
def put_domain_metadata_snapshot_cache(
    request: Request,
    payload: DomainMetadataSnapshotResponse,
) -> JSONResponse:
    validate_auth(request)
    payload_out = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
    if not str(payload_out.get("updatedAt") or "").strip():
        payload_out["updatedAt"] = _utc_timestamp()
    try:
        mdc.save_common_json_content(payload_out, _domain_metadata_ui_cache_path())
    except Exception as exc:
        logger.warning("Failed to persist UI domain metadata cache: %s", exc)
        raise HTTPException(status_code=503, detail=f"Failed to persist UI domain metadata cache: {exc}") from exc

    return JSONResponse(
        payload_out,
        headers={
            "Cache-Control": "no-store",
            "X-Domain-Metadata-UI-Cache": "written",
            "X-Domain-Metadata-Entry-Count": str(len(payload_out.get("entries") or {})),
        },
    )


class DomainColumnsResponse(BaseModel):
    layer: str
    domain: str
    columns: List[str] = Field(default_factory=list)
    found: bool = False
    promptRetrieve: bool = False
    source: Literal["common-file"] = "common-file"
    cachePath: str
    updatedAt: Optional[str] = None


class DomainColumnsRefreshRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    sample_limit: int = Field(default=500, ge=1, le=5000)


_DOMAIN_COLUMNS_CACHE_FILE_DEFAULT = "metadata/domain-columns.json"
_DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS_DEFAULT = 8.0
_DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS_DEFAULT = 25.0


def _domain_columns_cache_path() -> str:
    configured = (os.environ.get("DOMAIN_COLUMNS_CACHE_PATH") or "").strip()
    return configured or _DOMAIN_COLUMNS_CACHE_FILE_DEFAULT


def _parse_timeout_seconds_env(env_name: str, default_value: float) -> float:
    raw = (os.environ.get(env_name) or "").strip()
    if not raw:
        return float(default_value)
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid %s=%s. Using default=%s", env_name, raw, default_value)
        return float(default_value)
    if value <= 0:
        return float(default_value)
    return value


def _domain_columns_read_timeout_seconds() -> float:
    return _parse_timeout_seconds_env(
        "DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS",
        _DOMAIN_COLUMNS_READ_TIMEOUT_SECONDS_DEFAULT,
    )


def _domain_columns_refresh_timeout_seconds() -> float:
    return _parse_timeout_seconds_env(
        "DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS",
        _DOMAIN_COLUMNS_REFRESH_TIMEOUT_SECONDS_DEFAULT,
    )


def _run_with_timeout(fn: Callable[[], _T], *, timeout_seconds: float, timeout_message: str) -> _T:
    if timeout_seconds <= 0:
        return fn()

    done = threading.Event()
    state: Dict[str, Any] = {}

    def _worker() -> None:
        try:
            state["result"] = fn()
        except Exception as exc:
            state["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    if not done.wait(timeout_seconds):
        raise TimeoutError(timeout_message)

    if "error" in state:
        raise state["error"]
    return state["result"]


def _require_common_storage_for_domain_columns() -> None:
    if getattr(mdc, "common_storage_client", None) is None:
        raise HTTPException(
            status_code=503,
            detail="Common storage is unavailable (AZURE_CONTAINER_COMMON).",
        )


def _normalize_columns_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    normalized: List[str] = []
    for value in values:
        column = str(value or "").strip()
        if not column or column in seen:
            continue
        seen.add(column)
        normalized.append(column)
    return normalized


def _domain_columns_cache_key(layer: str, domain: str) -> str:
    normalized_layer = _normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = _normalize_domain(domain) or str(domain or "").strip().lower()
    return f"{normalized_layer}/{normalized_domain}"


def _default_domain_columns_document() -> Dict[str, Any]:
    return {"version": 1, "updatedAt": None, "entries": {}}


def _load_domain_columns_document() -> Dict[str, Any]:
    path = _domain_columns_cache_path()
    payload = mdc.get_common_json_content(path)
    if not isinstance(payload, dict):
        return _default_domain_columns_document()

    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    return payload


def _read_cached_domain_columns(layer: str, domain: str) -> tuple[List[str], Optional[str], bool]:
    key = _domain_columns_cache_key(layer, domain)
    payload = _load_domain_columns_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return [], None, False

    raw_entry = entries.get(key)
    if isinstance(raw_entry, list):
        columns = _normalize_columns_list(raw_entry)
        updated_at = payload.get("updatedAt")
        return columns, (str(updated_at) if isinstance(updated_at, str) else None), bool(columns)
    if not isinstance(raw_entry, dict):
        return [], None, False

    columns = _normalize_columns_list(raw_entry.get("columns"))
    updated_at = raw_entry.get("updatedAt")
    return columns, (str(updated_at) if isinstance(updated_at, str) else None), bool(columns)


def _write_cached_domain_columns(layer: str, domain: str, columns: List[str]) -> tuple[List[str], str]:
    normalized_columns = _normalize_columns_list(columns)
    if not normalized_columns:
        raise ValueError("No columns were discovered for cache update.")

    normalized_layer = _normalize_layer(layer) or str(layer or "").strip().lower()
    normalized_domain = _normalize_domain(domain) or str(domain or "").strip().lower()
    key = _domain_columns_cache_key(normalized_layer, normalized_domain)

    payload = _load_domain_columns_document()
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        payload["entries"] = entries

    now = _utc_timestamp()
    entries[key] = {
        "layer": normalized_layer,
        "domain": normalized_domain,
        "columns": normalized_columns,
        "updatedAt": now,
    }
    payload["version"] = 1
    payload["updatedAt"] = now
    mdc.save_common_json_content(payload, _domain_columns_cache_path())
    return normalized_columns, now


def _discover_first_delta_table_for_prefix(*, container: str, prefix: str) -> Optional[str]:
    normalized = f"{str(prefix or '').strip().strip('/')}/"
    if normalized == "/":
        return None

    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    marker = "/_delta_log/"
    for blob in client.container_client.list_blobs(name_starts_with=normalized):
        name = str(getattr(blob, "name", "") or "")
        if marker not in name:
            continue
        root = name.split(marker, 1)[0].strip("/")
        if root and root.startswith(normalized.rstrip("/")):
            return root
    return None


def _retrieve_domain_columns_from_schema(layer: str, domain: str) -> List[str]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if normalized_layer not in {"silver", "gold"}:
        return []
    if not normalized_domain:
        return []

    prefix = _RULE_DATA_PREFIXES.get(normalized_layer, {}).get(normalized_domain)
    if not prefix:
        return []

    container = _resolve_container(normalized_layer)
    first_table = _discover_first_delta_table_for_prefix(container=container, prefix=prefix)
    if not first_table:
        return []

    schema_columns = get_delta_schema_columns(container, first_table)
    return _normalize_columns_list(schema_columns or [])


def _retrieve_domain_columns(layer: str, domain: str, sample_limit: int) -> List[str]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if normalized_layer not in {"bronze", "silver", "gold"}:
        raise HTTPException(status_code=400, detail="layer must be bronze, silver, or gold.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")

    try:
        schema_columns = _retrieve_domain_columns_from_schema(normalized_layer, normalized_domain)
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "Schema-first column retrieval failed; falling back to sampled retrieval. layer=%s domain=%s err=%s",
            normalized_layer,
            normalized_domain,
            exc,
        )
        schema_columns = []

    if schema_columns:
        return schema_columns

    try:
        from api.data_service import DataService
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Data service unavailable: {exc}") from exc

    try:
        rows = DataService.get_data(
            layer=normalized_layer,
            domain=normalized_domain,
            ticker=None,
            limit=int(sample_limit),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Domain columns retrieval failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(status_code=500, detail=f"Failed to retrieve domain columns: {exc}") from exc

    for row in rows or []:
        if isinstance(row, dict) and row:
            return _normalize_columns_list(list(row.keys()))
    return []


@router.get("/domain-columns", response_model=DomainColumnsResponse)
def get_domain_columns(
    request: Request,
    layer: str = Query(..., description="Medallion layer key (bronze|silver|gold)"),
    domain: str = Query(..., description="Domain key (market|finance|earnings|price-target)"),
) -> JSONResponse:
    validate_auth(request)
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if not normalized_layer:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")
    _require_common_storage_for_domain_columns()

    read_timeout = _domain_columns_read_timeout_seconds()
    try:
        columns, updated_at, found = _run_with_timeout(
            lambda: _read_cached_domain_columns(normalized_layer, normalized_domain),
            timeout_seconds=read_timeout,
            timeout_message=(
                f"Domain columns cache read timed out after {read_timeout:.1f}s for "
                f"{normalized_layer}/{normalized_domain}."
            ),
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Domain columns cache read failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(status_code=503, detail=f"Domain columns cache unavailable: {exc}") from exc

    return JSONResponse(
        {
            "layer": normalized_layer,
            "domain": normalized_domain,
            "columns": columns,
            "found": found,
            "promptRetrieve": not found,
            "source": "common-file",
            "cachePath": _domain_columns_cache_path(),
            "updatedAt": updated_at,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/domain-columns/refresh", response_model=DomainColumnsResponse)
def refresh_domain_columns(payload: DomainColumnsRefreshRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    normalized_layer = _normalize_layer(payload.layer)
    normalized_domain = _normalize_domain(payload.domain)
    if not normalized_layer:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")
    _require_common_storage_for_domain_columns()

    refresh_timeout = _domain_columns_refresh_timeout_seconds()
    try:
        columns = _run_with_timeout(
            lambda: _retrieve_domain_columns(normalized_layer, normalized_domain, int(payload.sample_limit)),
            timeout_seconds=refresh_timeout,
            timeout_message=(
                f"Domain columns retrieval timed out after {refresh_timeout:.1f}s for "
                f"{normalized_layer}/{normalized_domain}."
            ),
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Domain columns refresh retrieval failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(status_code=503, detail=f"Domain columns retrieval unavailable: {exc}") from exc

    if not columns:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No columns discovered for {normalized_layer}/{normalized_domain}. "
                "Verify data exists and retry refresh."
            ),
        )

    try:
        cached_columns, updated_at = _run_with_timeout(
            lambda: _write_cached_domain_columns(normalized_layer, normalized_domain, columns),
            timeout_seconds=refresh_timeout,
            timeout_message=(
                f"Domain columns cache write timed out after {refresh_timeout:.1f}s for "
                f"{normalized_layer}/{normalized_domain}."
            ),
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Common storage is unavailable for column cache updates: {exc}",
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Domain columns cache update failed: layer=%s domain=%s",
            normalized_layer,
            normalized_domain,
        )
        raise HTTPException(status_code=500, detail=f"Failed to update domain columns cache: {exc}") from exc

    return JSONResponse(
        {
            "layer": normalized_layer,
            "domain": normalized_domain,
            "columns": cached_columns,
            "found": True,
            "promptRetrieve": False,
            "source": "common-file",
            "cachePath": _domain_columns_cache_path(),
            "updatedAt": updated_at,
        },
        headers={"Cache-Control": "no-store"},
    )


class SnoozeRequest(BaseModel):
    minutes: Optional[int] = Field(default=None, ge=1, le=7 * 24 * 60)
    until: Optional[datetime] = None


def _require_alert_store(request: Request):
    store = get_alert_state_store(request)
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Alert lifecycle persistence is not configured (POSTGRES_DSN).",
        )
    return store


@router.post("/alerts/{alert_id}/ack")
def acknowledge_alert(alert_id: str, request: Request) -> JSONResponse:
    validate_auth(request)
    store = _require_alert_store(request)
    actor = _get_actor(request)
    logger.info("Acknowledge alert: id=%s actor=%s", alert_id, actor or "-")
    try:
        state = store.acknowledge(alert_id, actor=actor)
    except Exception as exc:
        logger.exception("Failed to acknowledge alert: id=%s", alert_id)
        raise HTTPException(status_code=500, detail=f"Failed to acknowledge alert: {exc}") from exc
    response_payload = {
        "alertId": state.alert_id,
        "acknowledgedAt": _iso(state.acknowledged_at),
        "acknowledgedBy": state.acknowledged_by,
        "snoozedUntil": _iso(state.snoozed_until),
        "resolvedAt": _iso(state.resolved_at),
        "resolvedBy": state.resolved_by,
    }
    _emit_realtime(
        REALTIME_TOPIC_ALERTS,
        "ALERT_STATE_CHANGED",
        {
            "action": "acknowledge",
            "alertId": state.alert_id,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {"source": "alerts", "alertId": state.alert_id},
    )
    return JSONResponse(response_payload)


@router.post("/alerts/{alert_id}/snooze")
def snooze_alert(alert_id: str, payload: SnoozeRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    store = _require_alert_store(request)
    actor = _get_actor(request)

    until = payload.until
    if until is None:
        minutes = payload.minutes or 30
        until = datetime.now(timezone.utc) + timedelta(minutes=int(minutes))

    logger.info(
        "Snooze alert: id=%s actor=%s minutes=%s until=%s", alert_id, actor or "-", payload.minutes, payload.until
    )
    try:
        state = store.snooze(alert_id, until=until, actor=actor)
    except Exception as exc:
        logger.exception("Failed to snooze alert: id=%s", alert_id)
        raise HTTPException(status_code=500, detail=f"Failed to snooze alert: {exc}") from exc
    response_payload = {
        "alertId": state.alert_id,
        "acknowledgedAt": _iso(state.acknowledged_at),
        "acknowledgedBy": state.acknowledged_by,
        "snoozedUntil": _iso(state.snoozed_until),
        "resolvedAt": _iso(state.resolved_at),
        "resolvedBy": state.resolved_by,
    }
    _emit_realtime(
        REALTIME_TOPIC_ALERTS,
        "ALERT_STATE_CHANGED",
        {
            "action": "snooze",
            "alertId": state.alert_id,
            "snoozedUntil": response_payload["snoozedUntil"],
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {"source": "alerts", "alertId": state.alert_id},
    )
    return JSONResponse(response_payload)


@router.post("/alerts/{alert_id}/resolve")
def resolve_alert(alert_id: str, request: Request) -> JSONResponse:
    validate_auth(request)
    store = _require_alert_store(request)
    actor = _get_actor(request)
    logger.info("Resolve alert: id=%s actor=%s", alert_id, actor or "-")
    try:
        state = store.resolve(alert_id, actor=actor)
    except Exception as exc:
        logger.exception("Failed to resolve alert: id=%s", alert_id)
        raise HTTPException(status_code=500, detail=f"Failed to resolve alert: {exc}") from exc
    response_payload = {
        "alertId": state.alert_id,
        "acknowledgedAt": _iso(state.acknowledged_at),
        "acknowledgedBy": state.acknowledged_by,
        "snoozedUntil": _iso(state.snoozed_until),
        "resolvedAt": _iso(state.resolved_at),
        "resolvedBy": state.resolved_by,
    }
    _emit_realtime(
        REALTIME_TOPIC_ALERTS,
        "ALERT_STATE_CHANGED",
        {
            "action": "resolve",
            "alertId": state.alert_id,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {"source": "alerts", "alertId": state.alert_id},
    )
    return JSONResponse(response_payload)


@router.get("/lineage")
def system_lineage(request: Request) -> JSONResponse:
    validate_auth(request)
    payload = get_lineage_snapshot()
    logger.info(
        "System lineage generated: layers=%s strategies=%s domains=%s",
        len(payload.get("layers") or []),
        len(payload.get("strategies") or []),
        len((payload.get("impactsByDomain") or {}).keys()),
    )
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


class PurgeRequest(BaseModel):
    scope: Literal["layer-domain", "layer", "domain"]
    layer: Optional[str] = None
    domain: Optional[str] = None
    confirm: bool = False


class DomainListResetRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    confirm: bool = False


class DomainCheckpointResetRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    confirm: bool = False


class DomainListFileResponse(BaseModel):
    listType: Literal["whitelist", "blacklist"]
    path: str
    exists: bool
    symbolCount: int
    symbols: List[str] = Field(default_factory=list)
    truncated: bool = False
    warning: Optional[str] = None


class DomainListsResponse(BaseModel):
    layer: str
    domain: str
    container: str
    limit: int
    files: List[DomainListFileResponse] = Field(default_factory=list)
    loadedAt: str


class DomainCheckpointTargetResponse(BaseModel):
    operation: str
    path: str
    status: Literal["reset"]
    existed: bool
    deleted: bool


class DomainCheckpointResetResponse(BaseModel):
    layer: str
    domain: str
    container: Optional[str] = None
    resetCount: int
    deletedCount: int
    targets: List[DomainCheckpointTargetResponse] = Field(default_factory=list)
    updatedAt: str
    note: Optional[str] = None


class PurgeCandidatesRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    value: Optional[float] = None
    percentile: Optional[float] = None
    as_of: Optional[str] = None
    recent_rows: int = Field(default=1, ge=1, le=5000)
    aggregation: str = Field(default="avg", min_length=1, max_length=24)
    limit: Optional[int] = Field(default=None, ge=1, le=5000)
    offset: int = Field(default=0, ge=0)
    min_rows: int = Field(default=1, ge=1)


class PurgeSymbolRequest(BaseModel):
    symbol: str
    confirm: bool = False


class PurgeRuleAuditRequest(BaseModel):
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column_name: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    threshold: float
    aggregation: Optional[str] = Field(default=None, min_length=1, max_length=24)
    recent_rows: Optional[int] = Field(default=None, ge=1, le=5000)
    expression: Optional[str] = Field(default=None, max_length=512)
    selected_symbol_count: Optional[int] = Field(default=None, ge=0)
    matched_symbol_count: Optional[int] = Field(default=None, ge=0)


class PurgeSymbolsBatchRequest(BaseModel):
    symbols: List[str] = Field(..., min_length=1)
    confirm: bool = False
    scope_note: Optional[str] = None
    dry_run: bool = False
    audit_rule: Optional[PurgeRuleAuditRequest] = None


class PurgeRuleCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    layer: str = Field(..., min_length=1, max_length=32)
    domain: str = Field(..., min_length=1, max_length=64)
    column_name: str = Field(..., min_length=1, max_length=128)
    operator: str = Field(..., min_length=1, max_length=24)
    threshold: float
    run_interval_minutes: int = Field(..., ge=1)
    enabled: bool = True


class PurgeRuleUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=100)
    layer: Optional[str] = Field(default=None, min_length=1, max_length=32)
    domain: Optional[str] = Field(default=None, min_length=1, max_length=64)
    column_name: Optional[str] = Field(default=None, min_length=1, max_length=128)
    operator: Optional[str] = Field(default=None, min_length=1, max_length=24)
    threshold: Optional[float] = None
    run_interval_minutes: Optional[int] = Field(default=None, ge=1)
    enabled: Optional[bool] = None


class PurgeRulePreviewRequest(BaseModel):
    max_symbols: int = Field(default=200, ge=1, le=1000)


def _require_postgres_dsn(request: Request) -> str:
    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")
    return dsn


def _rule_normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").strip().lower())


def _serialize_purge_rule(rule: PurgeRule) -> Dict[str, Any]:
    return {
        "id": rule.id,
        "name": rule.name,
        "layer": rule.layer,
        "domain": rule.domain,
        "columnName": rule.column_name,
        "operator": rule.operator,
        "threshold": rule.threshold,
        "runIntervalMinutes": rule.run_interval_minutes,
        "enabled": rule.enabled,
        "nextRunAt": _iso(rule.next_run_at),
        "lastRunAt": _iso(rule.last_run_at),
        "lastStatus": rule.last_status,
        "lastError": rule.last_error,
        "lastMatchCount": rule.last_match_count,
        "lastPurgeCount": rule.last_purge_count,
        "createdAt": _iso(rule.created_at),
        "updatedAt": _iso(rule.updated_at),
        "createdBy": rule.created_by,
        "updatedBy": rule.updated_by,
    }


def _resolve_purge_rule_table(layer: str, domain: str) -> tuple[str, str]:
    prefix = _RULE_DATA_PREFIXES.get(layer, {}).get(domain)
    if not prefix:
        raise HTTPException(status_code=400, detail=f"Unsupported purge layer/domain: {layer}/{domain}.")
    container = _resolve_container(layer)
    return container, prefix


def _discover_delta_tables_for_prefix(*, container: str, prefix: str) -> List[str]:
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    normalized = f"{str(prefix or '').strip().strip('/')}/"
    if normalized == "/":
        return []
    roots: set[str] = set()
    for blob_name in client.list_files(name_starts_with=normalized):
        text = str(blob_name or "")
        marker = "/_delta_log/"
        if marker not in text:
            continue
        root = text.split(marker, 1)[0].strip("/")
        if root and root.startswith(normalized.rstrip("/")):
            roots.add(root)
    return sorted(roots)


def _load_rule_frame(layer: str, domain: str) -> pd.DataFrame:
    container, prefix = _resolve_purge_rule_table(layer, domain)
    table_paths = _discover_delta_tables_for_prefix(container=container, prefix=prefix)
    if not table_paths:
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    worker_count = _resolve_purge_preview_load_workers(len(table_paths))
    loaded_by_path: Dict[str, pd.DataFrame] = {}
    if worker_count <= 1:
        for table_path in table_paths:
            try:
                df = load_delta(container=container, path=table_path)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            loaded_by_path[table_path] = df
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-preview-load") as executor:
            future_to_path = {
                executor.submit(load_delta, container=container, path=table_path): table_path for table_path in table_paths
            }
            for future in as_completed(future_to_path):
                table_path = future_to_path[future]
                try:
                    df = future.result()
                except Exception:
                    continue
                if df is None or df.empty:
                    continue
                loaded_by_path[table_path] = df

    # Preserve deterministic ordering regardless of parallel completion order.
    for table_path in table_paths:
        df = loaded_by_path.get(table_path)
        if df is None or df.empty:
            continue
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _resolve_rule_symbol_column(df: pd.DataFrame) -> str:
    for column in df.columns:
        if _rule_normalize_column_name(column) in {"symbol", "ticker"}:
            return str(column)
    raise HTTPException(status_code=400, detail="Dataset does not contain symbol/ticker column.")


def _resolve_rule_value_column(df: pd.DataFrame, raw_column_name: str) -> str:
    target = _rule_normalize_column_name(raw_column_name)
    for column in df.columns:
        if _rule_normalize_column_name(column) == target:
            return str(column)
    raise HTTPException(
        status_code=400,
        detail=f"Column '{raw_column_name}' does not exist in the selected dataset.",
    )


def _resolve_rule_date_column(df: pd.DataFrame) -> Optional[str]:
    candidates = ["date", "obsdate", "obs_date", "timestamp", "datetime", "asof", "as_of_date", "tradingdate"]
    normalized_to_name: Dict[str, str] = {_rule_normalize_column_name(column): str(column) for column in df.columns}
    for candidate in candidates:
        column = normalized_to_name.get(_rule_normalize_column_name(candidate))
        if column:
            return column
    return None


def _collect_rule_symbol_values(rule: PurgeRule) -> List[tuple[str, float]]:
    layer = _rule_normalize_column_name(rule.layer)
    domain = rule.domain
    operator = rule.operator
    df = _load_rule_frame(layer, domain)

    if df is None or df.empty:
        return []

    symbol_column = _resolve_rule_symbol_column(df)
    value_column = _resolve_rule_value_column(df, rule.column_name)
    normalized_values = pd.to_numeric(df[value_column], errors="coerce")
    symbols = df[symbol_column].astype("string").str.upper().str.strip()

    work = pd.DataFrame(
        {
            "symbol": symbols,
            "value": normalized_values,
        }
    )
    work = work.dropna(subset=["symbol", "value"]).copy()
    if work.empty:
        return []

    date_column = _resolve_rule_date_column(df)
    if date_column:
        work["date"] = pd.to_datetime(df[date_column], errors="coerce")
        work = work.dropna(subset=["date"]).sort_values("date")
        selected = work.groupby("symbol", as_index=False).tail(1)
    else:
        selected = work.groupby("symbol", as_index=False)["value"].mean()

    selected["value"] = pd.to_numeric(selected["value"], errors="coerce")
    selected = selected.dropna(subset=["value"])
    if selected.empty:
        return []

    symbol_values = {
        str(row["symbol"]): float(row["value"])
        for _, row in selected.iterrows()
        if str(row["symbol"]).strip()
    }
    if not symbol_values:
        return []

    if is_percent_operator(operator):
        percentile = rule.threshold
        values = pd.Series(list(symbol_values.values()), dtype=float)
        if values.empty:
            return []
        if operator == "bottom_percent":
            cutoff = values.quantile(percentile / 100.0)
            return [
                (symbol, value)
                for symbol, value in symbol_values.items()
                if value <= cutoff
            ]
        cutoff = values.quantile(1.0 - (percentile / 100.0))
        return [
            (symbol, value)
            for symbol, value in symbol_values.items()
            if value >= cutoff
        ]

    ops: Dict[str, Any] = {
        "gt": lambda lhs, rhs: lhs > rhs,
        "gte": lambda lhs, rhs: lhs >= rhs,
        "lt": lambda lhs, rhs: lhs < rhs,
        "lte": lambda lhs, rhs: lhs <= rhs,
        "eq": lambda lhs, rhs: lhs == rhs,
        "ne": lambda lhs, rhs: lhs != rhs,
    }
    comparator = ops.get(operator)
    if comparator is None:
        raise HTTPException(status_code=400, detail=f"Unsupported operator '{operator}'.")

    return [
        (symbol, value)
        for symbol, value in symbol_values.items()
        if comparator(value, float(rule.threshold))
    ]


_CANDIDATE_AGGREGATION_ALIASES: Dict[str, str] = {
    "average": "avg",
    "mean": "avg",
    "std": "stddev",
    "stdev": "stddev",
    "std_dev": "stddev",
    "standard_deviation": "stddev",
}
_SUPPORTED_CANDIDATE_AGGREGATIONS = {"min", "max", "avg", "stddev"}


def _normalize_candidate_aggregation(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    resolved = _CANDIDATE_AGGREGATION_ALIASES.get(normalized, normalized)
    if resolved not in _SUPPORTED_CANDIDATE_AGGREGATIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported aggregation '{value}'. Supported: avg, min, max, stddev.",
        )
    return resolved


def _aggregate_series(values: pd.Series, aggregation: str) -> float:
    if aggregation == "min":
        return float(values.min())
    if aggregation == "max":
        return float(values.max())
    if aggregation == "stddev":
        # Use population stddev so a single-row window is deterministic (0.0).
        return float(values.std(ddof=0))
    return float(values.mean())


def _collect_purge_candidates(
    layer: str,
    domain: str,
    column: str,
    operator: str,
    raw_value: float,
    as_of: Optional[str] = None,
    min_rows: int = 1,
    recent_rows: int = 1,
    aggregation: str = "avg",
    limit: Optional[int] = None,
    offset: int = 0,
) -> tuple[List[Dict[str, Any]], int, int, int]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    if not normalized_layer or not normalized_domain:
        raise HTTPException(status_code=400, detail="layer and domain are required.")

    operator = normalize_purge_rule_operator(operator)
    threshold = float(raw_value)
    if not pd.notna(threshold) or not pd.api.types.is_number(threshold):
        raise HTTPException(status_code=400, detail="value must be a finite number.")
    recent_rows = int(recent_rows)
    if recent_rows < 1:
        raise HTTPException(status_code=400, detail="recent_rows must be >= 1.")
    resolved_aggregation = _normalize_candidate_aggregation(aggregation)

    df = _load_rule_frame(normalized_layer, normalized_domain)

    if df is None or df.empty:
        return [], 0, 0, 0

    symbol_column = _resolve_rule_symbol_column(df)
    value_column = _resolve_rule_value_column(df, column)
    rows = pd.to_numeric(df[value_column], errors="coerce")
    work = pd.DataFrame(
        {
            "symbol": df[symbol_column].astype("string").str.upper().str.strip(),
            "value": rows,
        }
    )

    date_column = _resolve_rule_date_column(df)
    if date_column:
        work["asOf"] = pd.to_datetime(df[date_column], errors="coerce")
        work = work.dropna(subset=["symbol", "value", "asOf"]).copy()
        if as_of:
            as_of_dt = pd.to_datetime(as_of, errors="coerce")
            if pd.isna(as_of_dt):
                raise HTTPException(status_code=400, detail=f"Invalid as_of value '{as_of}'.")
            work = work.loc[work["asOf"] <= as_of_dt]

        if work.empty:
            return [], 0, 0, 0

        work = work.sort_values(["symbol", "asOf"]).reset_index(drop=True)
        windowed = work.groupby("symbol", as_index=False, group_keys=False).tail(recent_rows)
        rows_per_symbol = windowed.groupby("symbol", as_index=False).size().rename(columns={"size": "rowsContributing"})
        latest = (
            windowed.groupby("symbol", as_index=False)
            .agg(
                value=("value", lambda series: _aggregate_series(series.astype(float), resolved_aggregation)),
                asOf=("asOf", "max"),
            )
            .merge(rows_per_symbol, on="symbol", how="left")
        )
    else:
        work = work.dropna(subset=["symbol", "value"]).copy()
        if work.empty:
            return [], 0, 0, 0

        windowed = work.groupby("symbol", as_index=False, group_keys=False).tail(recent_rows)
        latest = windowed.groupby("symbol", as_index=False).agg(
            value=("value", lambda series: _aggregate_series(series.astype(float), resolved_aggregation)),
            rowsContributing=("value", "size"),
        )
        latest["asOf"] = None

    latest["value"] = pd.to_numeric(latest["value"], errors="coerce")
    latest = latest.dropna(subset=["symbol", "value"])
    if latest.empty:
        return [], len(df), 0, 0

    if is_percent_operator(operator):
        if not (1 <= threshold <= 100):
            raise HTTPException(status_code=400, detail="Percent threshold must be between 1 and 100.")
        values = latest["value"].astype(float)
        if values.empty:
            return [], len(df), 0, 0

        if operator == "bottom_percent":
            cutoff = float(values.quantile(threshold / 100.0))
            latest = latest.loc[latest["value"] <= cutoff]
        else:
            cutoff = float(values.quantile(1.0 - (threshold / 100.0)))
            latest = latest.loc[latest["value"] >= cutoff]
    else:
        ops: Dict[str, Any] = {
            "gt": lambda lhs, rhs: lhs > rhs,
            "gte": lambda lhs, rhs: lhs >= rhs,
            "lt": lambda lhs, rhs: lhs < rhs,
            "lte": lambda lhs, rhs: lhs <= rhs,
            "eq": lambda lhs, rhs: lhs == rhs,
            "ne": lambda lhs, rhs: lhs != rhs,
        }
        comparator = ops.get(operator)
        if comparator is None:
            raise HTTPException(status_code=400, detail=f"Unsupported operator '{operator}'.")
        latest = latest.loc[latest.apply(lambda row: bool(comparator(float(row["value"]), threshold)), axis=1)]

    if latest.empty:
        return [], len(df), 0, 0

    latest = latest.loc[latest["rowsContributing"] >= int(min_rows)]
    if latest.empty:
        return [], len(df), 0, 0

    latest["rowsContributing"] = pd.to_numeric(latest["rowsContributing"], errors="coerce").fillna(0).astype(int)
    latest = latest.sort_values("value", ascending=False).reset_index(drop=True)

    matched_value_total = int(latest["rowsContributing"].sum()) if "rowsContributing" in latest else 0
    total = int(len(latest))
    if limit is None:
        window = latest.iloc[offset:]
    else:
        window = latest.iloc[offset : offset + int(limit)]

    matches: List[Dict[str, Any]] = []
    for _, row in window.iterrows():
        matched_value = row["value"]
        as_of_value = row.get("asOf")
        matches.append(
            {
                "symbol": str(row["symbol"]),
                "matchedValue": float(matched_value),
                "rowsContributing": int(row["rowsContributing"]),
                "latestAsOf": _iso(as_of_value.to_pydatetime()) if pd.notna(as_of_value) else None,
            }
        )

    return matches, len(df), total, matched_value_total


def _build_purge_candidates_response(
    *,
    layer: str,
    domain: str,
    column: str,
    operator: str,
    value: Optional[float],
    percentile: Optional[float],
    as_of: Optional[str],
    recent_rows: int,
    aggregation: str,
    limit: Optional[int],
    offset: int,
    min_rows: int,
) -> Dict[str, Any]:
    normalized_layer = _normalize_layer(layer)
    normalized_domain = _normalize_domain(domain)
    resolved_column = str(column or "").strip()
    if not normalized_layer:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not normalized_domain:
        raise HTTPException(status_code=400, detail="domain is required.")
    if not resolved_column:
        raise HTTPException(status_code=400, detail="column is required.")

    normalized_operator = normalize_purge_rule_operator(operator)
    normalized_aggregation = _normalize_candidate_aggregation(aggregation)
    raw_value = percentile if is_percent_operator(normalized_operator) else value
    if raw_value is None:
        raise HTTPException(
            status_code=400,
            detail="value is required for numeric operators; percentile is required for top/bottom percent operators.",
        )
    if is_percent_operator(normalized_operator) and percentile is None:
        raw_value = value
        if raw_value is None:
            raise HTTPException(status_code=400, detail="percentile is required for percent operators.")

    candidate_layer = "silver" if normalized_layer == "bronze" else normalized_layer
    matches, total_rows, matched, contrib = _collect_purge_candidates(
        layer=candidate_layer,
        domain=normalized_domain,
        column=resolved_column,
        operator=normalized_operator,
        raw_value=float(raw_value),
        as_of=as_of,
        min_rows=min_rows,
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
        limit=limit,
        offset=offset,
    )

    criteria = {
        "requestedLayer": normalized_layer,
        "resolvedLayer": candidate_layer,
        "domain": normalized_domain,
        "column": resolved_column,
        "operator": normalized_operator,
        "value": float(raw_value),
        "asOf": as_of,
        "minRows": min_rows,
        "recentRows": recent_rows,
        "aggregation": normalized_aggregation,
    }
    expression = _build_purge_expression(
        resolved_column,
        normalized_operator,
        float(raw_value),
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
    )
    return {
        "criteria": criteria,
        "expression": expression,
        "summary": {
            "totalRowsScanned": total_rows,
            "symbolsMatched": matched,
            "rowsContributing": contrib,
            "estimatedDeletionTargets": matched,
        },
        "symbols": matches,
        "offset": offset,
        "limit": limit if limit is not None else len(matches),
        "total": matched,
        "hasMore": bool(limit is not None and (offset + len(matches) < matched)),
        "note": (
            "Bronze preview uses silver dataset for ranking; bronze-wide criteria are supported for runtime purge targets only."
            if normalized_layer == "bronze"
            else None
        ),
    }


def _build_purge_expression(
    column: str,
    operator: str,
    value: float,
    *,
    recent_rows: int = 1,
    aggregation: str = "avg",
) -> str:
    operator = normalize_purge_rule_operator(operator)
    display_value = float(value)
    resolved_aggregation = _normalize_candidate_aggregation(aggregation)
    metric = (
        str(column)
        if int(recent_rows) == 1 and resolved_aggregation == "avg"
        else f"{resolved_aggregation}({column}) over last {int(recent_rows)} rows"
    )
    if operator == "gt":
        return f"{metric} > {display_value:g}"
    if operator == "gte":
        return f"{metric} >= {display_value:g}"
    if operator == "lt":
        return f"{metric} < {display_value:g}"
    if operator == "lte":
        return f"{metric} <= {display_value:g}"
    if operator == "eq":
        return f"{metric} == {display_value:g}"
    if operator == "ne":
        return f"{metric} != {display_value:g}"
    if operator == "top_percent":
        return f"top {display_value:g}% by {metric}"
    if operator == "bottom_percent":
        return f"bottom {display_value:g}% by {metric}"
    return f"{metric} {operator} {display_value:g}"


def _persist_purge_symbols_audit_rule(
    *,
    dsn: str,
    audit_rule: PurgeRuleAuditRequest,
    actor: Optional[str],
) -> PurgeRule:
    normalized_layer = _normalize_layer(audit_rule.layer)
    normalized_domain = _normalize_domain(audit_rule.domain)
    if not normalized_layer or not normalized_domain:
        raise HTTPException(status_code=400, detail="audit_rule.layer and audit_rule.domain are required.")

    resolved_column = str(audit_rule.column_name or "").strip()
    if not resolved_column:
        raise HTTPException(status_code=400, detail="audit_rule.column_name is required.")

    normalized_operator = normalize_purge_rule_operator(audit_rule.operator)
    threshold = float(audit_rule.threshold)
    if not pd.notna(threshold) or threshold in {float("inf"), float("-inf")}:
        raise HTTPException(status_code=400, detail="audit_rule.threshold must be a finite number.")
    if is_percent_operator(normalized_operator) and not (0 <= threshold <= 100):
        raise HTTPException(
            status_code=400,
            detail="audit_rule.threshold must be between 0 and 100 for percentile operators.",
        )

    recent_rows = int(audit_rule.recent_rows or 1)
    normalized_aggregation = _normalize_candidate_aggregation(audit_rule.aggregation or "avg")
    expression = str(audit_rule.expression or "").strip() or _build_purge_expression(
        resolved_column,
        normalized_operator,
        threshold,
        recent_rows=recent_rows,
        aggregation=normalized_aggregation,
    )

    details: List[str] = []
    if audit_rule.matched_symbol_count is not None:
        details.append(f"matched={int(audit_rule.matched_symbol_count)}")
    if audit_rule.selected_symbol_count is not None:
        details.append(f"selected={int(audit_rule.selected_symbol_count)}")
    detail_suffix = f" ({', '.join(details)})" if details else ""
    audit_name = f"audit {normalized_layer}/{normalized_domain}: {expression}{detail_suffix}"

    try:
        return create_purge_rule(
            dsn=dsn,
            name=audit_name,
            layer=normalized_layer,
            domain=normalized_domain,
            column_name=resolved_column,
            operator=normalized_operator,
            threshold=threshold,
            run_interval_minutes=_PURGE_RULE_AUDIT_INTERVAL_MINUTES,
            enabled=False,
            actor=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid audit_rule payload: {exc}") from exc
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to persist audit purge rule: {exc}") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Failed to persist audit purge rule: layer=%s domain=%s column=%s operator=%s",
            normalized_layer,
            normalized_domain,
            resolved_column,
            normalized_operator,
        )
        raise HTTPException(status_code=500, detail=f"Failed to persist audit purge rule: {exc}") from exc


def _normalize_candidate_symbols(symbols: List[str]) -> List[str]:
    seen = set()
    normalized: List[str] = []
    for symbol in symbols:
        normalized_symbol = _normalize_purge_symbol(symbol)
        if normalized_symbol in seen:
            continue
        seen.add(normalized_symbol)
        normalized.append(normalized_symbol)
    if not normalized:
        raise HTTPException(status_code=400, detail="At least one unique symbol is required.")
    return normalized


def _resolve_purge_symbol_workers(symbol_count: int) -> int:
    if symbol_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_MAX_WORKERS, symbol_count)
    raw = str(os.environ.get("PURGE_SYMBOL_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_MAX_WORKERS))
    return min(symbol_count, bounded)


def _resolve_purge_preview_load_workers(table_count: int) -> int:
    if table_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_PREVIEW_LOAD_MAX_WORKERS, table_count)
    raw = str(os.environ.get("PURGE_PREVIEW_LOAD_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS))
    return min(table_count, bounded)


def _resolve_purge_scope_workers(target_count: int) -> int:
    if target_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SCOPE_MAX_WORKERS, target_count)
    raw = str(os.environ.get("PURGE_SCOPE_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SCOPE_MAX_WORKERS))
    return min(target_count, bounded)


def _resolve_purge_symbol_target_workers(target_count: int) -> int:
    if target_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_TARGET_MAX_WORKERS, target_count)
    raw = str(os.environ.get("PURGE_SYMBOL_TARGET_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS))
    return min(target_count, bounded)


def _resolve_purge_symbol_layer_workers(layer_count: int) -> int:
    if layer_count <= 0:
        return 1
    default_workers = min(_DEFAULT_PURGE_SYMBOL_LAYER_MAX_WORKERS, layer_count)
    raw = str(os.environ.get("PURGE_SYMBOL_LAYER_MAX_WORKERS") or "").strip()
    if not raw:
        return default_workers
    try:
        requested = int(raw)
    except Exception:
        return default_workers
    bounded = max(1, min(requested, _MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS))
    return min(layer_count, bounded)


def _run_symbol_cleanup_tasks(
    tasks: List[Tuple[Dict[str, Any], Callable[[], int]]], *, worker_count: int, thread_name_prefix: str
) -> List[Dict[str, Any]]:
    if not tasks:
        return []

    results_by_index: Dict[int, Dict[str, Any]] = {}
    if worker_count <= 1:
        for idx, (base, work) in enumerate(tasks):
            deleted = int(work())
            item = dict(base)
            item["deleted"] = deleted
            results_by_index[idx] = item
    else:
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=thread_name_prefix) as executor:
            future_to_index: Dict[Any, Tuple[int, Dict[str, Any]]] = {
                executor.submit(work): (idx, base) for idx, (base, work) in enumerate(tasks)
            }
            for future in as_completed(future_to_index):
                idx, base = future_to_index[future]
                deleted = int(future.result())
                item = dict(base)
                item["deleted"] = deleted
                results_by_index[idx] = item

    return [results_by_index[idx] for idx in range(len(tasks))]


def _build_purge_symbols_summary(
    *,
    symbols: List[str],
    scope_note: Optional[str],
    dry_run: bool,
    succeeded: int,
    failed: int,
    skipped: int,
    total_deleted: int,
    symbol_results: List[Dict[str, Any]],
    in_progress: int = 0,
) -> Dict[str, Any]:
    requested = len(symbols)
    completed = int(succeeded) + int(failed) + int(skipped)
    pending = max(0, requested - completed - max(0, int(in_progress)))
    progress_pct = float((completed / requested) * 100.0) if requested > 0 else 100.0
    return {
        "scope": "symbols",
        "dryRun": bool(dry_run),
        "scopeNote": scope_note,
        "requestedSymbols": symbols,
        "requestedSymbolCount": requested,
        "completed": completed,
        "pending": pending,
        "inProgress": max(0, int(in_progress)),
        "progressPct": round(progress_pct, 2),
        "succeeded": int(succeeded),
        "failed": int(failed),
        "skipped": int(skipped),
        "totalDeleted": int(total_deleted),
        "symbolResults": list(symbol_results),
    }


def _create_purge_symbols_operation(
    symbols: List[str],
    actor: Optional[str],
    *,
    scope_note: Optional[str],
    dry_run: bool,
    audit_rule_id: Optional[int] = None,
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    initial_summary = _build_purge_symbols_summary(
        symbols=symbols,
        scope_note=scope_note,
        dry_run=bool(dry_run),
        succeeded=0,
        failed=0,
        skipped=0,
        total_deleted=0,
        symbol_results=[],
        in_progress=0,
    )
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "symbols",
            "requestedBy": actor,
            "symbols": symbols,
            "symbolCount": len(symbols),
            "scopeNote": scope_note,
            "dryRun": bool(dry_run),
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": initial_summary,
            "error": None,
            "auditRuleId": int(audit_rule_id) if audit_rule_id else None,
        }
    return operation_id


def _execute_purge_symbols_operation(
    operation_id: str,
    symbols: List[str],
    *,
    dry_run: bool,
    scope_note: Optional[str],
) -> None:
    symbol_results: List[Dict[str, Any]] = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_deleted = 0

    def _publish_progress(*, in_progress: int) -> None:
        summary = _build_purge_symbols_summary(
            symbols=symbols,
            scope_note=scope_note,
            dry_run=bool(dry_run),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            total_deleted=total_deleted,
            symbol_results=symbol_results,
            in_progress=in_progress,
        )
        _update_purge_operation(
            operation_id,
            {"status": "running", "result": summary},
        )

    _publish_progress(in_progress=0)

    if dry_run:
        for index, symbol in enumerate(symbols, start=1):
            symbol_results.append(
                {
                    "symbol": symbol,
                    "status": "skipped",
                    "deleted": 0,
                    "dryRun": True,
                }
            )
            skipped += 1
            _publish_progress(in_progress=0)
            logger.info(
                "Purge-symbols dry-run progress: operation=%s completed=%s/%s",
                operation_id,
                index,
                len(symbols),
            )
    else:
        worker_count = _resolve_purge_symbol_workers(len(symbols))
        logger.info(
            "Purge-symbols operation started: operation=%s symbols=%s workers=%s",
            operation_id,
            len(symbols),
            worker_count,
        )
        in_progress = len(symbols)
        _publish_progress(in_progress=in_progress)
        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-symbols") as executor:
            future_to_symbol = {
                executor.submit(
                    _run_purge_symbol_operation,
                    PurgeSymbolRequest(symbol=symbol, confirm=True),
                ): symbol
                for symbol in symbols
            }
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                in_progress = max(0, in_progress - 1)
                try:
                    result = future.result()
                    deleted = int(result.get("totalDeleted") or 0)
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "succeeded",
                            "deleted": deleted,
                            "targets": result.get("targets") or [],
                        }
                    )
                    total_deleted += deleted
                    succeeded += 1
                except HTTPException as exc:
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "failed",
                            "deleted": 0,
                            "error": str(exc.detail),
                        }
                    )
                    failed += 1
                except Exception as exc:
                    symbol_results.append(
                        {
                            "symbol": symbol,
                            "status": "failed",
                            "deleted": 0,
                            "error": f"{type(exc).__name__}: {exc}",
                        }
                    )
                    failed += 1
                _publish_progress(in_progress=in_progress)
                logger.info(
                    "Purge-symbols progress: operation=%s completed=%s/%s succeeded=%s failed=%s in_progress=%s",
                    operation_id,
                    len(symbol_results),
                    len(symbols),
                    succeeded,
                    failed,
                    in_progress,
                )

    summary = _build_purge_symbols_summary(
        symbols=symbols,
        scope_note=scope_note,
        dry_run=bool(dry_run),
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        total_deleted=total_deleted,
        symbol_results=symbol_results,
        in_progress=0,
    )
    status = "failed" if failed > 0 else "succeeded"

    logger.info(
        "Purge-symbols operation finished: operation=%s symbols=%s succeeded=%s failed=%s skipped=%s dry_run=%s",
        operation_id,
        len(symbols),
        succeeded,
        failed,
        skipped,
        bool(dry_run),
    )

    if status == "succeeded":
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": summary, "completedAt": _utc_timestamp()},
        )
    else:
        operation_error = "One or more symbols failed."
        _update_purge_operation(
            operation_id,
            {"status": "failed", "result": summary, "error": operation_error, "completedAt": _utc_timestamp()},
        )


def _execute_purge_rule(rule: PurgeRule, *, actor: Optional[str]) -> Dict[str, Any]:
    symbol_values = _collect_rule_symbol_values(rule)
    matches = sorted(symbol_values, key=lambda item: str(item[0]))
    matched_symbols = [symbol for symbol, _ in matches]
    matched_count = len(matched_symbols)
    purged_count = 0
    failed: List[str] = []
    if not matched_symbols:
        return {
            "ruleId": rule.id,
            "ruleName": rule.name,
            "matchedCount": matched_count,
            "purgedCount": purged_count,
            "symbols": [],
            "failedSymbols": [],
        }

    for symbol, metric in matches:
        try:
            payload = PurgeSymbolRequest(symbol=symbol, confirm=True)
            result = _run_purge_symbol_operation(payload)
            purged_count += int(result.get("totalDeleted") or 0)
        except HTTPException as exc:
            failed.append(f"{symbol}: {exc.detail}")
        except Exception as exc:
            failed.append(f"{symbol}: {type(exc).__name__}: {exc}")

    status = "failed" if failed else "succeeded"
    logger.info(
        "Purge rule executed: id=%s name=%s actor=%s matched=%s purged=%s status=%s",
        rule.id,
        rule.name,
        actor or "-",
        matched_count,
        purged_count,
        status,
    )
    return {
        "ruleId": rule.id,
        "ruleName": rule.name,
        "matchedCount": matched_count,
        "purgedCount": purged_count,
        "symbols": matched_symbols,
        "failedSymbols": failed,
    }
_FINANCE_BRONZE_TABLE_TYPES: List[Tuple[str, str]] = [
    # Bronze finance raw files are written to title-cased folders with spaces.
    # Keep these names aligned with tasks/finance_data/bronze_finance_data.py::REPORTS.
    ("Balance Sheet", "quarterly_balance-sheet"),
    ("Income Statement", "quarterly_financials"),
    ("Cash Flow", "quarterly_cash-flow"),
    ("Valuation", "quarterly_valuation_measures"),
]

_FINANCE_BRONZE_FOLDER_ALIASES: Dict[str, Tuple[str, ...]] = {
    "Balance Sheet": ("Balance Sheet", "balance_sheet"),
    "Income Statement": ("Income Statement", "income_statement"),
    "Cash Flow": ("Cash Flow", "cash_flow"),
    "Valuation": ("Valuation", "valuation"),
}

_RULE_DATA_PREFIXES: Dict[str, Dict[str, str]] = {
    "silver": {
        "market": "market-data/",
        "finance": "finance-data/",
        "earnings": "earnings-data/",
        "price-target": "price-target-data/",
    },
    "gold": {
        "market": "market/",
        "finance": "finance/",
        "earnings": "earnings/",
        "price-target": "targets/",
    },
}


def _normalize_purge_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="symbol is required.")
    return normalized


def _market_symbol(symbol: str) -> str:
    return _normalize_purge_symbol(symbol).replace(".", "-")


def _symbol_variants(symbol: str) -> List[str]:
    normalized = _normalize_purge_symbol(symbol)
    market_symbol = normalized.replace(".", "-")
    variants = [normalized]
    if market_symbol != normalized:
        variants.append(market_symbol)
    return variants


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _create_purge_operation(
    payload: PurgeRequest,
    actor: Optional[str],
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": payload.scope,
            "layer": payload.layer,
            "domain": payload.domain,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _create_purge_candidates_operation(payload: PurgeCandidatesRequest, actor: Optional[str]) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "candidate-preview",
            "layer": payload.layer,
            "domain": payload.domain,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _execute_purge_candidates_operation(operation_id: str, payload: PurgeCandidatesRequest) -> None:
    started = datetime.now(timezone.utc)
    try:
        result = _build_purge_candidates_response(
            layer=payload.layer,
            domain=payload.domain,
            column=payload.column,
            operator=payload.operator,
            value=payload.value,
            percentile=payload.percentile,
            as_of=payload.as_of,
            recent_rows=payload.recent_rows,
            aggregation=payload.aggregation,
            limit=payload.limit,
            offset=payload.offset,
            min_rows=payload.min_rows,
        )
        duration_ms = max(0, int((datetime.now(timezone.utc) - started).total_seconds() * 1000))
        summary = result.get("summary") if isinstance(result, dict) else {}
        logger.info(
            "Purge-candidates operation succeeded: operation=%s layer=%s domain=%s durationMs=%s totalRowsScanned=%s symbolsMatched=%s",
            operation_id,
            payload.layer,
            payload.domain,
            duration_ms,
            (summary or {}).get("totalRowsScanned"),
            (summary or {}).get("symbolsMatched"),
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "succeeded",
                "completedAt": _utc_timestamp(),
                "result": result,
                "error": None,
            },
        )
    except HTTPException as exc:
        detail = str(exc.detail) if exc.detail is not None else "Purge candidates failed."
        logger.warning(
            "Purge-candidates operation failed: operation=%s layer=%s domain=%s detail=%s",
            operation_id,
            payload.layer,
            payload.domain,
            detail,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "completedAt": _utc_timestamp(),
                "error": detail,
            },
        )
    except Exception as exc:
        logger.exception(
            "Purge-candidates operation failed: operation=%s layer=%s domain=%s",
            operation_id,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "completedAt": _utc_timestamp(),
                "error": f"{type(exc).__name__}: {exc}",
            },
        )


def _get_purge_operation(operation_id: str) -> Optional[Dict[str, Any]]:
    with _PURGE_OPERATIONS_LOCK:
        operation = _PURGE_OPERATIONS.get(operation_id)
        return dict(operation) if operation else None


def _update_purge_operation(operation_id: str, patch: Dict[str, Any]) -> bool:
    with _PURGE_OPERATIONS_LOCK:
        operation = _PURGE_OPERATIONS.get(operation_id)
        if not operation:
            return False
        operation.update(patch)
        operation["updatedAt"] = _utc_timestamp()
        return True


def _normalize_layer(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value).strip().lower()


def _normalize_domain(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = str(value).strip().lower().replace("_", "-").replace(" ", "-")
    if cleaned == "targets":
        return "price-target"
    return cleaned


_LAYER_CONTAINER_ENV = {
    "bronze": "AZURE_CONTAINER_BRONZE",
    "silver": "AZURE_CONTAINER_SILVER",
    "gold": "AZURE_CONTAINER_GOLD",
    "platinum": "AZURE_CONTAINER_PLATINUM",
}

_DOMAIN_PREFIXES: Dict[str, Dict[str, List[str]]] = {
    "bronze": {
        "market": ["market-data/"],
        "finance": ["finance-data/"],
        "earnings": ["earnings-data/"],
        "price-target": ["price-target-data/"],
    },
    "silver": {
        "market": ["market-data/"],
        "finance": ["finance-data/"],
        "earnings": ["earnings-data/"],
        "price-target": ["price-target-data/"],
    },
    "gold": {
        "market": ["market/"],
        "finance": ["finance/"],
        "earnings": ["earnings/"],
        "price-target": ["targets/"],
    },
    "platinum": {
        "platinum": ["platinum/"],
    },
}

_SILVER_JOB_CHECKPOINT_KEYS: Dict[str, Tuple[str, str]] = {
    "market": ("bronze_market_data", "silver_market_data"),
    "finance": ("bronze_finance_data", "silver_finance_data"),
    "earnings": ("bronze_earnings_data", "silver_earnings_data"),
    "price-target": ("bronze_price_target_data", "silver_price_target_data"),
}

_GOLD_JOB_WATERMARK_KEYS: Dict[str, str] = {
    "market": "gold_market_features",
    "finance": "gold_finance_features",
    "earnings": "gold_earnings_features",
    "price-target": "gold_price_target_features",
}


def _resolve_container(layer: str) -> str:
    env_key = _LAYER_CONTAINER_ENV.get(layer)
    if not env_key:
        raise HTTPException(status_code=400, detail=f"Unknown layer '{layer}'.")
    container = os.environ.get(env_key, "").strip()
    if not container:
        raise HTTPException(status_code=503, detail=f"Missing {env_key} for purge.")
    return container


def _targets_for_layer_domain(layer: str, domain: str) -> List[Tuple[str, str]]:
    prefixes = _DOMAIN_PREFIXES.get(layer, {}).get(domain, [])
    if not prefixes:
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain}' for layer '{layer}'.")
    container = _resolve_container(layer)
    return [(container, prefix) for prefix in prefixes]


def _delete_blob_if_exists(client: BlobStorageClient, path: str) -> int:
    if client.file_exists(path):
        client.delete_file(path)
        return 1
    return 0


def _delete_prefix_if_exists(client: BlobStorageClient, path: str) -> int:
    return int(client.delete_prefix(path))


def _bronze_blacklist_paths() -> List[str]:
    earnings_prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"
    return [
        "market-data/blacklist.csv",
        "finance-data/blacklist.csv",
        f"{earnings_prefix}/blacklist.csv",
        "price-target-data/blacklist.csv",
    ]


def _resolve_domain_list_paths(layer: str, domain: str) -> List[Dict[str, str]]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    prefixes = _DOMAIN_PREFIXES.get(layer_norm, {}).get(domain_norm, [])
    if not prefixes:
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain_norm}' for layer '{layer_norm}'.")

    paths: List[Dict[str, str]] = []
    seen: set[Tuple[str, str]] = set()
    for prefix in prefixes:
        base = str(prefix or "").strip().strip("/")
        if not base:
            continue
        for list_type in ("whitelist", "blacklist"):
            path = f"{base}/{list_type}.csv"
            dedupe_key = (list_type, path)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            paths.append({"listType": list_type, "path": path})

    if not paths:
        raise HTTPException(
            status_code=400,
            detail=f"No blacklist/whitelist list paths are configured for layer '{layer_norm}' domain '{domain_norm}'.",
        )
    return paths


def _load_domain_list_file_preview(
    client: BlobStorageClient,
    *,
    list_type: str,
    path: str,
    limit: int,
) -> Dict[str, Any]:
    exists = bool(client.file_exists(path))
    warning: Optional[str] = None
    symbols: List[str] = []

    if exists:
        try:
            loaded_symbols = mdc.load_ticker_list(path, client=client) or []
            symbols = _normalize_symbol_candidates(loaded_symbols)
        except Exception as exc:
            warning = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "Domain list load failed: container=%s path=%s error=%s",
                client.container_name,
                path,
                warning,
            )

    truncated = len(symbols) > limit
    preview_symbols = symbols[:limit]
    result: Dict[str, Any] = {
        "listType": list_type,
        "path": path,
        "exists": exists,
        "symbolCount": len(symbols),
        "symbols": preview_symbols,
        "truncated": truncated,
    }
    if warning:
        result["warning"] = warning
    return result


def _reset_domain_lists(client: BlobStorageClient, *, layer: str, domain: str) -> Dict[str, Any]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    list_paths = _resolve_domain_list_paths(layer_norm, domain_norm)
    empty_symbols = pd.DataFrame(columns=["Symbol"])
    targets: List[Dict[str, Any]] = []
    for item in list_paths:
        list_type = str(item["listType"]).strip().lower()
        path = str(item["path"]).strip()
        existed = bool(client.file_exists(path))
        try:
            mdc.store_csv(empty_symbols, path, client=client)
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to reset {list_type} list for {layer_norm}/{domain_norm}: {exc}",
            ) from exc
        targets.append({"listType": list_type, "path": path, "status": "reset", "existed": existed})

    return {
        "layer": layer_norm,
        "domain": domain_norm,
        "container": client.container_name,
        "resetCount": len(targets),
        "targets": targets,
        "updatedAt": _utc_timestamp(),
    }


def _reset_domain_checkpoints(*, layer: str, domain: str) -> Dict[str, Any]:
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    if domain_norm not in _DOMAIN_PREFIXES.get(layer_norm, {}):
        raise HTTPException(status_code=400, detail=f"Unknown domain '{domain_norm}' for layer '{layer_norm}'.")

    scope_targets: List[Dict[str, Optional[str]]] = [
        {
            "layer": layer_norm,
            "domain": domain_norm,
            "container": None,
            "prefix": None,
        }
    ]
    raw_targets = [
        *_build_silver_checkpoint_reset_targets(scope_targets),
        *_build_gold_checkpoint_reset_targets(scope_targets),
    ]

    deduped_targets: List[Dict[str, Optional[str]]] = []
    seen: set[Tuple[str, str, str]] = set()
    for target in raw_targets:
        container = str(target.get("container") or "").strip()
        prefix = str(target.get("prefix") or "").strip()
        operation = str(target.get("operation") or "reset-checkpoint").strip() or "reset-checkpoint"
        if not container or not prefix:
            continue
        dedupe_key = (container, prefix, operation)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped_targets.append(
            {
                "container": container,
                "prefix": prefix,
                "operation": operation,
            }
        )

    if not deduped_targets:
        return {
            "layer": layer_norm,
            "domain": domain_norm,
            "container": None,
            "resetCount": 0,
            "deletedCount": 0,
            "targets": [],
            "updatedAt": _utc_timestamp(),
            "note": "No checkpoint gates are configured for this layer/domain.",
        }

    clients: Dict[str, BlobStorageClient] = {}
    results: List[Dict[str, Any]] = []
    deleted_count = 0
    for target in deduped_targets:
        container = str(target["container"])
        prefix = str(target["prefix"])
        operation = str(target["operation"])
        client = clients.get(container)
        if client is None:
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
            clients[container] = client

        try:
            existed = bool(client.file_exists(prefix))
            deleted = False
            if existed:
                client.delete_file(prefix)
                deleted = True
        except Exception as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to reset checkpoint {operation} for {layer_norm}/{domain_norm}: {exc}",
            ) from exc

        results.append(
            {
                "operation": operation,
                "path": prefix,
                "status": "reset",
                "existed": existed,
                "deleted": deleted,
            }
        )
        if deleted:
            deleted_count += 1

    return {
        "layer": layer_norm,
        "domain": domain_norm,
        "container": str(deduped_targets[0]["container"]),
        "resetCount": len(results),
        "deletedCount": deleted_count,
        "targets": results,
        "updatedAt": _utc_timestamp(),
    }


def _normalize_symbol_candidates(symbols: Sequence[Any]) -> List[str]:
    seen: set[str] = set()
    normalized: List[str] = []
    for raw in symbols:
        try:
            symbol = _normalize_purge_symbol(str(raw or ""))
        except HTTPException:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _load_symbols_from_bronze_blacklists(client: BlobStorageClient) -> Dict[str, Any]:
    merged: List[str] = []
    sources: List[Dict[str, Any]] = []
    for path in _bronze_blacklist_paths():
        loaded: Sequence[Any] = []
        warning: Optional[str] = None
        try:
            loaded = mdc.load_ticker_list(path, client=client) or []
        except Exception as exc:
            warning = f"{type(exc).__name__}: {exc}"
            logger.warning("Blacklist load failed: container=%s path=%s error=%s", client.container_name, path, warning)

        normalized = _normalize_symbol_candidates(loaded)
        merged.extend(normalized)
        source_info: Dict[str, Any] = {
            "path": path,
            "symbolCount": len(normalized),
        }
        if warning:
            source_info["warning"] = warning
        sources.append(source_info)

    symbols = _normalize_symbol_candidates(merged)
    return {
        "container": client.container_name,
        "symbolCount": len(symbols),
        "symbols": symbols,
        "sources": sources,
    }


def _append_symbol_to_bronze_blacklists(client: BlobStorageClient, symbol: str) -> Dict[str, Any]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    blacklist_paths = _bronze_blacklist_paths()

    for path in blacklist_paths:
        mdc.update_csv_set(path, normalized_symbol, client=client)

    return {"updated": len(blacklist_paths), "paths": blacklist_paths}


def _remove_symbol_from_bronze_storage(client: BlobStorageClient, symbol: str) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    market_symbol = _market_symbol(normalized_symbol)
    earnings_prefix = getattr(cfg, "EARNINGS_DATA_PREFIX", "earnings-data") or "earnings-data"

    tasks: List[Tuple[Dict[str, Any], Callable[[], int]]] = []

    market_path = f"market-data/{market_symbol}.csv"
    tasks.append(
        (
            {
                "layer": "bronze",
                "domain": "market",
                "container": client.container_name,
                "path": market_path,
            },
            lambda path=market_path: _delete_blob_if_exists(client, path=path),
        )
    )

    for folder, suffix in _FINANCE_BRONZE_TABLE_TYPES:
        folder_candidates = _FINANCE_BRONZE_FOLDER_ALIASES.get(folder, (folder,))
        for folder_candidate in folder_candidates:
            finance_path = f"finance-data/{folder_candidate}/{normalized_symbol}_{suffix}.json"
            tasks.append(
                (
                    {
                        "layer": "bronze",
                        "domain": "finance",
                        "container": client.container_name,
                        "path": finance_path,
                    },
                    lambda path=finance_path: _delete_blob_if_exists(client, path=path),
                )
            )

    earnings_path = f"{earnings_prefix}/{normalized_symbol}.json"
    tasks.append(
        (
            {
                "layer": "bronze",
                "domain": "earnings",
                "container": client.container_name,
                "path": earnings_path,
            },
            lambda path=earnings_path: _delete_blob_if_exists(client, path=path),
        )
    )

    price_target_path = f"price-target-data/{normalized_symbol}.parquet"
    tasks.append(
        (
            {
                "layer": "bronze",
                "domain": "price-target",
                "container": client.container_name,
                "path": price_target_path,
            },
            lambda path=price_target_path: _delete_blob_if_exists(client, path=path),
        )
    )
    worker_count = _resolve_purge_symbol_target_workers(len(tasks))
    return _run_symbol_cleanup_tasks(tasks, worker_count=worker_count, thread_name_prefix="purge-symbol-bronze")


def _remove_symbol_from_layer_storage(
    client: BlobStorageClient,
    container: str,
    symbol: str,
    layer: Literal["silver", "gold"],
) -> List[Dict[str, Any]]:
    normalized_symbol = _normalize_purge_symbol(symbol)
    market_symbol = _market_symbol(normalized_symbol)
    tasks: List[Tuple[Dict[str, Any], Callable[[], int]]] = []

    if layer == "silver":
        market_path = DataPaths.get_market_data_path(market_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "market",
                    "container": container,
                    "path": market_path,
                },
                lambda path=market_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )

        for folder, suffix in _FINANCE_BRONZE_TABLE_TYPES:
            finance_path = DataPaths.get_finance_path(folder, normalized_symbol, suffix)
            tasks.append(
                (
                    {
                        "layer": layer,
                        "domain": "finance",
                        "container": container,
                        "path": finance_path,
                    },
                    lambda path=finance_path: _delete_prefix_if_exists(client=client, path=path),
                )
            )

        earnings_path = DataPaths.get_earnings_path(normalized_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "earnings",
                    "container": container,
                    "path": earnings_path,
                },
                lambda path=earnings_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )
        price_target_path = DataPaths.get_price_target_path(normalized_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "price-target",
                    "container": container,
                    "path": price_target_path,
                },
                lambda path=price_target_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )
    else:
        market_path = DataPaths.get_gold_features_path(market_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "market",
                    "container": container,
                    "path": market_path,
                },
                lambda path=market_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )
        finance_path = DataPaths.get_gold_finance_path(normalized_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "finance",
                    "container": container,
                    "path": finance_path,
                },
                lambda path=finance_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )
        earnings_path = DataPaths.get_gold_earnings_path(normalized_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "earnings",
                    "container": container,
                    "path": earnings_path,
                },
                lambda path=earnings_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )
        price_target_path = DataPaths.get_gold_price_targets_path(normalized_symbol)
        tasks.append(
            (
                {
                    "layer": layer,
                    "domain": "price-target",
                    "container": container,
                    "path": price_target_path,
                },
                lambda path=price_target_path: _delete_prefix_if_exists(client=client, path=path),
            )
        )

    worker_count = _resolve_purge_symbol_target_workers(len(tasks))
    thread_name = f"purge-symbol-{layer}"
    return _run_symbol_cleanup_tasks(tasks, worker_count=worker_count, thread_name_prefix=thread_name)


def _resolve_purge_targets(scope: str, layer: Optional[str], domain: Optional[str]) -> List[Dict[str, Optional[str]]]:
    scope = scope.strip().lower()
    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)

    targets: List[Dict[str, Optional[str]]] = []

    if scope == "layer-domain":
        if not layer_norm or not domain_norm:
            raise HTTPException(status_code=400, detail="layer and domain are required for scope 'layer-domain'.")
        for container, prefix in _targets_for_layer_domain(layer_norm, domain_norm):
            targets.append({"layer": layer_norm, "domain": domain_norm, "container": container, "prefix": prefix})
    elif scope == "layer":
        if not layer_norm:
            raise HTTPException(status_code=400, detail="layer is required for scope 'layer'.")
        container = _resolve_container(layer_norm)
        targets.append({"layer": layer_norm, "domain": None, "container": container, "prefix": None})
    elif scope == "domain":
        if not domain_norm:
            raise HTTPException(status_code=400, detail="domain is required for scope 'domain'.")
        for layer_name in _DOMAIN_PREFIXES.keys():
            if domain_norm not in _DOMAIN_PREFIXES.get(layer_name, {}):
                continue
            for container, prefix in _targets_for_layer_domain(layer_name, domain_norm):
                targets.append({"layer": layer_name, "domain": domain_norm, "container": container, "prefix": prefix})
        if not targets:
            raise HTTPException(status_code=400, detail=f"No targets found for domain '{domain_norm}'.")
    else:
        raise HTTPException(status_code=400, detail=f"Unknown scope '{scope}'.")

    return targets


def _watermark_blob_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/{cleaned}.json"


def _run_checkpoint_blob_path(key: str) -> str:
    cleaned = (key or "").strip().replace(" ", "_")
    return f"system/watermarks/runs/{cleaned}.json"


def _collect_domains_for_layer(
    targets: List[Dict[str, Optional[str]]],
    *,
    layer: str,
    supported_domains: Sequence[str],
) -> List[str]:
    domains: set[str] = set()
    include_all_domains = False

    for target in targets:
        target_layer = _normalize_layer(str(target.get("layer") or ""))
        if target_layer != layer:
            continue

        raw_domain = target.get("domain")
        target_domain = _normalize_domain(str(raw_domain or "")) if raw_domain is not None else None
        if not target_domain:
            include_all_domains = True
            continue
        if target_domain in supported_domains:
            domains.add(target_domain)

    if include_all_domains:
        domains.update(supported_domains)
    return [name for name in supported_domains if name in domains]


def _build_silver_checkpoint_reset_targets(targets: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    domains = _collect_domains_for_layer(
        targets,
        layer="silver",
        supported_domains=list(_SILVER_JOB_CHECKPOINT_KEYS.keys()),
    )
    if not domains:
        return []

    common_container = str(getattr(cfg, "AZURE_CONTAINER_COMMON", "") or "").strip()
    if not common_container:
        raise HTTPException(status_code=503, detail="Missing AZURE_CONTAINER_COMMON for silver checkpoint reset.")

    checkpoint_targets: List[Dict[str, Optional[str]]] = []
    for domain in domains:
        bronze_watermark_key, silver_run_key = _SILVER_JOB_CHECKPOINT_KEYS[domain]
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _watermark_blob_path(bronze_watermark_key),
                "operation": "reset-watermark",
            }
        )
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _run_checkpoint_blob_path(silver_run_key),
                "operation": "reset-run-checkpoint",
            }
        )

    return checkpoint_targets


def _build_gold_checkpoint_reset_targets(targets: List[Dict[str, Optional[str]]]) -> List[Dict[str, Optional[str]]]:
    domains = _collect_domains_for_layer(
        targets,
        layer="gold",
        supported_domains=list(_GOLD_JOB_WATERMARK_KEYS.keys()),
    )
    if not domains:
        return []

    common_container = str(getattr(cfg, "AZURE_CONTAINER_COMMON", "") or "").strip()
    if not common_container:
        raise HTTPException(status_code=503, detail="Missing AZURE_CONTAINER_COMMON for gold checkpoint reset.")

    checkpoint_targets: List[Dict[str, Optional[str]]] = []
    for domain in domains:
        watermark_key = _GOLD_JOB_WATERMARK_KEYS[domain]
        checkpoint_targets.append(
            {
                "layer": "common",
                "domain": domain,
                "container": common_container,
                "prefix": _watermark_blob_path(watermark_key),
                "operation": "reset-watermark",
            }
        )

    return checkpoint_targets


def _run_purge_operation(payload: PurgeRequest) -> Dict[str, Any]:
    targets = _resolve_purge_targets(payload.scope, payload.layer, payload.domain)
    targets = [
        *targets,
        *_build_silver_checkpoint_reset_targets(targets),
        *_build_gold_checkpoint_reset_targets(targets),
    ]

    worker_count = _resolve_purge_scope_workers(len(targets))
    planned_by_index: Dict[int, Tuple[BlobStorageClient, Dict[str, Optional[str]]]] = {}
    any_data = False
    if worker_count <= 1:
        for idx, target in enumerate(targets):
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            try:
                client = BlobStorageClient(container_name=container, ensure_container_exists=False)
                has_data = client.has_blobs(prefix)
            except Exception as exc:
                logger.exception(
                    "Purge preflight failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                    container,
                    prefix,
                    payload.scope,
                    target.get("layer"),
                    target.get("domain"),
                )
                raise HTTPException(
                    status_code=502, detail=f"Purge preflight failed for {container}:{prefix}: {exc}"
                ) from exc
            target["hasData"] = bool(has_data)
            planned_by_index[idx] = (client, target)
            any_data = any_data or bool(has_data)
    else:
        def _preflight_target(idx: int, target: Dict[str, Optional[str]]) -> Tuple[int, BlobStorageClient, bool]:
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            client = BlobStorageClient(container_name=container, ensure_container_exists=False)
            has_data = bool(client.has_blobs(prefix))
            return idx, client, has_data

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-preflight") as executor:
            future_to_target: Dict[Any, Tuple[int, Dict[str, Optional[str]]]] = {
                executor.submit(_preflight_target, idx, target): (idx, target) for idx, target in enumerate(targets)
            }
            for future in as_completed(future_to_target):
                idx, target = future_to_target[future]
                container = str(target.get("container") or "")
                prefix = target.get("prefix")
                try:
                    _, client, has_data = future.result()
                except Exception as exc:
                    logger.exception(
                        "Purge preflight failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                        container,
                        prefix,
                        payload.scope,
                        target.get("layer"),
                        target.get("domain"),
                    )
                    raise HTTPException(
                        status_code=502, detail=f"Purge preflight failed for {container}:{prefix}: {exc}"
                    ) from exc
                target["hasData"] = bool(has_data)
                planned_by_index[idx] = (client, target)
                any_data = any_data or bool(has_data)

    planned = [planned_by_index[idx] for idx in sorted(planned_by_index.keys())]

    if not any_data:
        raise HTTPException(status_code=409, detail="Nothing to purge for the selected scope.")

    results: List[Dict[str, Any]] = []
    total_deleted = 0

    if worker_count <= 1:
        for client, target in planned:
            if not target.get("hasData"):
                continue
            container = str(target["container"] or "")
            prefix = target.get("prefix")
            try:
                deleted = client.delete_prefix(prefix)
            except Exception as exc:
                logger.exception(
                    "Purge failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                    container,
                    prefix,
                    payload.scope,
                    target.get("layer"),
                    target.get("domain"),
                )
                raise HTTPException(status_code=502, detail=f"Purge failed for {container}:{prefix}: {exc}") from exc

            result: Dict[str, Any] = {
                "container": container,
                "prefix": prefix,
                "layer": target.get("layer"),
                "domain": target.get("domain"),
                "deleted": deleted,
            }
            if target.get("operation"):
                result["operation"] = target.get("operation")
            results.append(result)
            total_deleted += int(deleted or 0)
    else:
        delete_results_by_index: Dict[int, Dict[str, Any]] = {}

        def _delete_target(
            idx: int, client: BlobStorageClient, target: Dict[str, Optional[str]]
        ) -> Tuple[int, Dict[str, Any]]:
            container = str(target.get("container") or "")
            prefix = target.get("prefix")
            deleted = client.delete_prefix(prefix)
            result: Dict[str, Any] = {
                "container": container,
                "prefix": prefix,
                "layer": target.get("layer"),
                "domain": target.get("domain"),
                "deleted": deleted,
            }
            if target.get("operation"):
                result["operation"] = target.get("operation")
            return idx, result

        with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="purge-delete") as executor:
            future_to_target: Dict[Any, Tuple[int, Dict[str, Optional[str]]]] = {}
            for idx, (client, target) in enumerate(planned):
                if not target.get("hasData"):
                    continue
                future = executor.submit(_delete_target, idx, client, target)
                future_to_target[future] = (idx, target)

            for future in as_completed(future_to_target):
                _, target = future_to_target[future]
                container = str(target.get("container") or "")
                prefix = target.get("prefix")
                try:
                    idx, result = future.result()
                except Exception as exc:
                    logger.exception(
                        "Purge failed: container=%s prefix=%s scope=%s layer=%s domain=%s",
                        container,
                        prefix,
                        payload.scope,
                        target.get("layer"),
                        target.get("domain"),
                    )
                    raise HTTPException(status_code=502, detail=f"Purge failed for {container}:{prefix}: {exc}") from exc
                delete_results_by_index[idx] = result

        for idx in sorted(delete_results_by_index.keys()):
            result = delete_results_by_index[idx]
            results.append(result)
            total_deleted += int(result.get("deleted") or 0)

    logger.warning(
        "Purge completed: scope=%s layer=%s domain=%s targets=%s deleted=%s",
        payload.scope,
        payload.layer,
        payload.domain,
        len(results),
        total_deleted,
    )

    return {
        "scope": payload.scope,
        "layer": payload.layer,
        "domain": payload.domain,
        "totalDeleted": total_deleted,
        "targets": results,
    }


def _run_purge_symbol_operation(
    payload: PurgeSymbolRequest,
    *,
    update_blacklist: bool = True,
) -> Dict[str, Any]:
    normalized_symbol = _normalize_purge_symbol(payload.symbol)

    container_bronze = _resolve_container("bronze")
    container_silver = _resolve_container("silver")
    container_gold = _resolve_container("gold")

    bronze_client = BlobStorageClient(container_name=container_bronze, ensure_container_exists=False)
    silver_client = BlobStorageClient(container_name=container_silver, ensure_container_exists=False)
    gold_client = BlobStorageClient(container_name=container_gold, ensure_container_exists=False)

    results: List[Dict[str, Any]] = []
    total_deleted = 0

    if update_blacklist:
        with _PURGE_BLACKLIST_UPDATE_LOCK:
            blacklist_update = _append_symbol_to_bronze_blacklists(bronze_client, normalized_symbol)
        results.append(
            {
                "operation": "blacklist",
                "layer": "bronze",
                "domain": "all",
                "container": container_bronze,
                "status": "updated",
                "paths": blacklist_update["paths"],
                "updated": blacklist_update["updated"],
            }
        )

    layer_work: Dict[str, Callable[[], List[Dict[str, Any]]]] = {
        "bronze": lambda: _remove_symbol_from_bronze_storage(bronze_client, normalized_symbol),
        "silver": lambda: _remove_symbol_from_layer_storage(
            client=silver_client,
            container=container_silver,
            symbol=normalized_symbol,
            layer="silver",
        ),
        "gold": lambda: _remove_symbol_from_layer_storage(
            client=gold_client,
            container=container_gold,
            symbol=normalized_symbol,
            layer="gold",
        ),
    }
    layer_order = ["bronze", "silver", "gold"]
    layer_results: Dict[str, List[Dict[str, Any]]] = {}
    layer_worker_count = _resolve_purge_symbol_layer_workers(len(layer_order))
    if layer_worker_count <= 1:
        for layer_name in layer_order:
            layer_results[layer_name] = layer_work[layer_name]()
    else:
        with ThreadPoolExecutor(max_workers=layer_worker_count, thread_name_prefix="purge-symbol-layers") as executor:
            future_to_layer = {executor.submit(layer_work[layer_name]): layer_name for layer_name in layer_order}
            for future in as_completed(future_to_layer):
                layer_name = future_to_layer[future]
                layer_results[layer_name] = future.result()

    for layer_name in layer_order:
        for outcome in layer_results.get(layer_name, []):
            total_deleted += int(outcome.get("deleted") or 0)
            results.append(outcome)

    logger.warning(
        "Purge-symbol completed: symbol=%s bronze=%s silver=%s gold=%s",
        normalized_symbol,
        container_bronze,
        container_silver,
        container_gold,
    )

    return {
        "symbol": normalized_symbol,
        "symbolVariants": _symbol_variants(normalized_symbol),
        "totalDeleted": total_deleted,
        "targets": results,
    }


def _execute_purge_operation(operation_id: str, payload: PurgeRequest) -> None:
    try:
        result = _run_purge_operation(payload)
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": result, "completedAt": _utc_timestamp()},
        )
    except HTTPException as exc:
        logger.exception(
            "Purge operation failed: operation=%s scope=%s layer=%s domain=%s",
            operation_id,
            payload.scope,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {"status": "failed", "error": str(exc.detail), "completedAt": _utc_timestamp()},
        )
    except Exception as exc:
        logger.exception(
            "Purge operation crashed: operation=%s scope=%s layer=%s domain=%s",
            operation_id,
            payload.scope,
            payload.layer,
            payload.domain,
        )
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "completedAt": _utc_timestamp(),
            },
        )


def _create_purge_symbol_operation(
    payload: PurgeSymbolRequest,
    actor: Optional[str],
) -> str:
    operation_id = str(uuid.uuid4())
    now = _utc_timestamp()
    with _PURGE_OPERATIONS_LOCK:
        _PURGE_OPERATIONS[operation_id] = {
            "operationId": operation_id,
            "status": "running",
            "scope": "symbol",
            "symbol": payload.symbol,
            "requestedBy": actor,
            "createdAt": now,
            "updatedAt": now,
            "startedAt": now,
            "completedAt": None,
            "result": None,
            "error": None,
        }
    return operation_id


def _execute_purge_symbol_operation(operation_id: str, payload: PurgeSymbolRequest) -> None:
    try:
        result = _run_purge_symbol_operation(payload)
        _update_purge_operation(
            operation_id,
            {"status": "succeeded", "result": result, "completedAt": _utc_timestamp()},
        )
    except HTTPException as exc:
        logger.exception("Purge-symbol operation failed: operation=%s symbol=%s", operation_id, payload.symbol)
        _update_purge_operation(
            operation_id,
            {"status": "failed", "error": str(exc.detail), "completedAt": _utc_timestamp()},
        )
    except Exception as exc:
        logger.exception("Purge-symbol operation crashed: operation=%s symbol=%s", operation_id, payload.symbol)
        _update_purge_operation(
            operation_id,
            {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "completedAt": _utc_timestamp(),
            },
        )


def _run_due_purge_rules(dsn: str, *, actor: Optional[str]) -> Dict[str, Any]:
    due_rules = list_due_purge_rules(dsn=dsn)
    now = datetime.now(timezone.utc)
    result = {
        "checked": len(due_rules),
        "executed": 0,
        "succeeded": 0,
        "failed": 0,
    }

    for rule in due_rules:
        try:
            if not claim_purge_rule_for_run(
                dsn=dsn,
                rule_id=rule.id,
                now=now,
                require_due=True,
                actor=actor,
            ):
                continue
        except Exception:
            logger.exception("Failed to claim purge rule for execution: id=%s", rule.id)
            result["failed"] += 1
            continue

        try:
            execution = _execute_purge_rule(rule=rule, actor=actor)
            failed_symbols = execution.get("failedSymbols") or []
            status = "failed" if failed_symbols else "succeeded"
            complete_purge_rule_execution(
                dsn=dsn,
                rule_id=rule.id,
                status=status,
                error=None if not failed_symbols else "; ".join(failed_symbols),
                matched_count=int(execution.get("matchedCount") or 0),
                purged_count=int(execution.get("purgedCount") or 0),
                run_interval_minutes=rule.run_interval_minutes,
                actor=actor,
                now=now,
            )
            result["executed"] += 1
            if status == "succeeded":
                result["succeeded"] += 1
            else:
                result["failed"] += 1
        except Exception as exc:
            logger.exception("Purge rule execution failed: id=%s name=%s", rule.id, rule.name)
            try:
                complete_purge_rule_execution(
                    dsn=dsn,
                    rule_id=rule.id,
                    status="failed",
                    error=f"{type(exc).__name__}: {exc}",
                    matched_count=None,
                    purged_count=None,
                    run_interval_minutes=rule.run_interval_minutes,
                    actor=actor,
                    now=now,
                )
            except Exception:
                logger.exception("Failed to persist purge-rule failure status: id=%s", rule.id)
            result["failed"] += 1

    return result


def run_due_purge_rules(*, dsn: Optional[str], actor: Optional[str] = "system") -> Dict[str, Any]:
    if not dsn:
        raise ValueError("POSTGRES_DSN is not configured.")
    return _run_due_purge_rules(dsn=dsn, actor=actor)


@router.get("/purge-rules/operators")
def list_purge_rule_operators(request: Request) -> JSONResponse:
    validate_auth(request)
    return JSONResponse({"operators": supported_purge_rule_operators()}, headers={"Cache-Control": "no-store"})


@router.get("/purge-rules")
def list_purge_rules_endpoint(
    request: Request,
    enabled_only: bool = Query(default=False),
    layer: Optional[str] = Query(default=None),
    domain: Optional[str] = Query(default=None),
) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    try:
        normalized_layer = _normalize_layer(layer)
        normalized_domain = _normalize_domain(domain)
        rules = list_purge_rules(dsn=dsn, enabled_only=enabled_only, layer=normalized_layer, domain=normalized_domain)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid purge-rule query: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to list purge rules.")
        raise HTTPException(status_code=503, detail=f"Failed to list purge rules: {exc}") from exc

    return JSONResponse(
        {"items": [_serialize_purge_rule(rule) for rule in rules]},
        headers={"Cache-Control": "no-store"},
    )


@router.post("/purge-rules")
def create_purge_rule_endpoint(payload: PurgeRuleCreateRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    actor = _get_actor(request)
    normalized_layer = _normalize_layer(payload.layer)
    normalized_domain = _normalize_domain(payload.domain)
    if not normalized_layer or not normalized_domain:
        raise HTTPException(status_code=400, detail="layer and domain are required.")
    try:
        operator = normalize_purge_rule_operator(payload.operator)
        rule = create_purge_rule(
            dsn=dsn,
            name=payload.name,
            layer=normalized_layer,
            domain=normalized_domain,
            column_name=payload.column_name,
            operator=operator,
            threshold=payload.threshold,
            run_interval_minutes=payload.run_interval_minutes,
            enabled=payload.enabled,
            actor=actor,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid purge rule: {exc}") from exc
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to create purge rule: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to create purge rule.")
        raise HTTPException(status_code=500, detail=f"Failed to create purge rule: {exc}") from exc

    return JSONResponse(_serialize_purge_rule(rule), headers={"Cache-Control": "no-store"}, status_code=201)


@router.patch("/purge-rules/{rule_id}")
def update_purge_rule_endpoint(
    rule_id: int,
    payload: PurgeRuleUpdateRequest,
    request: Request,
) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    actor = _get_actor(request)
    if all(
        value is None
        for value in (
            payload.name,
            payload.layer,
            payload.domain,
            payload.column_name,
            payload.operator,
            payload.threshold,
            payload.run_interval_minutes,
            payload.enabled,
        )
    ):
        raise HTTPException(status_code=400, detail="No fields supplied for purge rule update.")

    try:
        rule = update_purge_rule(
            dsn=dsn,
            rule_id=rule_id,
            name=payload.name,
            layer=(_normalize_layer(payload.layer) if payload.layer is not None else None),
            domain=(_normalize_domain(payload.domain) if payload.domain is not None else None),
            column_name=payload.column_name,
            operator=normalize_purge_rule_operator(payload.operator) if payload.operator is not None else None,
            threshold=payload.threshold,
            run_interval_minutes=payload.run_interval_minutes,
            enabled=payload.enabled,
            actor=actor,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid purge-rule update: {exc}") from exc
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to update purge rule: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to update purge rule id=%s.", rule_id)
        raise HTTPException(status_code=500, detail=f"Failed to update purge rule: {exc}") from exc

    return JSONResponse(_serialize_purge_rule(rule), headers={"Cache-Control": "no-store"})


@router.delete("/purge-rules/{rule_id}", status_code=200)
def delete_purge_rule_endpoint(rule_id: int, request: Request) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    deleted = False
    try:
        deleted = delete_purge_rule_row(dsn=dsn, rule_id=rule_id)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to delete purge rule: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to delete purge rule id=%s.", rule_id)
        raise HTTPException(status_code=500, detail=f"Failed to delete purge rule: {exc}") from exc
    if not deleted:
        raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")
    return JSONResponse({"deleted": True, "id": rule_id}, headers={"Cache-Control": "no-store"})


@router.post("/purge-rules/{rule_id}/preview")
def preview_purge_rule(rule_id: int, request: Request, payload: PurgeRulePreviewRequest) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    rule = get_purge_rule(dsn=dsn, rule_id=rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")

    try:
        matches = _collect_rule_symbol_values(rule)
        matches = sorted(matches, key=lambda pair: str(pair[0]).strip().upper())
        preview = [
            {
                "symbol": symbol,
                "value": metric,
            }
            for symbol, metric in matches[: payload.max_symbols]
        ]
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to preview purge rule id=%s.", rule_id)
        raise HTTPException(status_code=500, detail=f"Failed to preview purge rule: {exc}") from exc

    return JSONResponse(
        {
            "rule": _serialize_purge_rule(rule),
            "matchCount": len(matches),
            "previewCount": len(preview),
            "matches": preview,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/purge-rules/{rule_id}/run")
def run_purge_rule_now(rule_id: int, request: Request) -> JSONResponse:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    actor = _get_actor(request)
    now = datetime.now(timezone.utc)
    rule = get_purge_rule(dsn=dsn, rule_id=rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail=f"Purge rule id={rule_id} not found.")

    if not rule.enabled:
        raise HTTPException(status_code=409, detail="Purge rule is disabled.")

    if not claim_purge_rule_for_run(
        dsn=dsn,
        rule_id=rule.id,
        now=now,
        require_due=False,
        actor=actor,
    ):
        raise HTTPException(status_code=409, detail="Purge rule is already running.")

    try:
        execution = _execute_purge_rule(rule=rule, actor=actor)
        failed_symbols = execution.get("failedSymbols") or []
        status = "failed" if failed_symbols else "succeeded"
        complete_purge_rule_execution(
            dsn=dsn,
            rule_id=rule.id,
            status=status,
            error=None if not failed_symbols else "; ".join(failed_symbols),
            matched_count=int(execution.get("matchedCount") or 0),
            purged_count=int(execution.get("purgedCount") or 0),
            run_interval_minutes=rule.run_interval_minutes,
            actor=actor,
            now=now,
        )
    except Exception as exc:
        logger.exception("Failed to run purge rule id=%s now.", rule_id)
        try:
            complete_purge_rule_execution(
                dsn=dsn,
                rule_id=rule.id,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
                matched_count=None,
                purged_count=None,
                run_interval_minutes=rule.run_interval_minutes,
                actor=actor,
                now=now,
            )
        except Exception:
            logger.exception("Failed to persist purge-rule manual failure: id=%s", rule_id)
        raise HTTPException(status_code=500, detail=f"Failed to run purge rule: {exc}") from exc

    return JSONResponse(
        {
            "rule": _serialize_purge_rule(get_purge_rule(dsn=dsn, rule_id=rule_id) or rule),
            "execution": execution,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/purge")
def purge_data(payload: PurgeRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to purge data.")

    actor = _get_actor(request)
    logger.info(
        "Purge request received: actor=%s scope=%s layer=%s domain=%s",
        actor or "-",
        payload.scope,
        payload.layer,
        payload.domain,
    )
    operation_id = _create_purge_operation(payload, actor)
    thread = threading.Thread(target=_execute_purge_operation, args=(operation_id, payload), daemon=True)
    thread.start()
    logger.info(
        "Purge operation queued: operation=%s actor=%s scope=%s layer=%s domain=%s",
        operation_id,
        actor or "-",
        payload.scope,
        payload.layer,
        payload.domain,
    )

    return JSONResponse(
        {
            "operationId": operation_id,
            "status": "running",
            "scope": payload.scope,
            "layer": payload.layer,
            "domain": payload.domain,
            "createdAt": _utc_timestamp(),
            "updatedAt": _utc_timestamp(),
            "startedAt": _utc_timestamp(),
            "completedAt": None,
            "result": None,
            "error": None,
        },
        status_code=202,
    )


@router.get("/domain-lists", response_model=DomainListsResponse)
def get_domain_lists(
    request: Request,
    layer: str = Query(..., description="Layer key (bronze|silver|gold|platinum)"),
    domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
    limit: int = Query(default=5000, ge=1, le=50000, description="Max symbols returned per list file."),
) -> JSONResponse:
    validate_auth(request)

    layer_norm = _normalize_layer(layer)
    domain_norm = _normalize_domain(domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    container = _resolve_container(layer_norm)
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    list_paths = _resolve_domain_list_paths(layer_norm, domain_norm)

    files: List[Dict[str, Any]] = []
    for item in list_paths:
        list_type = str(item.get("listType") or "").strip().lower()
        path = str(item.get("path") or "").strip()
        if list_type not in {"whitelist", "blacklist"} or not path:
            continue
        files.append(
            _load_domain_list_file_preview(
                client,
                list_type=list_type,
                path=path,
                limit=limit,
            )
        )

    payload = {
        "layer": layer_norm,
        "domain": domain_norm,
        "container": container,
        "limit": limit,
        "files": files,
        "loadedAt": _utc_timestamp(),
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/domain-lists/reset")
def reset_domain_lists(payload: DomainListResetRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to reset blacklist/whitelist lists.")

    layer_norm = _normalize_layer(payload.layer)
    domain_norm = _normalize_domain(payload.domain)
    if not layer_norm:
        raise HTTPException(status_code=400, detail="layer is required.")
    if not domain_norm:
        raise HTTPException(status_code=400, detail="domain is required.")

    container = _resolve_container(layer_norm)
    client = BlobStorageClient(container_name=container, ensure_container_exists=False)
    result = _reset_domain_lists(client, layer=layer_norm, domain=domain_norm)
    actor = _get_actor(request)
    logger.warning(
        "Domain lists reset: actor=%s layer=%s domain=%s container=%s reset=%s",
        actor or "-",
        layer_norm,
        domain_norm,
        container,
        result.get("resetCount"),
    )
    return JSONResponse(result, headers={"Cache-Control": "no-store"})


@router.post("/domain-checkpoints/reset")
def reset_domain_checkpoints(payload: DomainCheckpointResetRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to reset checkpoint gates.")

    result = _reset_domain_checkpoints(layer=payload.layer, domain=payload.domain)
    actor = _get_actor(request)
    logger.warning(
        "Domain checkpoints reset: actor=%s layer=%s domain=%s container=%s reset=%s deleted=%s",
        actor or "-",
        result.get("layer"),
        result.get("domain"),
        result.get("container") or "-",
        result.get("resetCount"),
        result.get("deletedCount"),
    )
    return JSONResponse(result, headers={"Cache-Control": "no-store"})


@router.get("/purge-candidates")
def get_purge_candidates(
    request: Request,
    layer: str = Query(..., description="Layer key (bronze/silver/gold)"),
    domain: str = Query(..., description="Domain key (market/finance/earnings/price-target)"),
    column: str = Query(..., description="Column to evaluate"),
    operator: str = Query(..., description="Supported operators: gt, gte, lt, lte, top_percent, bottom_percent"),
    value: Optional[float] = Query(default=None, description="Numeric threshold (required for numeric operators)"),
    percentile: Optional[float] = Query(default=None, description="Required for percent operators"),
    as_of: Optional[str] = Query(default=None, description="Optional date limit (YYYY-MM-DD)"),
    recent_rows: int = Query(default=1, ge=1, le=5000, description="Recent rows per symbol used for aggregation"),
    aggregation: str = Query(default="avg", description="Aggregation over recent rows: min|max|avg|stddev"),
    limit: Optional[int] = Query(default=None, ge=1, le=5000, description="Deprecated: optional max candidate rows"),
    offset: int = Query(default=0, ge=0, description="Candidate result offset"),
    min_rows: int = Query(default=1, ge=1, description="Minimum rows contributing per symbol"),
) -> JSONResponse:
    validate_auth(request)
    try:
        response_payload = _build_purge_candidates_response(
            layer=layer,
            domain=domain,
            column=column,
            operator=operator,
            value=value,
            percentile=percentile,
            as_of=as_of,
            recent_rows=recent_rows,
            aggregation=aggregation,
            limit=limit,
            offset=offset,
            min_rows=min_rows,
        )
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception(
            "Failed to collect purge candidates: layer=%s domain=%s column=%s",
            layer,
            domain,
            column,
        )
        raise HTTPException(status_code=500, detail=f"Failed to collect purge candidates: {exc}") from exc

    return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})


@router.post("/purge-candidates")
def create_purge_candidates_operation(payload: PurgeCandidatesRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    actor = _get_actor(request)
    operation_id = _create_purge_candidates_operation(payload, actor)
    thread = threading.Thread(target=_execute_purge_candidates_operation, args=(operation_id, payload), daemon=True)
    thread.start()

    operation = _get_purge_operation(operation_id)
    if not operation:
        raise HTTPException(status_code=500, detail="Failed to initialize purge-candidates operation.")

    return JSONResponse(operation, status_code=202)


@router.get("/purge-symbols/blacklist")
def get_blacklist_symbols_for_purge(request: Request) -> JSONResponse:
    validate_auth(request)

    container_bronze = _resolve_container("bronze")
    bronze_client = BlobStorageClient(container_name=container_bronze, ensure_container_exists=False)
    payload = _load_symbols_from_bronze_blacklists(bronze_client)
    payload["loadedAt"] = _utc_timestamp()

    logger.info(
        "Loaded blacklist symbols for purge: container=%s symbols=%s sources=%s",
        container_bronze,
        payload.get("symbolCount"),
        len(payload.get("sources") or []),
    )
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/purge-symbols")
def purge_symbols(payload: PurgeSymbolsBatchRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to purge symbols.")

    actor = _get_actor(request)
    normalized_symbols = _normalize_candidate_symbols(payload.symbols)
    if not normalized_symbols:
        raise HTTPException(status_code=400, detail="At least one symbol is required.")

    audit_rule: Optional[PurgeRule] = None
    if payload.audit_rule and not payload.dry_run:
        dsn = _require_postgres_dsn(request)
        audit_rule = _persist_purge_symbols_audit_rule(
            dsn=dsn,
            audit_rule=payload.audit_rule,
            actor=actor,
        )

    operation_id = _create_purge_symbols_operation(
        normalized_symbols,
        actor,
        scope_note=payload.scope_note,
        dry_run=bool(payload.dry_run),
        audit_rule_id=(audit_rule.id if audit_rule else None),
    )
    logger.info(
        "Purge-symbols requested: operation=%s actor=%s symbols=%s dry_run=%s audit_rule_id=%s",
        operation_id,
        actor or "-",
        len(normalized_symbols),
        bool(payload.dry_run),
        (audit_rule.id if audit_rule else None),
    )
    thread = threading.Thread(
        target=_execute_purge_symbols_operation,
        args=(
            operation_id,
            normalized_symbols,
        ),
        kwargs={"dry_run": bool(payload.dry_run), "scope_note": payload.scope_note},
        daemon=True,
    )
    thread.start()

    operation = _get_purge_operation(operation_id) or {}
    if not isinstance(operation, dict):
        raise HTTPException(status_code=500, detail="Failed to initialize purge-symbols operation.")

    return JSONResponse(operation, status_code=202)


@router.post("/purge-symbol")
def purge_symbol(payload: PurgeSymbolRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to purge a symbol.")

    actor = _get_actor(request)
    normalized_symbol = _normalize_purge_symbol(payload.symbol)
    symbol_payload = PurgeSymbolRequest(symbol=normalized_symbol, confirm=payload.confirm)
    operation_id = _create_purge_symbol_operation(symbol_payload, actor)
    thread = threading.Thread(target=_execute_purge_symbol_operation, args=(operation_id, symbol_payload), daemon=True)
    thread.start()

    return JSONResponse(
        {
            "operationId": operation_id,
            "status": "running",
            "scope": "symbol",
            "symbol": normalized_symbol,
            "createdAt": _utc_timestamp(),
            "updatedAt": _utc_timestamp(),
            "startedAt": _utc_timestamp(),
            "completedAt": None,
            "result": None,
            "error": None,
        },
        status_code=202,
    )


@router.get("/purge/{operation_id}")
def get_purge_operation(operation_id: str, request: Request) -> JSONResponse:
    validate_auth(request)
    operation = _get_purge_operation(operation_id)
    if not operation:
        raise HTTPException(status_code=404, detail="Purge operation not found.")
    return JSONResponse(operation)


RUNTIME_CONFIG_CATALOG: Dict[str, Dict[str, str]] = {
    "SYMBOLS_REFRESH_INTERVAL_HOURS": {
        "description": "Refresh symbol universe from NASDAQ/Alpha Vantage when older than this many hours (0 disables refresh).",
        "example": "24",
    },
    "ALPHA_VANTAGE_RATE_LIMIT_PER_MIN": {
        "description": "Alpha Vantage API rate limit per minute (integer).",
        "example": "300",
    },
    "ALPHA_VANTAGE_TIMEOUT_SECONDS": {
        "description": "Alpha Vantage request timeout (float seconds).",
        "example": "15",
    },
    "ALPHA_VANTAGE_RATE_WAIT_TIMEOUT_SECONDS": {
        "description": "Max wait time for API-side Alpha Vantage rate-limit queue before returning throttle (float seconds).",
        "example": "600",
    },
    "ALPHA_VANTAGE_THROTTLE_COOLDOWN_SECONDS": {
        "description": "Cooldown after Alpha Vantage throttle signals; outbound requests are paused for this duration (minimum 60 seconds).",
        "example": "60",
    },
    "ALPHA_VANTAGE_MAX_WORKERS": {
        "description": "Alpha Vantage concurrency (max worker threads) for ingestion jobs (integer).",
        "example": "32",
    },
    "ALPHA_VANTAGE_EARNINGS_FRESH_DAYS": {
        "description": "How many days earnings data is considered fresh before re-fetch (integer).",
        "example": "7",
    },
    "ALPHA_VANTAGE_FINANCE_FRESH_DAYS": {
        "description": "How many days finance statement data is considered fresh before re-fetch (integer).",
        "example": "28",
    },
    "MASSIVE_TIMEOUT_SECONDS": {
        "description": "Massive request timeout (float seconds) for API gateway and ETL callers.",
        "example": "30",
    },
    "MASSIVE_MAX_WORKERS": {
        "description": "Massive concurrency (max worker threads) for market/finance bronze ingestion jobs.",
        "example": "32",
    },
    "MASSIVE_FINANCE_FRESH_DAYS": {
        "description": "How many days finance statement data is considered fresh before re-fetch (integer).",
        "example": "28",
    },
    "MASSIVE_PREFER_OFFICIAL_SDK": {
        "description": "When true, Massive integration prefers the official Massive SDK if installed.",
        "example": "true",
    },
    "SILVER_LATEST_ONLY": {
        "description": "When true, silver jobs prefer latest-only processing if supported.",
        "example": "true",
    },
    "SILVER_MARKET_LATEST_ONLY": {
        "description": "Domain override for market silver latest-only behavior.",
        "example": "true",
    },
    "SILVER_FINANCE_LATEST_ONLY": {
        "description": "Domain override for finance silver latest-only behavior.",
        "example": "true",
    },
    "SILVER_FINANCE_CATCHUP_MAX_PASSES": {
        "description": "Max catch-up relist passes for silver finance ingestion to absorb late Bronze writes (integer).",
        "example": "3",
    },
    "SILVER_FINANCE_USE_BRONZE_MANIFEST": {
        "description": "When true, silver finance prefers the latest unacknowledged Bronze finance run manifest.",
        "example": "true",
    },
    "SILVER_EARNINGS_LATEST_ONLY": {
        "description": "Domain override for earnings silver latest-only behavior.",
        "example": "true",
    },
    "SILVER_PRICE_TARGET_LATEST_ONLY": {
        "description": "Domain override for price-target silver latest-only behavior.",
        "example": "true",
    },
    "FEATURE_ENGINEERING_MAX_WORKERS": {
        "description": "Max workers for feature engineering concurrency (integer).",
        "example": "8",
    },
    "GOLD_MARKET_BY_DATE_ENABLED": {
        "description": "When true, gold-market-job also materializes the consolidated Gold market by-date view.",
        "example": "true",
    },
    "GOLD_BY_DATE_DOMAIN": {
        "description": "Gold domain to materialize by-date (market|finance|earnings|price-target).",
        "example": "market",
    },
    "GOLD_MARKET_BY_DATE_PATH": {
        "description": "Target Delta table path for the consolidated Gold market by-date view.",
        "example": "market_by_date",
    },
    "GOLD_MARKET_BY_DATE_COLUMNS": {
        "description": "Optional comma-separated projection list for by-date view columns (date/symbol always included).",
        "example": "close,volume,return_1d,vol_20d",
    },
    "MATERIALIZE_YEAR_MONTH": {
        "description": "Optional YYYY-MM or YYYY-MM..YYYY-MM partition filter used by by-date materialization for partial rebuilds.",
        "example": "2026-01..2026-03",
    },
    "TRIGGER_NEXT_JOB_NAME": {
        "description": "Optional downstream job name to trigger on success.",
        "example": "silver-market-job",
    },
    "TRIGGER_NEXT_JOB_REQUIRED": {
        "description": "When true, a downstream trigger failure fails the job; when false, it is best-effort.",
        "example": "true",
    },
    "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS": {
        "description": "Downstream trigger retry attempts (integer).",
        "example": "3",
    },
    "TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS": {
        "description": "Downstream trigger retry base delay (float seconds).",
        "example": "1.0",
    },
    "FINANCE_RUN_MANIFESTS_ENABLED": {
        "description": "When true, Bronze finance writes run manifests and Silver finance can persist per-run acknowledgements.",
        "example": "true",
    },
    "FINANCE_PIPELINE_SHARED_LOCK_NAME": {
        "description": "Shared distributed lock key used to serialize Bronze/Silver finance jobs.",
        "example": "finance-pipeline-shared",
    },
    "BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS": {
        "description": "How long Bronze finance waits for the shared finance lock before skipping/failing (float seconds).",
        "example": "0",
    },
    "SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS": {
        "description": "How long Silver finance waits for the shared finance lock before failing (float seconds).",
        "example": "3600",
    },
    "SYSTEM_HEALTH_TTL_SECONDS": {
        "description": "System health cache TTL for the API (float seconds).",
        "example": "300",
    },
    "SYSTEM_HEALTH_MAX_AGE_SECONDS": {
        "description": "Max staleness window before marking layers stale (integer seconds).",
        "example": "129600",
    },
    "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON": {
        "description": (
            "JSON object of per-domain freshness overrides. "
            "Keys support layer.domain, layer:domain, domain, layer.*, and *."
        ),
        "example": '{"silver.market":{"maxAgeSeconds":43200},"gold.*":{"maxAgeSeconds":172800}}',
    },
    "SYSTEM_HEALTH_MARKERS_ENABLED": {
        "description": "When true, prefer system-health marker blobs before legacy freshness probes.",
        "example": "true",
    },
    "SYSTEM_HEALTH_MARKERS_CONTAINER": {
        "description": "Container name holding marker blobs (defaults to AZURE_CONTAINER_COMMON).",
        "example": "common",
    },
    "SYSTEM_HEALTH_MARKERS_PREFIX": {
        "description": "Prefix path for marker blobs inside marker container.",
        "example": "system/health_markers",
    },
    "SYSTEM_HEALTH_MARKERS_DUAL_READ": {
        "description": "When true, run marker and legacy probes for parity checks.",
        "example": "false",
    },
    "SYSTEM_HEALTH_MARKERS_DUAL_READ_TOLERANCE_SECONDS": {
        "description": "Allowed marker-vs-legacy timestamp skew before warning (integer seconds).",
        "example": "21600",
    },
    "SYSTEM_HEALTH_VERBOSE_IDS": {
        "description": "Comma-separated list of alert IDs/components to include in verbose mode.",
        "example": "AzureMonitorMetrics,AzureLogAnalytics",
    },
    "SYSTEM_HEALTH_ARM_API_VERSION": {
        "description": "Azure ARM API version for Container Apps Job queries (string).",
        "example": "2024-03-01",
    },
    "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS": {
        "description": "Timeout for Azure ARM calls made by system health (float seconds).",
        "example": "5",
    },
    "SYSTEM_HEALTH_ARM_CONTAINERAPPS": {
        "description": "Comma-separated list of Container App names to probe via ARM.",
        "example": "asset-allocation-api,asset-allocation-ui",
    },
    "SYSTEM_HEALTH_ARM_JOBS": {
        "description": "Comma-separated list of Container App Job names to probe via ARM.",
        "example": "silver-market-job,gold-finance-job",
    },
    "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB": {
        "description": "How many recent job executions to pull per job during system-health probes (integer).",
        "example": "10",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_ENABLED": {
        "description": "When true, system health will query Azure Monitor Metrics for configured resources.",
        "example": "true",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION": {
        "description": "Azure Monitor Metrics API version.",
        "example": "2018-01-01",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES": {
        "description": "Timespan window (minutes) for Azure Monitor Metrics queries (integer).",
        "example": "15",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL": {
        "description": "Metrics query interval (ISO8601 duration string).",
        "example": "PT1M",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION": {
        "description": "Metrics aggregation (e.g., Average, Total).",
        "example": "Average",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS": {
        "description": "Comma-separated metric names to query for Container Apps.",
        "example": "CpuUsage,MemoryWorkingSetBytes",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS": {
        "description": "Comma-separated metric names to query for Container Apps Jobs.",
        "example": "JobExecutionCount,JobExecutionTime",
    },
    "SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON": {
        "description": "JSON object mapping metric name to thresholds (warn_above/error_above/etc).",
        "example": '{"CpuUsage":{"warn_above":80,"error_above":95}}',
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED": {
        "description": "When true, system health will query Azure Log Analytics for configured resources.",
        "example": "true",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID": {
        "description": "Log Analytics workspace ID for system health queries.",
        "example": "00000000-0000-0000-0000-000000000000",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS": {
        "description": "Timeout for Log Analytics queries made by system health (float seconds).",
        "example": "5",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES": {
        "description": "Timespan window (minutes) for Log Analytics queries (integer).",
        "example": "15",
    },
    "SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON": {
        "description": "JSON array of Log Analytics query specs used by system health (KQL templates).",
        "example": '[{"resourceType":"Microsoft.App/jobs","name":"job_errors_15m","query":"ContainerAppConsoleLogs_CL|...","warnAbove":1,"errorAbove":10,"unit":"count"}]',
    },
    "SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED": {
        "description": "When true, system health includes Azure Resource Health checks.",
        "example": "true",
    },
    "SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION": {
        "description": "Azure Resource Health API version.",
        "example": "2022-10-01",
    },
    "DOMAIN_METADATA_MAX_SCANNED_BLOBS": {
        "description": "Limit for blob scanning when computing domain metadata (integer).",
        "example": "200000",
    },
    "DOMAIN_METADATA_CACHE_PATH": {
        "description": "Common-container JSON file path used to persist per-layer/domain metadata snapshots.",
        "example": "metadata/domain-metadata.json",
    },
    "DOMAIN_METADATA_UI_CACHE_PATH": {
        "description": "Common-container JSON file path used to persist UI-hydrated domain metadata snapshots.",
        "example": "metadata/ui-cache/domain-metadata-snapshot.json",
    },
    "DOMAIN_METADATA_SNAPSHOT_CACHE_TTL_SECONDS": {
        "description": "In-process TTL (seconds) for the parsed domain metadata snapshot document.",
        "example": "30",
    },
}


class RuntimeConfigUpsertRequest(BaseModel):
    key: str = Field(..., description="Configuration key (env-var style).")
    scope: str = Field(default="global", description="Scope for this key (e.g., global or job:<name>).")
    enabled: bool = Field(default=True, description="When true, apply this value as an override.")
    value: str = Field(default="", description="Raw string value to apply (can be empty).")
    description: Optional[str] = Field(default=None, description="Optional human-readable description.")


@router.get("/runtime-config/catalog")
def get_runtime_config_catalog(request: Request) -> JSONResponse:
    validate_auth(request)
    items = []
    for key in sorted(DEFAULT_ENV_OVERRIDE_KEYS):
        meta = RUNTIME_CONFIG_CATALOG.get(key, {})
        items.append(
            {
                "key": key,
                "description": str(meta.get("description") or ""),
                "example": str(meta.get("example") or ""),
            }
        )
    return JSONResponse({"items": items}, headers={"Cache-Control": "no-store"})


@router.get("/runtime-config")
def get_runtime_config(request: Request, scope: str = Query("global")) -> JSONResponse:
    validate_auth(request)

    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

    resolved_scope = str(scope or "").strip() or "global"
    try:
        rows = list_runtime_config(dsn, scopes=[resolved_scope], keys=sorted(DEFAULT_ENV_OVERRIDE_KEYS))
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to load runtime config: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to load runtime config.")
        raise HTTPException(status_code=502, detail=f"Failed to load runtime config: {exc}") from exc

    return JSONResponse(
        {
            "scope": resolved_scope,
            "items": [
                {
                    "scope": item.scope,
                    "key": item.key,
                    "enabled": item.enabled,
                    "value": item.value,
                    "description": item.description,
                    "updatedAt": _iso(item.updated_at),
                    "updatedBy": item.updated_by,
                }
                for item in rows
            ],
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/runtime-config")
def set_runtime_config(payload: RuntimeConfigUpsertRequest, request: Request) -> JSONResponse:
    validate_auth(request)

    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

    key = str(payload.key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="key is required.")
    if key not in DEFAULT_ENV_OVERRIDE_KEYS:
        raise HTTPException(status_code=400, detail="Key is not allowed for DB override.")

    scope = str(payload.scope or "").strip() or "global"
    normalized_value = str(payload.value or "")
    if payload.enabled:
        try:
            normalized_value = normalize_env_override(key, payload.value)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    actor = _get_actor(request)
    try:
        row = upsert_runtime_config(
            dsn=dsn,
            scope=scope,
            key=key,
            enabled=bool(payload.enabled),
            value=normalized_value,
            description=payload.description or RUNTIME_CONFIG_CATALOG.get(key, {}).get("description"),
            actor=actor,
        )
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to update runtime config: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to update runtime config.")
        raise HTTPException(status_code=502, detail=f"Failed to update runtime config: {exc}") from exc

    response_payload = {
        "scope": row.scope,
        "key": row.key,
        "enabled": row.enabled,
        "value": row.value,
        "description": row.description,
        "updatedAt": _iso(row.updated_at),
        "updatedBy": row.updated_by,
    }
    _emit_realtime(
        REALTIME_TOPIC_RUNTIME_CONFIG,
        "RUNTIME_CONFIG_CHANGED",
        {
            "scope": row.scope,
            "key": row.key,
            "enabled": bool(row.enabled),
        },
    )
    return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})


@router.delete("/runtime-config/{key}")
def remove_runtime_config(key: str, request: Request, scope: str = Query("global")) -> JSONResponse:
    validate_auth(request)

    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

    resolved = str(key or "").strip()
    if not resolved:
        raise HTTPException(status_code=400, detail="key is required.")
    if resolved not in DEFAULT_ENV_OVERRIDE_KEYS:
        raise HTTPException(status_code=400, detail="Key is not allowed for DB override.")

    resolved_scope = str(scope or "").strip() or "global"
    try:
        deleted = delete_runtime_config(dsn=dsn, scope=resolved_scope, key=resolved)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to delete runtime config: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to delete runtime config.")
        raise HTTPException(status_code=502, detail=f"Failed to delete runtime config: {exc}") from exc

    response_payload = {"scope": resolved_scope, "key": resolved, "deleted": bool(deleted)}
    _emit_realtime(
        REALTIME_TOPIC_RUNTIME_CONFIG,
        "RUNTIME_CONFIG_CHANGED",
        {
            "scope": resolved_scope,
            "key": resolved,
            "deleted": bool(deleted),
        },
    )
    return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})


class DebugSymbolsUpdateRequest(BaseModel):
    enabled: bool = Field(..., description="When true, apply debug symbols from Postgres to ETL jobs.")
    symbols: Optional[str] = Field(
        default=None,
        description="Comma-separated list or JSON array. When omitted, keeps the stored symbols.",
    )


@router.get("/debug-symbols")
def get_debug_symbols(request: Request) -> JSONResponse:
    validate_auth(request)

    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

    try:
        state = read_debug_symbols_state(dsn)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to load debug symbols: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to load debug symbols.")
        raise HTTPException(status_code=502, detail=f"Failed to load debug symbols: {exc}") from exc

    return JSONResponse(
        {
            "enabled": state.enabled,
            "symbols": state.symbols_raw,
            "updatedAt": _iso(state.updated_at),
            "updatedBy": state.updated_by,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/debug-symbols")
def set_debug_symbols(payload: DebugSymbolsUpdateRequest, request: Request) -> JSONResponse:
    validate_auth(request)

    settings = get_settings(request)
    dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

    try:
        current = read_debug_symbols_state(dsn)
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to load debug symbols: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to load debug symbols.")
        raise HTTPException(status_code=502, detail=f"Failed to load debug symbols: {exc}") from exc

    raw = payload.symbols if payload.symbols is not None else current.symbols_raw
    raw_text = str(raw or "").strip()
    if payload.enabled and not raw_text:
        raise HTTPException(status_code=400, detail="Debug symbols are required when enabled.")

    actor = _get_actor(request)
    try:
        state = update_debug_symbols_state(
            dsn=dsn,
            enabled=payload.enabled,
            symbols=raw_text,
            actor=actor,
        )
    except PostgresError as exc:
        raise HTTPException(status_code=503, detail=f"Failed to update debug symbols: {exc}") from exc
    except Exception as exc:
        logger.exception("Failed to update debug symbols.")
        raise HTTPException(status_code=502, detail=f"Failed to update debug symbols: {exc}") from exc

    response_payload = {
        "enabled": state.enabled,
        "symbols": state.symbols_raw,
        "updatedAt": _iso(state.updated_at),
        "updatedBy": state.updated_by,
    }
    _emit_realtime(
        REALTIME_TOPIC_DEBUG_SYMBOLS,
        "DEBUG_SYMBOLS_CHANGED",
        {
            "enabled": bool(state.enabled),
        },
    )
    return JSONResponse(response_payload, headers={"Cache-Control": "no-store"})


@router.get("/container-apps")
def list_container_apps(
    request: Request,
    probe: bool = Query(True, description="When true, perform live health pings for each app."),
) -> JSONResponse:
    validate_auth(request)

    subscription_id, resource_group, app_allowlist = _container_app_allowlist()
    if not (subscription_id and resource_group and app_allowlist):
        raise HTTPException(status_code=503, detail="Container app monitoring is not configured.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    probe_timeout_env = os.environ.get("SYSTEM_HEALTH_CONTAINERAPP_PING_TIMEOUT_SECONDS")
    try:
        probe_timeout_seconds = float(probe_timeout_env.strip()) if probe_timeout_env else 5.0
    except ValueError:
        probe_timeout_seconds = 5.0

    health_url_overrides = _container_app_health_url_overrides()

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    items: List[Dict[str, Any]] = []
    checked_at = datetime.now(timezone.utc).isoformat()

    with AzureArmClient(cfg) as arm:
        for app_name in app_allowlist:
            resolved = (app_name or "").strip()
            if not resolved:
                continue
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved):
                items.append(
                    {
                        "name": resolved,
                        "status": "error",
                        "error": "Invalid container app name in allowlist.",
                        "health": None,
                        "checkedAt": checked_at,
                    }
                )
                continue

            app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
            try:
                payload = arm.get_json(app_url)
            except Exception as exc:
                items.append(
                    {
                        "name": resolved,
                        "status": "error",
                        "error": f"Failed to read ARM state: {type(exc).__name__}: {exc}",
                        "health": None,
                        "checkedAt": checked_at,
                    }
                )
                continue

            props = _extract_container_app_properties(payload if isinstance(payload, dict) else {})
            status = _resource_status_from_provisioning_state(str(props.get("provisioningState") or ""))
            health_url = _resolve_container_app_health_url(
                resolved,
                ingress_fqdn=props.get("ingressFqdn"),
                overrides=health_url_overrides,
            )
            health: Optional[Dict[str, Any]]
            if probe and health_url:
                health = _probe_container_app_health(health_url, timeout_seconds=probe_timeout_seconds)
                status = _worse_status(status, str(health.get("status") or "unknown"))
            elif probe and not health_url:
                health = {
                    "status": "unknown",
                    "url": None,
                    "httpStatus": None,
                    "checkedAt": checked_at,
                    "error": "No health URL is configured and no ingress FQDN was found.",
                }
            else:
                health = None

            details = f"provisioningState={props.get('provisioningState') or 'Unknown'}"
            if props.get("runningState"):
                details += f", runningState={props.get('runningState')}"
            if props.get("latestReadyRevisionName"):
                details += f", latestReadyRevision={props.get('latestReadyRevisionName')}"

            items.append(
                {
                    "name": resolved,
                    "resourceType": "Microsoft.App/containerApps",
                    "status": status,
                    "details": details,
                    "provisioningState": props.get("provisioningState"),
                    "runningState": props.get("runningState"),
                    "latestReadyRevisionName": props.get("latestReadyRevisionName"),
                    "ingressFqdn": props.get("ingressFqdn"),
                    "azureId": props.get("azureId"),
                    "health": health,
                    "checkedAt": checked_at,
                    "error": None,
                }
            )

    return JSONResponse(
        {
            "probed": bool(probe),
            "apps": items,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.get("/container-apps/{app_name}/logs")
def get_container_app_logs(
    app_name: str,
    request: Request,
    minutes: int = Query(60, ge=1, le=1440),
    tail: int = Query(50, ge=1, le=200),
) -> JSONResponse:
    """
    Returns recent console logs for a specific Container App from Log Analytics.

    Requires:
    - SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID / SYSTEM_HEALTH_ARM_RESOURCE_GROUP / SYSTEM_HEALTH_ARM_CONTAINERAPPS
    - SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=true + SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID
    """
    validate_auth(request)

    subscription_id, resource_group, app_allowlist = _container_app_allowlist()
    if not (subscription_id and resource_group and app_allowlist):
        raise HTTPException(status_code=503, detail="Container app log retrieval is not configured.")

    resolved = (app_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid container app name.")
    if resolved not in app_allowlist:
        raise HTTPException(status_code=404, detail="Container app not found.")

    log_analytics_enabled = _is_truthy(os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED"))
    workspace_id_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
    workspace_id = workspace_id_raw.strip() if workspace_id_raw else ""
    if not log_analytics_enabled or not workspace_id:
        raise HTTPException(status_code=503, detail="Log Analytics is not configured for container app log retrieval.")

    log_timeout_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS")
    try:
        log_timeout_seconds = float(log_timeout_raw.strip()) if log_timeout_raw else 5.0
    except ValueError:
        log_timeout_seconds = 5.0

    now = datetime.now(timezone.utc)
    start = now - timedelta(minutes=max(1, int(minutes)))
    timespan = f"{start.isoformat()}/{now.isoformat()}"

    app_kql = _escape_kql_literal(resolved)
    tail_lines = max(1, int(tail))
    query = f"""
let appName = '{app_kql}';
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs
| extend app = tostring(
    column_ifexists('ContainerAppName_s',
        column_ifexists('ContainerName_s',
            column_ifexists('ContainerName',
                column_ifexists('AppName_s', '')
            )
        )
    )
)
| extend resource = tostring(column_ifexists('_ResourceId', column_ifexists('ResourceId', '')))
| extend msg = tostring(
    column_ifexists('Log_s',
        column_ifexists('Log',
            column_ifexists('LogMessage_s',
                column_ifexists('Message',
                    column_ifexists('message', '')
                )
            )
        )
    )
)
| where (app != '' and app contains appName)
    or (resource contains strcat('/containerApps/', appName))
| where msg != ''
| order by TimeGenerated desc
| take {tail_lines}
| project TimeGenerated, msg
| order by TimeGenerated asc
""".strip()

    try:
        with AzureLogAnalyticsClient(timeout_seconds=log_timeout_seconds) as log_client:
            payload = log_client.query(workspace_id=workspace_id, query=query, timespan=timespan)
            lines = _extract_log_lines(payload)
    except Exception as exc:
        logger.exception("Failed to query container app logs: app=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to query container app logs: {exc}") from exc

    return JSONResponse(
        {
            "appName": resolved,
            "lookbackMinutes": int(minutes),
            "tailLines": tail_lines,
            "logs": lines[-tail_lines:],
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/container-apps/{app_name}/start")
def start_container_app(app_name: str, request: Request) -> JSONResponse:
    validate_auth(request)

    subscription_id, resource_group, app_allowlist = _container_app_allowlist()
    if not (subscription_id and resource_group and app_allowlist):
        raise HTTPException(status_code=503, detail="Container app control is not configured.")

    resolved = (app_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid container app name.")
    if resolved not in app_allowlist:
        raise HTTPException(status_code=404, detail="Container app not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
            payload = arm.post_json(f"{app_url}/start")
    except httpx.HTTPStatusError as exc:
        message = _extract_arm_error_message(exc.response)
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Failed to start container app: {message or str(exc)}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to start container app: {exc}") from exc

    props = _extract_container_app_properties(payload if isinstance(payload, dict) else {})
    response_payload = {
        "appName": resolved,
        "action": "start",
        "provisioningState": props.get("provisioningState"),
        "runningState": props.get("runningState"),
    }
    _emit_realtime(
        REALTIME_TOPIC_CONTAINER_APPS,
        "CONTAINER_APP_STATE_CHANGED",
        response_payload,
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {"source": "container-app-control", "appName": resolved, "action": "start"},
    )
    return JSONResponse(response_payload, status_code=202)


@router.post("/container-apps/{app_name}/stop")
def stop_container_app(app_name: str, request: Request) -> JSONResponse:
    validate_auth(request)

    subscription_id, resource_group, app_allowlist = _container_app_allowlist()
    if not (subscription_id and resource_group and app_allowlist):
        raise HTTPException(status_code=503, detail="Container app control is not configured.")

    resolved = (app_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid container app name.")
    if resolved not in app_allowlist:
        raise HTTPException(status_code=404, detail="Container app not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=resolved)
            payload = arm.post_json(f"{app_url}/stop")
    except httpx.HTTPStatusError as exc:
        message = _extract_arm_error_message(exc.response)
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Failed to stop container app: {message or str(exc)}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to stop container app: {exc}") from exc

    props = _extract_container_app_properties(payload if isinstance(payload, dict) else {})
    response_payload = {
        "appName": resolved,
        "action": "stop",
        "provisioningState": props.get("provisioningState"),
        "runningState": props.get("runningState"),
    }
    _emit_realtime(
        REALTIME_TOPIC_CONTAINER_APPS,
        "CONTAINER_APP_STATE_CHANGED",
        response_payload,
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {"source": "container-app-control", "appName": resolved, "action": "stop"},
    )
    return JSONResponse(response_payload, status_code=202)


@router.post("/jobs/{job_name}/run")
def trigger_job_run(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    control_context = _job_control_context(request)
    logger.info(
        "Trigger job run requested: job=%s actor=%s requestId=%s",
        job_name,
        control_context.get("actor") or "-",
        control_context.get("requestId") or "-",
    )

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""

    job_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    job_allowlist = [item.strip() for item in (job_names_raw or "").split(",") if item.strip()]

    if not (subscription_id and resource_group and job_allowlist):
        raise HTTPException(status_code=503, detail="Azure job triggering is not configured.")

    resolved = (job_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")

    if resolved not in job_allowlist:
        raise HTTPException(status_code=404, detail="Job not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            start_url = f"{job_url}/start"
            payload = arm.post_json(start_url)
    except httpx.HTTPStatusError as exc:
        message = _extract_arm_error_message(exc.response)
        logger.warning(
            "Azure job start failed: job=%s status=%s message=%s",
            resolved,
            exc.response.status_code,
            message or "?",
        )
        if "suspended" in (message or "").lower():
            raise HTTPException(
                status_code=409,
                detail=f"Job is suspended. Resume it, then trigger again. ({message})",
            ) from exc
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Failed to trigger job: {message or str(exc)}",
        ) from exc
    except Exception as exc:
        logger.exception("Failed to trigger Azure job run: job=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to trigger job: {exc}") from exc

    execution_id: Optional[str] = None
    execution_name: Optional[str] = None
    if isinstance(payload, dict):
        execution_id = str(payload.get("id") or "") or None
        execution_name = str(payload.get("name") or "") or None

    logger.info("Triggered Azure job run: job=%s execution=%s", resolved, execution_name or execution_id or "?")
    response_payload = {
        "jobName": resolved,
        "status": "queued",
        "executionId": execution_id,
        "executionName": execution_name,
        "command": "run",
        **control_context,
    }
    _emit_realtime(
        REALTIME_TOPIC_JOBS,
        "JOB_STATE_CHANGED",
        {
            "jobName": resolved,
            "action": "run",
            "command": "run",
            "status": "queued",
            "executionId": execution_id,
            "executionName": execution_name,
            **control_context,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {
            "source": "job-control",
            "jobName": resolved,
            "action": "run",
            "command": "run",
            **control_context,
        },
    )
    return JSONResponse(response_payload, status_code=202)


@router.post("/jobs/{job_name}/suspend")
def suspend_job(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    control_context = _job_control_context(request)
    logger.info(
        "Suspend job requested: job=%s actor=%s requestId=%s",
        job_name,
        control_context.get("actor") or "-",
        control_context.get("requestId") or "-",
    )

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""

    job_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    job_allowlist = [item.strip() for item in (job_names_raw or "").split(",") if item.strip()]

    if not (subscription_id and resource_group and job_allowlist):
        raise HTTPException(status_code=503, detail="Azure job control is not configured.")

    resolved = (job_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")

    if resolved not in job_allowlist:
        raise HTTPException(status_code=404, detail="Job not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            suspend_url = f"{job_url}/suspend"
            payload = arm.post_json(suspend_url)
    except Exception as exc:
        logger.exception("Failed to suspend Azure job: job=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to suspend job: {exc}") from exc

    running_state: Optional[str] = None
    if isinstance(payload, dict):
        props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
        running_state = str(props.get("runningState") or "") or None

    logger.info("Suspended Azure job: job=%s running_state=%s", resolved, running_state or "?")
    response_payload = {
        "jobName": resolved,
        "action": "suspend",
        "runningState": running_state,
        "command": "suspend",
        **control_context,
    }
    _emit_realtime(
        REALTIME_TOPIC_JOBS,
        "JOB_STATE_CHANGED",
        {
            "jobName": resolved,
            "action": "suspend",
            "command": "suspend",
            "runningState": running_state,
            **control_context,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {
            "source": "job-control",
            "jobName": resolved,
            "action": "suspend",
            "command": "suspend",
            **control_context,
        },
    )
    return JSONResponse(response_payload, status_code=202)


@router.post("/jobs/{job_name}/stop")
def stop_job(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    control_context = _job_control_context(request)
    logger.info(
        "Stop job requested: job=%s actor=%s requestId=%s",
        job_name,
        control_context.get("actor") or "-",
        control_context.get("requestId") or "-",
    )

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""

    job_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    job_allowlist = [item.strip() for item in (job_names_raw or "").split(",") if item.strip()]

    if not (subscription_id and resource_group and job_allowlist):
        raise HTTPException(status_code=503, detail="Azure job control is not configured.")

    resolved = (job_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")

    if resolved not in job_allowlist:
        raise HTTPException(status_code=404, detail="Job not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            stop_url = f"{job_url}/stop"
            try:
                payload = arm.post_json(stop_url)
            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                if status_code in {404, 405}:
                    logger.warning(
                        "Stop job endpoint unavailable, falling back to suspend for job=%s status=%s",
                        resolved,
                        status_code,
                    )
                    payload = arm.post_json(f"{job_url}/suspend")
                else:
                    message = _extract_arm_error_message(exc.response)
                    raise HTTPException(
                        status_code=exc.response.status_code,
                        detail=f"Failed to stop job: {message or str(exc)}",
                    ) from exc
    except Exception as exc:
        logger.exception("Failed to stop Azure job: job=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to stop job: {exc}") from exc

    running_state: Optional[str] = None
    if isinstance(payload, dict):
        props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
        running_state = str(props.get("runningState") or "") or None

    logger.info("Stopped Azure job: job=%s running_state=%s", resolved, running_state or "?")
    response_payload = {
        "jobName": resolved,
        "action": "stop",
        "runningState": running_state,
        "command": "stop",
        **control_context,
    }
    _emit_realtime(
        REALTIME_TOPIC_JOBS,
        "JOB_STATE_CHANGED",
        {
            "jobName": resolved,
            "action": "stop",
            "command": "stop",
            "runningState": running_state,
            **control_context,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {
            "source": "job-control",
            "jobName": resolved,
            "action": "stop",
            "command": "stop",
            **control_context,
        },
    )
    return JSONResponse(response_payload, status_code=202)


@router.post("/jobs/{job_name}/resume")
def resume_job(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    control_context = _job_control_context(request)
    logger.info(
        "Resume job requested: job=%s actor=%s requestId=%s",
        job_name,
        control_context.get("actor") or "-",
        control_context.get("requestId") or "-",
    )

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""

    job_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    job_allowlist = [item.strip() for item in (job_names_raw or "").split(",") if item.strip()]

    if not (subscription_id and resource_group and job_allowlist):
        raise HTTPException(status_code=503, detail="Azure job control is not configured.")

    resolved = (job_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")

    if resolved not in job_allowlist:
        raise HTTPException(status_code=404, detail="Job not found.")

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            resume_url = f"{job_url}/resume"
            payload = arm.post_json(resume_url)
    except Exception as exc:
        logger.exception("Failed to resume Azure job: job=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to resume job: {exc}") from exc

    running_state: Optional[str] = None
    if isinstance(payload, dict):
        props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
        running_state = str(props.get("runningState") or "") or None

    logger.info("Resumed Azure job: job=%s running_state=%s", resolved, running_state or "?")
    response_payload = {
        "jobName": resolved,
        "action": "resume",
        "runningState": running_state,
        "command": "resume",
        **control_context,
    }
    _emit_realtime(
        REALTIME_TOPIC_JOBS,
        "JOB_STATE_CHANGED",
        {
            "jobName": resolved,
            "action": "resume",
            "command": "resume",
            "runningState": running_state,
            **control_context,
        },
    )
    _emit_realtime(
        REALTIME_TOPIC_SYSTEM_HEALTH,
        "SYSTEM_HEALTH_UPDATE",
        {
            "source": "job-control",
            "jobName": resolved,
            "action": "resume",
            "command": "resume",
            **control_context,
        },
    )
    return JSONResponse(response_payload, status_code=202)


def _is_truthy(raw: Optional[str]) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _escape_kql_literal(value: str) -> str:
    return str(value or "").replace("'", "''")


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _extract_log_lines(payload: Dict[str, Any]) -> List[str]:
    tables = payload.get("tables") if isinstance(payload.get("tables"), list) else []
    if not tables or not isinstance(tables[0], dict):
        return []
    table = tables[0]
    columns = table.get("columns") if isinstance(table.get("columns"), list) else []
    rows = table.get("rows") if isinstance(table.get("rows"), list) else []

    name_to_idx: Dict[str, int] = {}
    for idx, col in enumerate(columns):
        if not isinstance(col, dict):
            continue
        name = str(col.get("name") or "").strip()
        if name:
            name_to_idx[name] = idx

    msg_idx = name_to_idx.get("msg")
    if msg_idx is None:
        # Be resilient to schema differences / casing in Log Analytics responses.
        lowered = {name.lower(): idx for name, idx in name_to_idx.items()}
        msg_idx = lowered.get("msg")

    if msg_idx is None:
        for candidate in ("Log_s", "Log", "LogMessage_s", "Message", "message"):
            if candidate in name_to_idx:
                msg_idx = name_to_idx[candidate]
                break

    if msg_idx is None:
        lowered = {name.lower(): idx for name, idx in name_to_idx.items()}
        for candidate in ("log_s", "log", "logmessage_s", "message"):
            if candidate in lowered:
                msg_idx = lowered[candidate]
                break

    if msg_idx is None:
        return []

    out: List[str] = []
    for row in rows:
        if not isinstance(row, list) or msg_idx >= len(row):
            continue
        value = row[msg_idx]
        if value is None:
            continue
        out.append(str(value))
    return out


@router.get("/jobs/{job_name}/logs")
def get_job_logs(
    job_name: str,
    request: Request,
    runs: int = Query(1, ge=1, le=10),
) -> JSONResponse:
    """
    Returns the tail of console logs for the last N Container App Job executions (default: 1).

    Requires:
    - SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID / SYSTEM_HEALTH_ARM_RESOURCE_GROUP / SYSTEM_HEALTH_ARM_JOBS (allowlist)
    - SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=true + SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID
    """
    validate_auth(request)

    subscription_id_raw = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
    subscription_id = subscription_id_raw.strip() if subscription_id_raw else ""
    resource_group_raw = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
    resource_group = resource_group_raw.strip() if resource_group_raw else ""

    job_names_raw = os.environ.get("SYSTEM_HEALTH_ARM_JOBS")
    job_allowlist = [item.strip() for item in (job_names_raw or "").split(",") if item.strip()]

    if not (subscription_id and resource_group and job_allowlist):
        raise HTTPException(status_code=503, detail="Azure job log retrieval is not configured.")

    resolved = (job_name or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?", resolved or ""):
        raise HTTPException(status_code=400, detail="Invalid job name.")

    if resolved not in job_allowlist:
        raise HTTPException(status_code=404, detail="Job not found.")

    log_analytics_enabled = _is_truthy(os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED"))
    workspace_id_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
    workspace_id = workspace_id_raw.strip() if workspace_id_raw else ""
    if not log_analytics_enabled or not workspace_id:
        raise HTTPException(status_code=503, detail="Log Analytics is not configured for job log retrieval.")

    log_timeout_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS")
    try:
        log_timeout_seconds = float(log_timeout_raw.strip()) if log_timeout_raw else 5.0
    except ValueError:
        log_timeout_seconds = 5.0

    api_version_env = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION")
    api_version = api_version_env.strip() if api_version_env else ""
    if not api_version:
        api_version = ArmConfig.api_version

    timeout_env = os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS")
    try:
        timeout_seconds = float(timeout_env.strip()) if timeout_env else 5.0
    except ValueError:
        timeout_seconds = 5.0

    cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            executions_url = f"{job_url}/executions"
            exec_payload = arm.get_json(executions_url)
    except Exception as exc:
        logger.exception("Failed to list Azure job executions: job=%s", resolved)
        raise HTTPException(status_code=502, detail=f"Failed to list job executions: {exc}") from exc

    now = datetime.now(timezone.utc)
    values = exec_payload.get("value") if isinstance(exec_payload.get("value"), list) else []
    executions: List[Dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        props = item.get("properties") if isinstance(item.get("properties"), dict) else {}
        start_time = str(props.get("startTime") or "")
        end_time = str(props.get("endTime") or "")
        executions.append(
            {
                "executionName": str(item.get("name") or "") or None,
                "executionId": str(item.get("id") or "") or None,
                "status": str(props.get("status") or "") or None,
                "startTime": start_time or None,
                "endTime": end_time or None,
                "_start_ts": (_parse_dt(start_time) or now).timestamp(),
            }
        )

    executions.sort(key=lambda e: float(e.get("_start_ts") or 0.0), reverse=True)
    selected = executions[: max(0, int(runs))]

    tail_lines = 10

    out_runs: List[Dict[str, Any]] = []
    with AzureLogAnalyticsClient(timeout_seconds=log_timeout_seconds) as log_client:
        for run in selected:
            exec_name = str(run.get("executionName") or "").strip()
            start_dt = _parse_dt(str(run.get("startTime") or "")) or now
            end_dt = _parse_dt(str(run.get("endTime") or "")) or now
            if end_dt < start_dt:
                end_dt = now

            # Keep query windows bounded.
            start = start_dt - timedelta(minutes=5)
            end = end_dt + timedelta(minutes=10)
            if end - start > timedelta(hours=24):
                start = end - timedelta(hours=24)

            timespan = f"{start.isoformat()}/{end.isoformat()}"

            job_kql = _escape_kql_literal(resolved)
            exec_kql = _escape_kql_literal(exec_name)
            query = f"""
let jobName = '{job_kql}';
let execName = '{exec_kql}';
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs
| extend job = tostring(
    column_ifexists('ContainerJobName_s',
        column_ifexists('ContainerName_s',
            column_ifexists('ContainerAppJobName_s',
                column_ifexists('JobName_s',
                    column_ifexists('JobName',
                        column_ifexists('ContainerAppName_s', '')
                    )
                )
            )
        )
    )
)
| extend exec = tostring(
    column_ifexists('ContainerGroupName_s',
        column_ifexists('ContainerGroupName',
            column_ifexists('ContainerAppJobExecutionName_s',
                column_ifexists('ExecutionName_s',
                    column_ifexists('ExecutionName',
                        column_ifexists('ContainerGroupId_g',
                            column_ifexists('ContainerAppJobExecutionId_g',
                                column_ifexists('ContainerAppJobExecutionId_s', '')
                            )
                        )
                    )
                )
            )
        )
    )
)
| extend resource = tostring(column_ifexists('_ResourceId', column_ifexists('ResourceId', '')))
| extend msg = tostring(
    column_ifexists('Log_s',
        column_ifexists('Log',
            column_ifexists('LogMessage_s',
                column_ifexists('Message',
                    column_ifexists('message', '')
                )
            )
        )
    )
)
| extend jobMatch = (job != '' and job contains jobName) or (resource contains jobName)
| extend execMatch = execName != '' and ((exec != '' and exec contains execName) or (resource contains execName))
| where jobMatch or execMatch
| order by execMatch desc, jobMatch desc, TimeGenerated desc
| take {tail_lines}
| project TimeGenerated, msg
| order by TimeGenerated asc
""".strip()

            try:
                payload = log_client.query(workspace_id=workspace_id, query=query, timespan=timespan)
                lines = _extract_log_lines(payload)
                err = None
            except Exception as exc:
                lines = []
                err = str(exc)

            out_runs.append(
                {
                    "executionName": run.get("executionName"),
                    "executionId": run.get("executionId"),
                    "status": run.get("status"),
                    "startTime": run.get("startTime"),
                    "endTime": run.get("endTime"),
                    "tail": lines,
                    "error": err,
                }
            )

    # Strip internal parse helper key
    for item in selected:
        item.pop("_start_ts", None)

    return JSONResponse(
        {
            "jobName": resolved,
            "runsRequested": int(runs),
            "runsReturned": len(out_runs),
            "tailLines": tail_lines,
            "runs": out_runs,
        },
        headers={"Cache-Control": "no-store"},
    )
