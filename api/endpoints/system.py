import logging
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Literal, Tuple

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from api.service.dependencies import (
    get_alert_state_store,
    get_auth_manager,
    get_settings,
    get_system_health_cache,
    validate_auth,
)
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.lineage import get_lineage_snapshot
from monitoring.domain_metadata import collect_domain_metadata
from monitoring.log_analytics import AzureLogAnalyticsClient
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache
from core.blob_storage import BlobStorageClient
from core.debug_symbols import read_debug_symbols_state, update_debug_symbols_state
from core.postgres import PostgresError
from core.runtime_config import (
    DEFAULT_ENV_OVERRIDE_KEYS,
    delete_runtime_config,
    list_runtime_config,
    normalize_env_override,
    upsert_runtime_config,
)

logger = logging.getLogger("asset-allocation.api.system")

router = APIRouter()


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
        "System health payload ready: cache_hit=%s refresh_error=%s layers=%s alerts=%s resources=%s",
        result.cache_hit,
        bool(result.refresh_error),
        len(payload.get("dataLayers") or []),
        len(payload.get("alerts") or []),
        len(payload.get("resources") or []),
    )

    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-System-Health-Cache": "hit" if result.cache_hit else "miss",
    }
    if result.refresh_error:
        headers["X-System-Health-Stale"] = "1"
    return JSONResponse(payload, headers=headers)


class DomainDateRange(BaseModel):
    min: Optional[str] = None
    max: Optional[str] = None
    column: Optional[str] = None


class DomainMetadataResponse(BaseModel):
    layer: str
    domain: str
    container: str
    type: Literal["blob", "delta"]
    computedAt: str
    symbolCount: Optional[int] = None
    dateRange: Optional[DomainDateRange] = None
    totalRows: Optional[int] = None
    fileCount: Optional[int] = None
    totalBytes: Optional[int] = None
    deltaVersion: Optional[int] = None
    tablePath: Optional[str] = None
    prefix: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


@router.get("/domain-metadata", response_model=DomainMetadataResponse)
def domain_metadata(
    request: Request,
    layer: str = Query(..., description="Medallion layer key (bronze|silver|gold|platinum)"),
    domain: str = Query(..., description="Domain key (market|finance|earnings|price-target|platinum)"),
) -> JSONResponse:
    validate_auth(request)
    try:
        payload = collect_domain_metadata(layer=layer, domain=domain)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Domain metadata collection failed: layer=%s domain=%s", layer, domain)
        raise HTTPException(status_code=503, detail=f"Domain metadata unavailable: {exc}") from exc

    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


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
    return JSONResponse(
        {
            "alertId": state.alert_id,
            "acknowledgedAt": _iso(state.acknowledged_at),
            "acknowledgedBy": state.acknowledged_by,
            "snoozedUntil": _iso(state.snoozed_until),
            "resolvedAt": _iso(state.resolved_at),
            "resolvedBy": state.resolved_by,
        }
    )


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

    return JSONResponse(
        {
            "alertId": state.alert_id,
            "acknowledgedAt": _iso(state.acknowledged_at),
            "acknowledgedBy": state.acknowledged_by,
            "snoozedUntil": _iso(state.snoozed_until),
            "resolvedAt": _iso(state.resolved_at),
            "resolvedBy": state.resolved_by,
        }
    )


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
    return JSONResponse(
        {
            "alertId": state.alert_id,
            "acknowledgedAt": _iso(state.acknowledged_at),
            "acknowledgedBy": state.acknowledged_by,
            "snoozedUntil": _iso(state.snoozed_until),
            "resolvedAt": _iso(state.resolved_at),
            "resolvedBy": state.resolved_by,
        }
    )


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
        "market": ["market-data/", "market-data-by-date"],
        "finance": ["finance-data/", "finance-data-by-date"],
        "earnings": ["earnings-data/", "earnings-data-by-date"],
        "price-target": ["price-target-data/", "price-target-data-by-date"],
    },
    "gold": {
        "market": ["market/", "market_by_date"],
        "finance": ["finance/", "finance_by_date"],
        "earnings": ["earnings/", "earnings_by_date"],
        "price-target": ["targets/", "targets_by_date"],
    },
    "platinum": {
        "platinum": ["platinum/"],
    },
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


@router.post("/purge")
def purge_data(payload: PurgeRequest, request: Request) -> JSONResponse:
    validate_auth(request)
    if not payload.confirm:
        raise HTTPException(status_code=400, detail="Confirmation required to purge data.")

    targets = _resolve_purge_targets(payload.scope, payload.layer, payload.domain)

    planned: List[Tuple[BlobStorageClient, Dict[str, Optional[str]]]] = []
    any_data = False
    for target in targets:
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

        planned.append((client, target))
        any_data = any_data or has_data
        target["hasData"] = bool(has_data)

    if not any_data:
        raise HTTPException(status_code=409, detail="Nothing to purge for the selected scope.")

    results: List[Dict[str, Any]] = []
    total_deleted = 0

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

        results.append(
            {
                "container": container,
                "prefix": prefix,
                "layer": target.get("layer"),
                "domain": target.get("domain"),
                "deleted": deleted,
            }
        )
        total_deleted += int(deleted or 0)

    logger.warning(
        "Purge completed: scope=%s layer=%s domain=%s targets=%s deleted=%s",
        payload.scope,
        payload.layer,
        payload.domain,
        len(results),
        total_deleted,
    )

    return JSONResponse(
        {
            "scope": payload.scope,
            "layer": payload.layer,
            "domain": payload.domain,
            "totalDeleted": total_deleted,
            "targets": results,
        }
    )


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
        "example": "120",
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
    "BACKFILL_START_DATE": {
        "description": "Optional inclusive start date for backfill runs (YYYY-MM-DD).",
        "example": "2024-01-01",
    },
    "BACKFILL_END_DATE": {
        "description": "Optional inclusive end date for backfill runs (YYYY-MM-DD).",
        "example": "2024-03-31",
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
    "SILVER_EARNINGS_LATEST_ONLY": {
        "description": "Domain override for earnings silver latest-only behavior.",
        "example": "true",
    },
    "SILVER_PRICE_TARGET_LATEST_ONLY": {
        "description": "Domain override for price-target silver latest-only behavior.",
        "example": "true",
    },
    "MATERIALIZE_YEAR_MONTH": {
        "description": "Override year-month partition for by-date materialization (YYYY-MM).",
        "example": "2026-01",
    },
    "MATERIALIZE_WINDOW_MONTHS": {
        "description": "How many year-month partitions to materialize (integer).",
        "example": "1",
    },
    "MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR": {
        "description": "Optional UTC hour gate for by-date runs (0-23). Empty disables gating.",
        "example": "0",
    },
    "FEATURE_ENGINEERING_MAX_WORKERS": {
        "description": "Max workers for feature engineering concurrency (integer).",
        "example": "8",
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
    "SYSTEM_HEALTH_TTL_SECONDS": {
        "description": "System health cache TTL for the API (float seconds).",
        "example": "300",
    },
    "SYSTEM_HEALTH_MAX_AGE_SECONDS": {
        "description": "Max staleness window before marking layers stale (integer seconds).",
        "example": "129600",
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

    return JSONResponse(
        {
            "scope": row.scope,
            "key": row.key,
            "enabled": row.enabled,
            "value": row.value,
            "description": row.description,
            "updatedAt": _iso(row.updated_at),
            "updatedBy": row.updated_by,
        },
        headers={"Cache-Control": "no-store"},
    )


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

    return JSONResponse(
        {"scope": resolved_scope, "key": resolved, "deleted": bool(deleted)},
        headers={"Cache-Control": "no-store"},
    )


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

    return JSONResponse(
        {
            "enabled": state.enabled,
            "symbols": state.symbols_raw,
            "updatedAt": _iso(state.updated_at),
            "updatedBy": state.updated_by,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.post("/jobs/{job_name}/run")
def trigger_job_run(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    logger.info("Trigger job run requested: job=%s", job_name)

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
    return JSONResponse(
        {
            "jobName": resolved,
            "status": "queued",
            "executionId": execution_id,
            "executionName": execution_name,
        },
        status_code=202,
    )


@router.post("/jobs/{job_name}/suspend")
def suspend_job(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    logger.info("Suspend job requested: job=%s", job_name)

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
    return JSONResponse(
        {
            "jobName": resolved,
            "action": "suspend",
            "runningState": running_state,
        },
        status_code=202,
    )


@router.post("/jobs/{job_name}/resume")
def resume_job(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)
    logger.info("Resume job requested: job=%s", job_name)

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
    return JSONResponse(
        {
            "jobName": resolved,
            "action": "resume",
            "runningState": running_state,
        },
        status_code=202,
    )


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
