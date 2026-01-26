import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

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
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("asset-allocation.api.system")

router = APIRouter()


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
    logger.info(
        "System health request: refresh=%s path=%s host=%s fwd=%s",
        refresh,
        request.url.path,
        request.headers.get("host", ""),
        request.headers.get("x-forwarded-for", ""),
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
        states = alert_store.get_states(alert_ids)

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


class SnoozeRequest(BaseModel):
    minutes: Optional[int] = Field(default=None, ge=1, le=7 * 24 * 60)
    until: Optional[datetime] = None


def _require_alert_store(request: Request):
    store = get_alert_state_store(request)
    if store is None:
        raise HTTPException(
            status_code=503,
            detail="Alert lifecycle persistence is not configured (BACKTEST_POSTGRES_DSN).",
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

    logger.info("Snooze alert: id=%s actor=%s minutes=%s until=%s", alert_id, actor or "-", payload.minutes, payload.until)
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
