import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

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
from monitoring.log_analytics import AzureLogAnalyticsClient
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
        try:
            states = alert_store.get_states(alert_ids)
        except Exception as exc:
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
union isfuzzy=true ContainerAppConsoleLogs_CL, ContainerAppConsoleLogs, ContainerAppSystemLogs_CL, ContainerAppSystemLogs
| extend job = tostring(column_ifexists('ContainerAppJobName_s', column_ifexists('JobName_s', '')))
| extend exec = tostring(column_ifexists('ContainerAppJobExecutionName_s', column_ifexists('ExecutionName_s', '')))
| extend msg = tostring(column_ifexists('Log_s', column_ifexists('Log', column_ifexists('LogMessage_s', column_ifexists('Message', column_ifexists('message', ''))))))
| where job == jobName and (execName == '' or exec == execName)
| project TimeGenerated, msg
| order by TimeGenerated desc
| take {tail_lines}
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
