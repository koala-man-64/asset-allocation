import logging
import os
import re
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from backtest.service.dependencies import get_settings, get_system_health_cache, validate_auth
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.system_health import collect_system_health_snapshot
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("backtest.api.system")

router = APIRouter()


@router.get("/health")
def system_health(request: Request, refresh: bool = Query(False)) -> JSONResponse:
    logger.info(f"Accessing /system/health endpoint (refresh={refresh})")
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
        raise HTTPException(status_code=503, detail=f"System health unavailable: {exc}") from exc

    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-System-Health-Cache": "hit" if result.cache_hit else "miss",
    }
    if result.refresh_error:
        headers["X-System-Health-Stale"] = "1"
    return JSONResponse(result.value, headers=headers)


@router.post("/jobs/{job_name}/run")
def trigger_job_run(job_name: str, request: Request) -> JSONResponse:
    validate_auth(request)

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
