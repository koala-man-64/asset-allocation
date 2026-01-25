import copy
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

from api.service.dependencies import (
    get_auth_manager,
    get_settings,
    get_system_health_cache,
    validate_auth,
)
from api.service.secure_links import LinkTokenError, build_link_token, resolve_link_token
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


def _azure_portal_url(azure_id: str) -> Optional[str]:
    text = str(azure_id or "").strip()
    if not text:
        return None
    if not text.startswith("/"):
        text = f"/{text}"
    return f"https://portal.azure.com/#resource{text}"


def _apply_link_tokens(payload: Dict[str, Any]) -> None:
    link_tokens_disabled = False
    stats: Dict[str, Dict[str, int]] = {
        "layer": {"seen": 0, "has_url": 0, "tokenized": 0},
        "domain_folder": {"seen": 0, "has_url": 0, "tokenized": 0},
        "domain_base": {"seen": 0, "has_url": 0, "tokenized": 0},
        "domain_job": {"seen": 0, "has_url": 0, "tokenized": 0},
        "job_execution": {"seen": 0, "has_url": 0, "tokenized": 0},
        "resource": {"seen": 0, "has_url": 0, "tokenized": 0},
    }
    token_errors = 0

    def _maybe_tokenize(url: Optional[str], *, context: str, kind: str) -> Optional[str]:
        nonlocal link_tokens_disabled
        nonlocal token_errors
        stats[kind]["seen"] += 1
        if not url:
            return None
        stats[kind]["has_url"] += 1
        try:
            token = build_link_token(url)
        except LinkTokenError as exc:
            token_errors += 1
            logger.warning("Link token error: context=%s error=%s", context, exc)
            return None
        if token is None:
            if not link_tokens_disabled:
                logger.warning("Link tokens disabled: SYSTEM_HEALTH_LINK_TOKEN_SECRET not set.")
                link_tokens_disabled = True
            return None
        stats[kind]["tokenized"] += 1
        return token

    layers = payload.get("dataLayers")
    if isinstance(layers, list):
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            layer_token = _maybe_tokenize(
                layer.pop("portalUrl", None),
                context=f"layer:{layer.get('name')}",
                kind="layer",
            )
            if layer_token:
                layer["portalLinkToken"] = layer_token
            domains = layer.get("domains")
            if isinstance(domains, list):
                for domain in domains:
                    if not isinstance(domain, dict):
                        continue
                    folder_token = _maybe_tokenize(
                        domain.pop("portalUrl", None),
                        context=f"domain:{domain.get('name')}",
                        kind="domain_folder",
                    )
                    if folder_token:
                        domain["portalLinkToken"] = folder_token
                    base_token = _maybe_tokenize(
                        domain.pop("basePortalUrl", None),
                        context=f"domain_base:{domain.get('name')}",
                        kind="domain_base",
                    )
                    if base_token:
                        domain["basePortalLinkToken"] = base_token
                    job_token = _maybe_tokenize(
                        domain.pop("jobUrl", None),
                        context=f"job:{domain.get('jobName')}",
                        kind="domain_job",
                    )
                    if job_token:
                        domain["jobLinkToken"] = job_token

    resources = payload.get("resources")
    if isinstance(resources, list):
        for resource in resources:
            if not isinstance(resource, dict):
                continue
            azure_id = resource.get("azureId")
            portal_url = _azure_portal_url(azure_id) if azure_id else None
            portal_token = _maybe_tokenize(
                portal_url,
                context=f"resource:{resource.get('name')}",
                kind="resource",
            )
            if portal_token:
                resource["portalLinkToken"] = portal_token

    jobs = payload.get("recentJobs")
    if isinstance(jobs, list):
        for job in jobs:
            if not isinstance(job, dict):
                continue
            execution_token = _maybe_tokenize(
                job.pop("logUrl", None),
                context=f"execution:{job.get('jobName')}:{job.get('startTime')}",
                kind="job_execution",
            )
            if execution_token:
                job["logLinkToken"] = execution_token

    # Emit a compact summary so missing portal/job icons can be diagnosed from logs.
    totals = {k: dict(v) for k, v in stats.items()}
    tokenized_total = sum(item["tokenized"] for item in stats.values())
    has_url_total = sum(item["has_url"] for item in stats.values())
    if tokenized_total == 0 and has_url_total == 0:
        logger.info(
            "System health links: no URLs present to tokenize "
            "(check SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID/SYSTEM_HEALTH_ARM_RESOURCE_GROUP/AZURE_STORAGE_ACCOUNT_NAME)."
        )
    elif tokenized_total == 0 and has_url_total > 0:
        logger.warning(
            "System health links: URLs present but none tokenized (check SYSTEM_HEALTH_LINK_TOKEN_SECRET/allowlist/ttl)."
        )

    logger.info(
        "System health link tokenization: layer=%s/%s domain_folder=%s/%s domain_base=%s/%s domain_job=%s/%s job_execution=%s/%s resource=%s/%s errors=%s",
        totals["layer"]["tokenized"],
        totals["layer"]["has_url"],
        totals["domain_folder"]["tokenized"],
        totals["domain_folder"]["has_url"],
        totals["domain_base"]["tokenized"],
        totals["domain_base"]["has_url"],
        totals["domain_job"]["tokenized"],
        totals["domain_job"]["has_url"],
        totals["job_execution"]["tokenized"],
        totals["job_execution"]["has_url"],
        totals["resource"]["tokenized"],
        totals["resource"]["has_url"],
        token_errors,
    )


def _link_requires_auth() -> bool:
    raw = os.environ.get("SYSTEM_HEALTH_LINK_REQUIRE_AUTH", "").strip().lower()
    return raw in {"1", "true", "t", "yes", "y", "on"}


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

    payload: Dict[str, Any] = copy.deepcopy(result.value or {})
    _apply_link_tokens(payload)

    logger.info(
        "System health payload ready: cache_hit=%s refresh_error=%s layers=%s resources=%s",
        result.cache_hit,
        bool(result.refresh_error),
        len(payload.get("dataLayers") or []),
        len(payload.get("resources") or []),
    )

    headers: Dict[str, str] = {
        "Cache-Control": "no-store",
        "X-System-Health-Cache": "hit" if result.cache_hit else "miss",
    }
    if result.refresh_error:
        headers["X-System-Health-Stale"] = "1"
    return JSONResponse(payload, headers=headers)


@router.get("/links/{token}")
def resolve_link(token: str, request: Request) -> RedirectResponse:
    actor = None
    if _link_requires_auth():
        validate_auth(request)
        actor = _get_actor(request)
    try:
        url = resolve_link_token(token)
    except LinkTokenError as exc:
        logger.warning("Link token resolve failed: actor=%s error=%s", actor or "-", exc)
        raise HTTPException(status_code=404, detail="Link not available.") from exc

    host = urlparse(url).hostname or "-"
    logger.info("Link token resolved: actor=%s host=%s", actor or "-", host)
    return RedirectResponse(url=url, status_code=307, headers={"Cache-Control": "no-store"})


class LinkResolveRequest(BaseModel):
    token: str


@router.post("/links/resolve")
def resolve_link_url(payload: LinkResolveRequest, request: Request) -> JSONResponse:
    actor = None
    if _link_requires_auth():
        validate_auth(request)
        actor = _get_actor(request)

    try:
        url = resolve_link_token(payload.token)
    except LinkTokenError as exc:
        logger.warning("Link token resolve failed (json): actor=%s error=%s", actor or "-", exc)
        raise HTTPException(status_code=404, detail="Link not available.") from exc

    host = urlparse(url).hostname or "-"
    logger.info("Link token resolved (json): actor=%s host=%s", actor or "-", host)
    return JSONResponse({"url": url}, headers={"Cache-Control": "no-store"})


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
