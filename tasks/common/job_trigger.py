from __future__ import annotations

import os
import re
import time
from typing import Optional
from urllib.parse import urlparse

import httpx

from core import core as mdc
from monitoring.arm_client import ArmConfig, AzureArmClient


_JOB_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,126}[A-Za-z0-9]?")


def _parse_bool(raw: Optional[str], *, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def _parse_int(raw: Optional[str], *, default: int, minimum: int) -> int:
    try:
        value = int((raw or "").strip()) if raw is not None and str(raw).strip() else default
    except ValueError:
        value = default
    return max(minimum, value)


def _parse_float(raw: Optional[str], *, default: float, minimum: float) -> float:
    try:
        value = float((raw or "").strip()) if raw is not None and str(raw).strip() else default
    except ValueError:
        value = default
    return max(minimum, value)


def _parse_csv(raw: Optional[str]) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def _resolve_api_base_url() -> str:
    return (os.environ.get("ASSET_ALLOCATION_API_BASE_URL") or os.environ.get("ASSET_ALLOCATION_API_URL") or "").strip()


def _normalize_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return f"http://{value}"
    return value


def _resolve_api_health_url(base_url: str) -> str:
    normalized = _normalize_url(base_url)
    parsed = urlparse(normalized)
    if not parsed.scheme or not parsed.netloc:
        return ""

    health_path = (os.environ.get("JOB_STARTUP_API_HEALTH_PATH") or "/healthz").strip() or "/healthz"
    if not health_path.startswith("/"):
        health_path = f"/{health_path}"
    return f"{parsed.scheme}://{parsed.netloc}{health_path}"


def _get_startup_probe_config() -> tuple[int, float, float]:
    attempts = _parse_int(os.environ.get("JOB_STARTUP_API_PROBE_ATTEMPTS"), default=6, minimum=1)
    sleep_seconds = _parse_float(os.environ.get("JOB_STARTUP_API_PROBE_SLEEP_SECONDS"), default=10.0, minimum=0.1)
    timeout_seconds = _parse_float(os.environ.get("JOB_STARTUP_API_PROBE_TIMEOUT_SECONDS"), default=5.0, minimum=0.5)
    return attempts, sleep_seconds, timeout_seconds


def _get_startup_start_retry_config() -> tuple[int, float]:
    attempts = _parse_int(os.environ.get("JOB_STARTUP_API_START_ATTEMPTS"), default=3, minimum=1)
    base_seconds = _parse_float(os.environ.get("JOB_STARTUP_API_START_BASE_SECONDS"), default=1.0, minimum=0.1)
    return attempts, base_seconds


def _resolve_startup_container_apps(base_url: str) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()

    def _append(raw_name: str) -> None:
        name = str(raw_name or "").strip()
        if not name or not _JOB_NAME_RE.fullmatch(name):
            return
        normalized = name.lower()
        if normalized in seen:
            return
        seen.add(normalized)
        resolved.append(name)

    for item in _parse_csv(os.environ.get("JOB_STARTUP_API_CONTAINER_APPS")):
        _append(item)

    for key in ("API_CONTAINER_APP_NAME", "CONTAINER_APP_API_NAME"):
        _append(os.environ.get(key) or "")

    allowlist = _parse_csv(os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS"))

    parsed = urlparse(_normalize_url(base_url))
    host = str(parsed.hostname or "").strip().lower()
    if host and "." not in host:
        _append(host)
    elif host:
        for allowed in allowlist:
            value = allowed.strip().lower()
            if not value:
                continue
            if host == value or host.startswith(f"{value}."):
                _append(allowed)
                break

    if not resolved and len(allowlist) == 1:
        _append(allowlist[0])

    return resolved


def _probe_health(*, health_url: str, timeout_seconds: float) -> tuple[bool, str]:
    try:
        response = httpx.get(health_url, timeout=timeout_seconds, follow_redirects=True)
        status = int(response.status_code)
        if 200 <= status < 300:
            return True, f"status={status}"
        return False, f"status={status}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _start_container_app(*, app_name: str, cfg: ArmConfig, required: bool = True) -> bool:
    attempts, base_delay = _get_startup_start_retry_config()
    for attempt in range(1, attempts + 1):
        try:
            with AzureArmClient(cfg) as arm:
                app_url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=app_name)
                arm.post_json(f"{app_url}/start")
            mdc.write_line(f"Container app start requested: {app_name}")
            return True
        except httpx.HTTPStatusError as exc:
            status = int(exc.response.status_code)
            if status == 409:
                # Start is idempotent; 409 typically means the app is already running.
                mdc.write_line(f"Container app already running (start not required): {app_name}")
                return True
            retryable = _is_retryable(exc)
            mdc.write_warning(
                f"Failed to start container app '{app_name}' (attempt {attempt}/{attempts}, status={status}): {exc}"
            )
            if not retryable or attempt >= attempts:
                if required:
                    raise
                return False
            sleep_seconds = base_delay * (2 ** (attempt - 1))
            mdc.write_line(f"Retrying container app start in {sleep_seconds:.1f}s...")
            time.sleep(sleep_seconds)
        except Exception as exc:
            retryable = _is_retryable(exc)
            mdc.write_warning(f"Failed to start container app '{app_name}' (attempt {attempt}/{attempts}): {exc}")
            if not retryable or attempt >= attempts:
                if required:
                    raise
                return False
            sleep_seconds = base_delay * (2 ** (attempt - 1))
            mdc.write_line(f"Retrying container app start in {sleep_seconds:.1f}s...")
            time.sleep(sleep_seconds)
    return False


def _get_arm_cfg() -> Optional[ArmConfig]:
    subscription_id = (os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID") or os.environ.get("AZURE_SUBSCRIPTION_ID") or "").strip()
    resource_group = (os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP") or os.environ.get("RESOURCE_GROUP") or "").strip()

    if not (subscription_id and resource_group):
        return None

    api_version = (os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION") or "").strip() or ArmConfig.api_version

    timeout_seconds_raw = (os.environ.get("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS") or "").strip()
    try:
        timeout_seconds = float(timeout_seconds_raw) if timeout_seconds_raw else 5.0
    except ValueError:
        timeout_seconds = 5.0

    return ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )


def _get_retry_config() -> tuple[int, float]:
    attempts_raw = (os.environ.get("TRIGGER_NEXT_JOB_RETRY_ATTEMPTS") or "").strip()
    base_raw = (os.environ.get("TRIGGER_NEXT_JOB_RETRY_BASE_SECONDS") or "").strip()
    try:
        attempts = int(attempts_raw) if attempts_raw else 3
    except ValueError:
        attempts = 3
    try:
        base_seconds = float(base_raw) if base_raw else 1.0
    except ValueError:
        base_seconds = 1.0
    return max(1, attempts), max(0.1, base_seconds)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, httpx.RequestError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status in {408, 429, 500, 502, 503, 504}
    return False


def ensure_api_awake_from_env(*, required: Optional[bool] = None) -> None:
    enabled = _parse_bool(os.environ.get("JOB_STARTUP_API_WAKE_ENABLED"), default=True)
    if not enabled:
        mdc.write_line("Skipping startup API wake check (JOB_STARTUP_API_WAKE_ENABLED=false).")
        return

    required_resolved = (
        required
        if required is not None
        else _parse_bool(os.environ.get("JOB_STARTUP_API_REQUIRED"), default=False)
    )

    base_url = _resolve_api_base_url()
    if not base_url:
        message = "Skipping startup API wake check (ASSET_ALLOCATION_API_BASE_URL is not configured)."
        if required_resolved:
            raise RuntimeError(message)
        mdc.write_line(message)
        return

    health_url = _resolve_api_health_url(base_url)
    if not health_url:
        message = f"Skipping startup API wake check (invalid ASSET_ALLOCATION_API_BASE_URL={base_url!r})."
        if required_resolved:
            raise RuntimeError(message)
        mdc.write_warning(message)
        return

    probe_attempts, probe_sleep_seconds, probe_timeout_seconds = _get_startup_probe_config()
    arm_start_enabled = _parse_bool(os.environ.get("JOB_STARTUP_API_ARM_START_ENABLED"), default=True)
    arm_start_attempted = False

    for probe_attempt in range(1, probe_attempts + 1):
        healthy, detail = _probe_health(health_url=health_url, timeout_seconds=probe_timeout_seconds)
        if healthy:
            if probe_attempt == 1:
                mdc.write_line(f"Startup API health probe succeeded ({detail}).")
            else:
                mdc.write_line(f"Startup API became healthy after {probe_attempt} attempts ({detail}).")
            return

        mdc.write_warning(f"Startup API health probe failed (attempt {probe_attempt}/{probe_attempts}, {detail}).")

        if (not arm_start_attempted) and arm_start_enabled:
            arm_start_attempted = True
            app_names = _resolve_startup_container_apps(base_url)
            if not app_names:
                mdc.write_warning(
                    "No startup container app target resolved. Set JOB_STARTUP_API_CONTAINER_APPS or API_CONTAINER_APP_NAME."
                )
            else:
                cfg = _get_arm_cfg()
                if cfg is None:
                    mdc.write_warning("Skipping startup container app start (ARM config not provided).")
                else:
                    for app_name in app_names:
                        _start_container_app(app_name=app_name, cfg=cfg, required=required_resolved)

        if probe_attempt < probe_attempts:
            mdc.write_line(f"Retrying startup API health probe in {probe_sleep_seconds:.1f}s...")
            time.sleep(probe_sleep_seconds)

    message = (
        "Startup API did not become healthy after "
        f"{probe_attempts} attempts (health_url={health_url})."
    )
    if required_resolved:
        raise RuntimeError(message)
    mdc.write_warning(message)


def trigger_containerapp_job_start(*, job_name: str, required: bool = True) -> None:
    resolved = (job_name or "").strip()
    if not resolved or not _JOB_NAME_RE.fullmatch(resolved):
        raise ValueError(f"Invalid job name: {job_name!r}")

    cfg = _get_arm_cfg()
    if cfg is None:
        mdc.write_line("Skipping job trigger (ARM config not provided).")
        return

    mdc.write_line(f"Triggering downstream job: {resolved}")
    attempts, base_delay = _get_retry_config()
    for attempt in range(1, attempts + 1):
        try:
            with AzureArmClient(cfg) as arm:
                job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
                start_url = f"{job_url}/start"
                arm.post_json(start_url)
            mdc.write_line(f"Downstream job triggered: {resolved}")
            return
        except Exception as exc:
            retryable = _is_retryable(exc)
            mdc.write_error(f"Failed to trigger downstream job '{resolved}' (attempt {attempt}/{attempts}): {exc}")
            if not retryable or attempt >= attempts:
                if required:
                    raise
                return
            sleep_seconds = base_delay * (2 ** (attempt - 1))
            mdc.write_line(f"Retrying downstream trigger in {sleep_seconds:.1f}s...")
            time.sleep(sleep_seconds)


def trigger_next_job_from_env() -> None:
    raw_next_jobs = (os.environ.get("TRIGGER_NEXT_JOB_NAME") or "").strip()
    if not raw_next_jobs:
        return

    required = _parse_bool(os.environ.get("TRIGGER_NEXT_JOB_REQUIRED"), default=True)
    
    # Support multiple comma-separated jobs
    next_jobs = [j.strip() for j in raw_next_jobs.split(",") if j.strip()]
    
    for job_name in next_jobs:
        trigger_containerapp_job_start(job_name=job_name, required=required)
