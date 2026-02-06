from __future__ import annotations

import os
import re
import time
from typing import Optional

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
        return status in {429, 500, 502, 503, 504}
    return False


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
