from __future__ import annotations

import os
import re
from typing import Optional

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


def trigger_containerapp_job_start(*, job_name: str, required: bool = True) -> None:
    resolved = (job_name or "").strip()
    if not resolved or not _JOB_NAME_RE.fullmatch(resolved):
        raise ValueError(f"Invalid job name: {job_name!r}")

    cfg = _get_arm_cfg()
    if cfg is None:
        mdc.write_line("Skipping job trigger (ARM config not provided).")
        return

    mdc.write_line(f"Triggering downstream job: {resolved}")
    try:
        with AzureArmClient(cfg) as arm:
            job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=resolved)
            start_url = f"{job_url}/start"
            arm.post_json(start_url)
    except Exception as exc:
        mdc.write_error(f"Failed to trigger downstream job '{resolved}': {exc}")
        if required:
            raise
    else:
        mdc.write_line(f"Downstream job triggered: {resolved}")


def trigger_next_job_from_env() -> None:
    next_job = (os.environ.get("TRIGGER_NEXT_JOB_NAME") or "").strip()
    if not next_job:
        return

    required = _parse_bool(os.environ.get("TRIGGER_NEXT_JOB_REQUIRED"), default=True)
    trigger_containerapp_job_start(job_name=next_job, required=required)
