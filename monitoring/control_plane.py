from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

from monitoring.arm_client import AzureArmClient
from monitoring.resource_health import DEFAULT_RESOURCE_HEALTH_API_VERSION, get_current_availability


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration_seconds(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if not start or not end:
        return None
    seconds = int((end - start).total_seconds())
    return seconds if seconds >= 0 else None


def _map_job_execution_status(raw: str) -> str:
    status = (raw or "").strip().lower()
    if status in {"succeeded", "success"}:
        return "success"
    if status in {"failed", "error"}:
        return "failed"
    if status in {"running", "processing"}:
        return "running"
    if status in {"stopped"}:
        return "failed"
    return "pending"


def _job_type_from_name(job_name: str) -> str:
    text = (job_name or "").lower()
    if "backtest" in text:
        return "backtest"
    if "risk" in text:
        return "risk-calc"
    if "attribution" in text:
        return "attribution"
    if "rank" in text or "signal" in text or "portfolio" in text:
        return "portfolio-build"
    return "data-ingest"


def _resource_status_from_provisioning_state(state: str, *, has_ready_signal: bool = True) -> Tuple[str, str]:
    raw = (state or "").strip()
    normalized = raw.lower()
    if normalized == "succeeded":
        return ("healthy" if has_ready_signal else "warning"), raw or "Succeeded"
    if normalized in {"failed", "canceled", "cancelled"}:
        return "error", raw or "Failed"
    if normalized in {"creating", "updating", "deleting", "inprogress"}:
        return "warning", raw or "InProgress"
    if not raw:
        return "unknown", "Unknown"
    return "warning", raw


def _combine_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


@dataclass(frozen=True)
class ResourceHealthItem:
    name: str
    resource_type: str
    status: str  # healthy|warning|error|unknown
    last_checked: str
    details: str
    azure_id: Optional[str] = None
    running_state: Optional[str] = None
    signals: Tuple[Dict[str, Any], ...] = ()

    def to_dict(self, *, include_ids: bool) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.name,
            "resourceType": self.resource_type,
            "status": self.status,
            "lastChecked": self.last_checked,
            "details": self.details,
        }
        if include_ids and self.azure_id:
            payload["azureId"] = self.azure_id
        if self.running_state:
            payload["runningState"] = self.running_state
        if self.signals:
            payload["signals"] = list(self.signals)
        return payload


def collect_container_apps(
    arm: AzureArmClient,
    *,
    app_names: Sequence[str],
    last_checked_iso: str,
    include_ids: bool,
    resource_health_enabled: bool = False,
    resource_health_api_version: str = DEFAULT_RESOURCE_HEALTH_API_VERSION,
) -> List[ResourceHealthItem]:
    items: List[ResourceHealthItem] = []
    for name in app_names:
        url = arm.resource_url(provider="Microsoft.App", resource_type="containerApps", name=name)
        try:
            payload = arm.get_json(url)
            props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
            provisioning_state = str(props.get("provisioningState") or "")
            latest_ready = str(props.get("latestReadyRevisionName") or "")
            status, state_text = _resource_status_from_provisioning_state(
                provisioning_state, has_ready_signal=bool(latest_ready)
            )
            resource_id = str(payload.get("id") or "") or None
            details = f"provisioningState={state_text}"
            if latest_ready:
                details += f", latestReadyRevision={latest_ready}"

            if resource_health_enabled and resource_id:
                signal, availability_status = get_current_availability(
                    arm, resource_id=resource_id, api_version=resource_health_api_version
                )
                if signal is not None:
                    status = _combine_status(status, availability_status)
                    details += f", {signal.to_details_fragment()}"
            items.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/containerApps",
                    status=status,
                    last_checked=last_checked_iso,
                    details=details,
                    azure_id=resource_id,
                )
            )
        except Exception as exc:
            items.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/containerApps",
                    status="error",
                    last_checked=last_checked_iso,
                    details=f"probe_error={exc}",
                    azure_id=None,
                )
            )
    return items


def collect_jobs_and_executions(
    arm: AzureArmClient,
    *,
    job_names: Sequence[str],
    last_checked_iso: str,
    include_ids: bool,
    max_executions_per_job: int = 3,
    resource_health_enabled: bool = False,
    resource_health_api_version: str = DEFAULT_RESOURCE_HEALTH_API_VERSION,
) -> Tuple[List[ResourceHealthItem], List[Dict[str, Any]]]:
    resources: List[ResourceHealthItem] = []
    runs: List[Dict[str, Any]] = []

    for name in job_names:
        job_url = arm.resource_url(provider="Microsoft.App", resource_type="jobs", name=name)
        try:
            job_payload = arm.get_json(job_url)
            job_props = job_payload.get("properties") if isinstance(job_payload.get("properties"), dict) else {}
            provisioning_state = str(job_props.get("provisioningState") or "")
            status, state_text = _resource_status_from_provisioning_state(provisioning_state, has_ready_signal=True)

            resource_id = str(job_payload.get("id") or "") or None
            details = f"provisioningState={state_text}"
            running_state_raw = str(job_props.get("runningState") or "").strip()
            running_state = running_state_raw or None
            if running_state:
                details += f", runningState={running_state}"
            if resource_health_enabled and resource_id:
                signal, availability_status = get_current_availability(
                    arm, resource_id=resource_id, api_version=resource_health_api_version
                )
                if signal is not None:
                    status = _combine_status(status, availability_status)
                    details += f", {signal.to_details_fragment()}"
            resources.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/jobs",
                    status=status,
                    last_checked=last_checked_iso,
                    details=details,
                    azure_id=resource_id,
                    running_state=running_state,
                )
            )
        except Exception as exc:
            resources.append(
                ResourceHealthItem(
                    name=name,
                    resource_type="Microsoft.App/jobs",
                    status="error",
                    last_checked=last_checked_iso,
                    details=f"probe_error={exc}",
                    azure_id=None,
                )
            )
            continue

        executions_url = f"{job_url}/executions"
        try:
            exec_payload = arm.get_json(executions_url)
            values = exec_payload.get("value") if isinstance(exec_payload.get("value"), list) else []
            for item in values[:max_executions_per_job]:
                if not isinstance(item, dict):
                    continue
                props = item.get("properties") if isinstance(item.get("properties"), dict) else {}
                raw_status = str(props.get("status") or "")
                start_time = str(props.get("startTime") or "")
                end_time = str(props.get("endTime") or "")

                start_dt = _parse_dt(start_time)
                end_dt = _parse_dt(end_time)
                duration = _duration_seconds(start_dt, end_dt)

                runs.append(
                    {
                        "jobName": name,
                        "jobType": _job_type_from_name(name),
                        "status": _map_job_execution_status(raw_status),
                        "startTime": start_dt.isoformat() if start_dt else start_time or last_checked_iso,
                        "duration": duration,
                        "triggeredBy": "azure",
                    }
                )
        except Exception:
            # Executions are best-effort; rely on job resource status + alerts in aggregation.
            continue

    runs.sort(key=lambda r: r.get("startTime", ""), reverse=True)
    return resources, runs
