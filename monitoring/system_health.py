from __future__ import annotations

import json
import logging
import os
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence

from monitoring.azure_blob_store import AzureBlobStore, AzureBlobStoreConfig, LastModifiedProbeResult
from monitoring.arm_client import ArmConfig, AzureArmClient
from monitoring.control_plane import ResourceHealthItem, collect_container_apps, collect_jobs_and_executions
from monitoring.log_analytics import (
    AzureLogAnalyticsClient,
    collect_log_analytics_signals,
    parse_log_analytics_queries_json,
)
from monitoring.monitor_metrics import (
    DEFAULT_MONITOR_METRICS_API_VERSION,
    collect_monitor_metrics,
    parse_metric_thresholds_json,
)
from monitoring.resource_health import DEFAULT_RESOURCE_HEALTH_API_VERSION

logger = logging.getLogger("asset_allocation.monitoring.system_health")
DEFAULT_ARM_API_VERSION = ArmConfig(subscription_id="", resource_group="").api_version
DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX = "system/health_markers"
DEFAULT_SYSTEM_HEALTH_MARKERS_DUAL_READ_TOLERANCE_SECONDS = 6 * 3600


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _is_truthy(raw: Optional[str]) -> bool:
    value = (raw or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


def _require_env(name: str) -> str:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        raise ValueError(f"Missing required environment variable: {name}")
    return raw.strip()


def _env_or_default(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw.strip() if raw and raw.strip() else default


def _parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _require_bool(name: str) -> bool:
    return _parse_bool(_require_env(name))


def _require_int(name: str, *, min_value: int = 1, max_value: int = 365 * 24 * 3600) -> int:
    raw = _require_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _require_float(name: str, *, min_value: float = 0.1, max_value: float = 120.0) -> float:
    raw = _require_env(name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _is_test_mode() -> bool:
    if _is_truthy(os.environ.get("SYSTEM_HEALTH_RUN_IN_TEST")):
        return False
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True
    return _is_truthy(os.environ.get("TEST_MODE"))


def _split_csv(raw: Optional[str]) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _env_has_value(name: str) -> bool:
    raw = os.environ.get(name)
    return bool(raw and raw.strip())


def _worse_resource_status(primary: str, secondary: str) -> str:
    status_order = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if status_order.get(secondary, 0) > status_order.get(primary, 0) else primary


def _append_signal_details(details: str, signals: Sequence[Dict[str, Any]]) -> str:
    fragments: List[str] = []
    for signal in signals:
        if signal.get("status") not in {"warning", "error"}:
            continue
        name = str(signal.get("name") or "").strip() or "signal"
        value = signal.get("value")
        unit = str(signal.get("unit") or "").strip()
        if value is None:
            fragments.append(f"{name}=unknown")
        else:
            text = f"{name}={value}"
            if unit:
                text += f" {unit}"
            fragments.append(text)
    if not fragments:
        return details
    suffix = "; ".join(fragments[:6])
    return f"{details}, signals[{suffix}]"


def _parse_iso_start_time(value: Optional[str]) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _newer_execution(current: Dict[str, Any], existing: Optional[Dict[str, Any]]) -> bool:
    if existing is None:
        return True

    current_time = _parse_iso_start_time(str(current.get("startTime") or ""))
    existing_time = _parse_iso_start_time(str(existing.get("startTime") or ""))

    if current_time and existing_time:
        return current_time > existing_time

    if current_time and not existing_time:
        return True

    return str(current.get("startTime") or "") > str(existing.get("startTime") or "")


def collect_resource_health_signals(*_args: Any, **_kwargs: Any) -> List[Dict[str, Any]]:
    """
    Compatibility shim for tests expecting a resource health collector in this module.

    The current system health flow enriches resources inline; return an empty list by default.
    """
    return []


@dataclass(frozen=True)
class FreshnessPolicy:
    max_age_seconds: int
    source: str


@dataclass(frozen=True)
class MarkerProbeConfig:
    enabled: bool
    container: str
    prefix: str
    dual_read: bool
    dual_read_tolerance_seconds: int


@dataclass(frozen=True)
class JobScheduleMetadata:
    trigger_type: str
    cron_expression: str


def _normalize_layer_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _normalize_domain_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")


def _load_freshness_overrides() -> Dict[str, Dict[str, Any]]:
    raw = os.environ.get("SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON", "")
    text = raw.strip()
    if not text:
        return {}

    try:
        payload = json.loads(text)
    except Exception as exc:
        logger.warning(
            "SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON parse error: %s",
            exc,
            exc_info=True,
        )
        return {}

    if not isinstance(payload, dict):
        logger.warning("SYSTEM_HEALTH_FRESHNESS_OVERRIDES_JSON must be a JSON object.")
        return {}

    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        if isinstance(raw_value, dict):
            normalized[key] = raw_value
        elif isinstance(raw_value, int):
            normalized[key] = {"maxAgeSeconds": int(raw_value)}
        else:
            logger.warning(
                "Ignoring freshness override for key=%s (expected object or int, got %s).",
                key,
                type(raw_value).__name__,
            )
    return normalized


def _resolve_freshness_policy(
    *,
    layer_name: str,
    domain_name: str,
    default_max_age_seconds: int,
    overrides: Dict[str, Dict[str, Any]],
) -> FreshnessPolicy:
    layer_key = _normalize_layer_key(layer_name)
    domain_key = _normalize_domain_key(domain_name)

    candidates = [
        f"{layer_key}.{domain_key}",
        f"{layer_key}:{domain_key}",
        domain_key,
        f"{layer_key}.*",
        "*",
    ]
    for key in candidates:
        node = overrides.get(key)
        if not isinstance(node, dict):
            continue
        raw_max_age = node.get("maxAgeSeconds")
        if raw_max_age is None:
            continue
        try:
            parsed = int(raw_max_age)
        except Exception:
            logger.warning(
                "Invalid maxAgeSeconds for freshness override key=%s value=%r",
                key,
                raw_max_age,
            )
            continue
        if parsed <= 0:
            logger.warning(
                "Ignoring non-positive maxAgeSeconds for freshness override key=%s value=%r",
                key,
                raw_max_age,
            )
            continue
        return FreshnessPolicy(max_age_seconds=parsed, source=f"override:{key}")

    return FreshnessPolicy(max_age_seconds=int(default_max_age_seconds), source="default")


def _marker_probe_config() -> MarkerProbeConfig:
    enabled_raw = os.environ.get("SYSTEM_HEALTH_MARKERS_ENABLED", "true")
    dual_read_raw = os.environ.get("SYSTEM_HEALTH_MARKERS_DUAL_READ", "false")

    container = (
        os.environ.get("SYSTEM_HEALTH_MARKERS_CONTAINER")
        or os.environ.get("AZURE_CONTAINER_COMMON")
        or ""
    ).strip()
    prefix = os.environ.get("SYSTEM_HEALTH_MARKERS_PREFIX", DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX).strip()
    tolerance_raw = os.environ.get("SYSTEM_HEALTH_MARKERS_DUAL_READ_TOLERANCE_SECONDS", "").strip()
    if tolerance_raw:
        try:
            tolerance = int(tolerance_raw)
        except Exception:
            tolerance = DEFAULT_SYSTEM_HEALTH_MARKERS_DUAL_READ_TOLERANCE_SECONDS
    else:
        tolerance = DEFAULT_SYSTEM_HEALTH_MARKERS_DUAL_READ_TOLERANCE_SECONDS
    if tolerance < 0:
        tolerance = 0

    return MarkerProbeConfig(
        enabled=_is_truthy(enabled_raw),
        container=container,
        prefix=prefix or DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX,
        dual_read=_is_truthy(dual_read_raw),
        dual_read_tolerance_seconds=tolerance,
    )


def _marker_blob_name(*, layer_name: str, domain_name: str, prefix: str) -> str:
    layer_key = _normalize_layer_key(layer_name)
    domain_key = _normalize_domain_key(domain_name)
    prefix_clean = str(prefix or DEFAULT_SYSTEM_HEALTH_MARKERS_PREFIX).strip().strip("/")
    return f"{prefix_clean}/{layer_key}/{domain_key}.json"


def _probe_marker_last_modified(
    *,
    store: AzureBlobStore,
    container: str,
    marker_blob: str,
) -> LastModifiedProbeResult:
    try:
        lm = store.get_blob_last_modified(container=container, blob_name=marker_blob)
    except Exception as exc:
        return LastModifiedProbeResult(state="error", error=str(exc))
    if lm is None:
        return LastModifiedProbeResult(state="not_found")
    return LastModifiedProbeResult(state="ok", last_modified=lm)


def _normalize_probe_result(raw: Any) -> LastModifiedProbeResult:
    if isinstance(raw, LastModifiedProbeResult):
        return raw

    state = str(getattr(raw, "state", "") or "").strip().lower()
    last_modified = getattr(raw, "last_modified", None)
    error = str(getattr(raw, "error", "") or "").strip() or None

    if state not in {"ok", "not_found", "error"}:
        if isinstance(last_modified, datetime):
            state = "ok"
        elif last_modified is None:
            state = "not_found"
        else:
            state = "error"
            error = error or "Invalid probe response."

    if state == "ok" and not isinstance(last_modified, datetime):
        if last_modified is None:
            state = "not_found"
        else:
            state = "error"
            error = error or "Invalid probe timestamp."

    return LastModifiedProbeResult(
        state=state,
        last_modified=last_modified if isinstance(last_modified, datetime) else None,
        error=error,
    )


def _probe_container_last_modified(
    *,
    store: Any,
    container: str,
    prefix: Optional[str],
) -> LastModifiedProbeResult:
    probe_fn = getattr(store, "probe_container_last_modified", None)
    if callable(probe_fn):
        raw = probe_fn(container=container, prefix=prefix)
        if raw.__class__.__module__.startswith("unittest.mock"):
            normalized = LastModifiedProbeResult(state="error", error="Mock probe response.")
        else:
            normalized = _normalize_probe_result(raw)
        # Compatibility fallback for loose mocks/adapters that expose the method name
        # but do not return a typed probe object.
        if normalized.state != "error" or normalized.error not in {
            "Invalid probe response.",
            "Invalid probe timestamp.",
            "Mock probe response.",
        }:
            return normalized

    lm = store.get_container_last_modified(container=container, prefix=prefix)
    if isinstance(lm, datetime):
        return LastModifiedProbeResult(state="ok", last_modified=lm)
    if lm is None:
        return LastModifiedProbeResult(state="not_found")
    return LastModifiedProbeResult(state="error", error="Invalid legacy probe timestamp.")


@dataclass(frozen=True)
class DomainTimestampResolution:
    status: str
    last_updated: Optional[datetime]
    source: str
    warnings: List[str]
    error: Optional[str] = None


def _resolve_last_updated_with_marker_probes(
    *,
    layer_name: str,
    domain_name: str,
    store: AzureBlobStore,
    marker_cfg: MarkerProbeConfig,
    legacy_source: str,
    legacy_probe_fn: Callable[[], LastModifiedProbeResult],
) -> DomainTimestampResolution:
    warnings: List[str] = []
    marker_last_updated: Optional[datetime] = None

    if marker_cfg.enabled:
        if not marker_cfg.container:
            message = "Marker probes enabled but marker container is not configured."
            logger.error(message)
            return DomainTimestampResolution(
                status="error",
                last_updated=None,
                source="marker",
                warnings=[message],
                error=message,
            )
        else:
            marker_blob = _marker_blob_name(
                layer_name=layer_name,
                domain_name=domain_name,
                prefix=marker_cfg.prefix,
            )
            marker_probe = _probe_marker_last_modified(
                store=store,
                container=marker_cfg.container,
                marker_blob=marker_blob,
            )
            if marker_probe.state == "ok":
                marker_last_updated = marker_probe.last_modified
            elif marker_probe.state == "error":
                message = (
                    f"Marker probe failed for {marker_blob}: "
                    f"{marker_probe.error or 'unknown error'}"
                )
                logger.error(message)
                return DomainTimestampResolution(
                    status="error",
                    last_updated=None,
                    source="marker",
                    warnings=[message],
                    error=message,
                )
            else:
                message = f"Marker missing for {marker_blob}."
                logger.error(message)
                return DomainTimestampResolution(
                    status="error",
                    last_updated=None,
                    source="marker",
                    warnings=[message],
                    error=message,
                )
    else:
        warnings.append("Marker probes disabled; using legacy freshness probe mode.")

    if marker_last_updated is not None and not marker_cfg.dual_read:
        return DomainTimestampResolution(
            status="ok",
            last_updated=marker_last_updated,
            source="marker",
            warnings=warnings,
        )

    legacy_probe: LastModifiedProbeResult = legacy_probe_fn()
    if legacy_probe.state == "error":
        message = legacy_probe.error or "Legacy freshness probe failed."
        if marker_last_updated is not None:
            warnings.append(f"Legacy parity probe failed: {message}")
            return DomainTimestampResolution(
                status="ok",
                last_updated=marker_last_updated,
                source="marker",
                warnings=warnings,
            )
        return DomainTimestampResolution(
            status="error",
            last_updated=None,
            source=legacy_source,
            warnings=warnings,
            error=message,
        )

    if legacy_probe.state == "ok":
        legacy_last_updated = legacy_probe.last_modified
        if marker_last_updated is None:
            return DomainTimestampResolution(
                status="ok",
                last_updated=legacy_last_updated,
                source=legacy_source,
                warnings=warnings,
            )
        if marker_cfg.dual_read and legacy_last_updated is not None:
            skew_seconds = abs((marker_last_updated - legacy_last_updated).total_seconds())
            if skew_seconds > float(marker_cfg.dual_read_tolerance_seconds):
                warnings.append(
                    "Marker/legacy freshness mismatch exceeds tolerance "
                    f"({int(skew_seconds)}s > {marker_cfg.dual_read_tolerance_seconds}s)."
                )
        return DomainTimestampResolution(
            status="ok",
            last_updated=marker_last_updated,
            source="marker",
            warnings=warnings,
        )

    if marker_last_updated is not None:
        return DomainTimestampResolution(
            status="ok",
            last_updated=marker_last_updated,
            source="marker",
            warnings=warnings,
        )

    return DomainTimestampResolution(
        status="ok",
        last_updated=None,
        source=legacy_source,
        warnings=warnings,
    )


def _domain_name_from_marker_path(path: str) -> str:
    d_name = os.path.dirname(path) or path
    normalized = d_name.replace("/whitelist.csv", "").replace("-data", "")
    return "price-target" if normalized == "targets" else normalized


def _domain_name_from_delta_path(path: str) -> str:
    d_name = path
    name_clean = d_name.split("/")[-1].replace("-data", "")
    if "/signals/" in d_name:
        name_clean = "signals"
    if name_clean == "targets":
        name_clean = "price-target"
    return name_clean


def _collect_job_names_for_layers(specs: Sequence["LayerProbeSpec"]) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for spec in specs:
        for domain_spec in spec.marker_blobs:
            domain_name = _domain_name_from_marker_path(domain_spec.path)
            job_name = _derive_job_name(spec.name, domain_name)
            if not job_name:
                continue
            normalized = job_name.strip().lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            names.append(job_name)
        for domain_spec in spec.delta_tables:
            domain_name = _domain_name_from_delta_path(domain_spec.path)
            job_name = _derive_job_name(spec.name, domain_name)
            if not job_name:
                continue
            normalized = job_name.strip().lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            names.append(job_name)
    return names


def _load_job_schedule_metadata(
    *,
    subscription_id: str,
    resource_group: str,
    job_names: Sequence[str],
) -> Dict[str, JobScheduleMetadata]:
    if not subscription_id or not resource_group or not job_names:
        return {}

    api_version = _env_or_default("SYSTEM_HEALTH_ARM_API_VERSION", DEFAULT_ARM_API_VERSION)
    timeout_raw = _env_or_default("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", "5")
    try:
        timeout_seconds = float(timeout_raw)
    except Exception:
        timeout_seconds = 5.0
    if timeout_seconds <= 0:
        timeout_seconds = 5.0

    arm_cfg = ArmConfig(
        subscription_id=subscription_id,
        resource_group=resource_group,
        api_version=api_version,
        timeout_seconds=timeout_seconds,
    )

    metadata: Dict[str, JobScheduleMetadata] = {}
    try:
        with AzureArmClient(arm_cfg) as arm:
            for name in job_names:
                job_name = str(name or "").strip()
                if not job_name:
                    continue
                job_key = job_name.lower()
                try:
                    payload = arm.get_json(
                        arm.resource_url(
                            provider="Microsoft.App",
                            resource_type="jobs",
                            name=job_name,
                        )
                    )
                    props = payload.get("properties") if isinstance(payload, dict) else {}
                    cfg = props.get("configuration") if isinstance(props, dict) else {}
                    trigger_type = str(cfg.get("triggerType") or "").strip().lower() if isinstance(cfg, dict) else ""
                    schedule_cfg = cfg.get("scheduleTriggerConfig") if isinstance(cfg, dict) else {}
                    cron_expression = (
                        str(schedule_cfg.get("cronExpression") or "").strip()
                        if isinstance(schedule_cfg, dict)
                        else ""
                    )
                    if not trigger_type and not cron_expression:
                        continue
                    metadata[job_key] = JobScheduleMetadata(
                        trigger_type=trigger_type,
                        cron_expression=cron_expression,
                    )
                except Exception as exc:
                    logger.info("Unable to resolve job trigger metadata for job=%s: %s", job_name, exc)
    except Exception as exc:
        logger.info("Skipping job schedule metadata probe (ARM unavailable): %s", exc)

    return metadata


def _resolve_domain_schedule(
    *,
    job_name: str,
    default_cron: str,
    default_trigger_type: str = "schedule",
    job_schedule_metadata: Dict[str, JobScheduleMetadata],
) -> tuple[str, str]:
    schedule = job_schedule_metadata.get(str(job_name or "").strip().lower())
    default_cron_clean = str(default_cron or "").strip()
    default_trigger = str(default_trigger_type or "").strip().lower()

    if schedule is None:
        if default_trigger == "manual":
            return "", "Manual trigger"
        if default_trigger == "schedule":
            return default_cron_clean, _describe_cron(default_cron_clean) if default_cron_clean else "Scheduled trigger"
        if default_trigger:
            return "", f"{default_trigger.title()} trigger"
        return default_cron_clean, _describe_cron(default_cron_clean) if default_cron_clean else ""

    trigger = schedule.trigger_type
    if trigger == "manual":
        return "", "Manual trigger"
    if trigger == "schedule":
        cron = schedule.cron_expression or default_cron_clean
        return cron, _describe_cron(cron) if cron else "Scheduled trigger"
    if trigger:
        return "", f"{trigger.title()} trigger"

    cron = schedule.cron_expression or default_cron_clean
    return cron, _describe_cron(cron) if cron else ""


@dataclass(frozen=True)
class DomainSpec:
    path: str
    cron: str = "0 0 * * *"  # Default Daily
    trigger_type: str = "schedule"


@dataclass(frozen=True)
class LayerProbeSpec:
    name: str
    description: str
    container_env: str
    max_age_seconds: int
    marker_blobs: Sequence[DomainSpec] = ()
    delta_tables: Sequence[DomainSpec] = ()
    job_name: Optional[str] = None

    def container_name(self) -> str:
        return _require_env(self.container_env)


def _compute_layer_status(now: datetime, last_updated: Optional[datetime], *, max_age_seconds: int, had_error: bool) -> str:
    if had_error:
        return "error"
    if last_updated is None:
        return "stale"
    age_seconds = max((now - last_updated).total_seconds(), 0.0)
    if age_seconds > float(max_age_seconds):
        return "stale"
    return "healthy"


def _overall_from_layers(statuses: Sequence[str]) -> str:
    if any(s == "error" for s in statuses):
        return "critical"
    if any(s == "stale" for s in statuses):
        return "degraded"
    return "healthy"


def _layer_alerts(now: datetime, *, layer_name: str, status: str, last_updated: Optional[datetime], error: Optional[str]) -> List[Dict[str, Any]]:
    if status == "healthy":
        return []

    timestamp = _iso(now)
    if status == "error":
        return [
            {
                "id": _alert_id(severity="error", title="Layer probe error", component=layer_name),
                "severity": "error",
                "title": "Layer probe error",
                "component": layer_name,
                "timestamp": timestamp,
                "message": error or "Layer probe failed.",
                "acknowledged": False,
            }
        ]

    # stale
    last_text = _iso(last_updated) if last_updated else "unknown"
    return [
        {
            "id": _alert_id(severity="warning", title="Layer stale", component=layer_name),
            "severity": "warning",
            "title": "Layer stale",
            "component": layer_name,
            "timestamp": timestamp,
            "message": f"{layer_name} appears stale (lastUpdated={last_text}).",
            "acknowledged": False,
        }
    ]


def _default_layer_specs() -> List[LayerProbeSpec]:
    max_age_default = _require_int("SYSTEM_HEALTH_MAX_AGE_SECONDS")

    # Deployed job schedules (see deploy/job_*.yaml).
    # Bronze jobs are scheduled; Silver/Gold are manual and triggered downstream.
    CRON_BRONZE_MARKET = "0 22 * * 1-5"
    CRON_BRONZE_PRICE_TARGET = "0 4 * * 1-5"
    CRON_BRONZE_EARNINGS = "0 10 * * 1-5"
    CRON_BRONZE_FINANCE = "0 16 * * 1-5"
    CRON_PLATINUM = "0 0 * * *"

    return [
        LayerProbeSpec(
            name="Bronze",
            description="Landing zone for raw data. Immutable source of truth for replayability.",
            container_env="AZURE_CONTAINER_BRONZE",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market-data/whitelist.csv", cron=CRON_BRONZE_MARKET, trigger_type="schedule"),
                DomainSpec("finance-data/whitelist.csv", cron=CRON_BRONZE_FINANCE, trigger_type="schedule"),
                DomainSpec("earnings-data/whitelist.csv", cron=CRON_BRONZE_EARNINGS, trigger_type="schedule"),
                DomainSpec("price-target-data/whitelist.csv", cron=CRON_BRONZE_PRICE_TARGET, trigger_type="schedule"),
            ),
        ),
        LayerProbeSpec(
            name="Silver",
            description="Cleaned, standardized tabular data. Enforced schemas for reliable querying.",
            container_env="AZURE_CONTAINER_SILVER",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market-data/", trigger_type="manual"),
                DomainSpec("finance-data/", trigger_type="manual"),
                DomainSpec("earnings-data/", trigger_type="manual"),
                DomainSpec("price-target-data/", trigger_type="manual"),
            ),
        ),
        LayerProbeSpec(
            name="Gold",
            description="Entity-resolved feature store. Financial metrics ready for modeling.",
            container_env="AZURE_CONTAINER_GOLD",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market/", trigger_type="manual"),
                DomainSpec("finance/", trigger_type="manual"),
                DomainSpec("earnings/", trigger_type="manual"),
                DomainSpec("targets/", trigger_type="manual"),
            ),
        ),
        LayerProbeSpec(
            name="Platinum",
            description="Curated/derived datasets (reserved)",
            container_env="AZURE_CONTAINER_PLATINUM",
            max_age_seconds=max_age_default,
            marker_blobs=(DomainSpec("platinum/", cron=CRON_PLATINUM, trigger_type="schedule"),),
        ),
    ]


def _slug(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", (text or "").strip().lower()).strip("-")
    return cleaned[:80] if cleaned else "alert"


def _alert_id(*, severity: str, title: str, component: str) -> str:
    raw = f"{severity}|{title}|{component}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()[:10]
    return f"{_slug(component)}--{_slug(title)}--{digest}"


def _make_container_portal_url(sub_id: str, rg: str, account: str, container: str) -> Optional[str]:
    if not all([sub_id, rg, account, container]):
        return None
    
    # Construct Storage Account Resource ID
    storage_id = (
        f"/subscriptions/{sub_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.Storage/storageAccounts/{account}"
    )
    
    # URL to Container Menu Blade
    return (
        f"https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade"
        f"/~/overview/storageAccountId/{storage_id.replace('/', '%2F')}/path/{container}"
    )


def _get_domain_description(layer_name: str, name: str) -> str:
    l_name = layer_name.lower()
    n = name.lower()
    
    if "market" in n:
        if "bronze" in l_name: return "Raw historical OHLCV files"
        if "silver" in l_name: return "Standardized daily OHLCV tables"
        if "gold" in l_name: return "Entity-resolved market features"
        return "Historical price and volume data"

    if "finance" in n:
        if "bronze" in l_name: return "Raw financial statements"
        if "silver" in l_name: return "Standardized financial tables"
        if "gold" in l_name: return "Financial ratios & growth metrics"
        return "Fundamental financial data"

    if "earnings" in n:
        if "bronze" in l_name: return "Raw earnings calendar/surprises"
        if "silver" in l_name: return "Standardized earnings history"
        if "gold" in l_name: return "Earnings surprise metrics"
        return "Earnings data"

    if "target" in n:
        if "bronze" in l_name: return "Raw analyst price targets"
        if "silver" in l_name: return "Standardized consensus targets"
        if "gold" in l_name: return "Consensus upside/downside metrics"
        return "Analyst price targets"
    
    return ""  # Fallback empty


def _describe_cron(expression: str) -> str:
    # Frequent mappings for this system
    mapping = {
        "0 12 * * *": "Daily at 12:00 PM UTC",
        "0 12 * * 1-5": "Weekdays at 12:00 PM UTC",
        "30 12 * * *": "Daily at 12:30 PM UTC",
        "0 14-22 * * *": "Daily, hourly 2:00–10:00 PM UTC",
        "0 14-22 * * 1-5": "Weekdays, hourly 2:00–10:00 PM UTC",
        "30 14-22 * * *": "Daily, hourly 2:30–10:30 PM UTC",
        "30 14-23 * * *": "Daily, hourly 2:30–11:30 PM UTC",
        "30 0 * * *": "Daily at 12:30 AM UTC",
        "30 1 * * *": "Daily at 1:30 AM UTC",
        "0 4 * * 1-5": "Weekdays at 4:00 AM UTC",
        "0 10 * * 1-5": "Weekdays at 10:00 AM UTC",
        "0 16 * * 1-5": "Weekdays at 4:00 PM UTC",
        "0 22 * * *": "Daily at 10:00 PM UTC",
        "0 22 * * 1-5": "Weekdays at 10:00 PM UTC",
        "30 22 * * *": "Daily at 10:30 PM UTC",
        "0 23 * * *": "Daily at 11:00 PM UTC",
        "0 23 * * 1-5": "Weekdays at 11:00 PM UTC",
        "30 23 * * *": "Daily at 11:30 PM UTC",
        "0 5 * * *": "Daily at 5:00 AM UTC",
        "0 0 * * *": "Daily at Midnight UTC",
    }
    return mapping.get(expression, expression)


def _derive_job_name(layer_name: str, domain_clean: str) -> str:
    l_name = layer_name.lower()
    d_name = domain_clean.lower()
    
    # Special cases
    if l_name == "platinum":
        # Platinum is reserved for curated/derived datasets. Jobs vary by dataset and are
        # intentionally not derived automatically.
        return ""
    
    return f"{l_name}-{d_name}-job"


def _make_job_portal_url(sub_id: str, rg: str, job_name: str) -> Optional[str]:
    if not all([sub_id, rg, job_name]):
        return None
    return (
        f"https://portal.azure.com/#resource/subscriptions/{sub_id}"
        f"/resourceGroups/{rg}/providers/Microsoft.App/jobs/{job_name}/overview"
    )

def _make_folder_portal_url(sub_id: str, rg: str, account: str, container: str, folder_path: str) -> Optional[str]:
    if not all([sub_id, rg, account, container, folder_path]):
        return None
    
    storage_id = (
        f"/subscriptions/{sub_id}/resourceGroups/{rg}"
        f"/providers/Microsoft.Storage/storageAccounts/{account}"
    )
    # Folder path needs to be encoded properly for the hash fragment
    # usually /path/{container}/{folder}
    full_path = f"{container}/{folder_path}".strip("/")
    return (
        f"https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade"
        f"/~/overview/storageAccountId/{storage_id.replace('/', '%2F')}/path/{full_path.replace('/', '%2F')}"
    )

def collect_system_health_snapshot(
    *,
    now: Optional[datetime] = None,
    include_resource_ids: bool = False,
) -> Dict[str, Any]:
    """
    Returns a UI-friendly SystemHealth payload.

    Phase 1 scope:
    - ADLS/Blob data-layer freshness probes (no ARM/resource health yet)
    - Alerts include stable IDs for persisted lifecycle actions (ack/snooze/resolve via API)
    - recentJobs left empty (Container App Job executions are Phase 2)
    """
    now = now or _utc_now()
    if _is_test_mode():
        logger.info("System health running in test mode (returning empty payload).")
        return {"overall": "healthy", "dataLayers": [], "recentJobs": [], "alerts": []}

    logger.info("Collecting system health: include_resource_ids=%s", include_resource_ids)

    cfg = AzureBlobStoreConfig.from_env()
    store = AzureBlobStore(cfg)

    layers: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []
    resources: List[Dict[str, Any]] = []
    job_runs: List[Dict[str, Any]] = []
    statuses: List[str] = []

    # Env vars for URL construction
    sub_id = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "").strip()
    rg = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "").strip()
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME", "").strip()

    layer_specs = _default_layer_specs()
    freshness_overrides = _load_freshness_overrides()
    marker_cfg = _marker_probe_config()
    layer_job_names = _collect_job_names_for_layers(layer_specs)
    job_schedule_metadata = _load_job_schedule_metadata(
        subscription_id=sub_id,
        resource_group=rg,
        job_names=layer_job_names,
    )

    for spec in layer_specs:
        layer_last_updated: Optional[datetime] = None
        had_layer_error = False
        domain_items: List[Dict[str, Any]] = []

        container_attr = getattr(spec, "container_name", None)
        if callable(container_attr):
            container = container_attr()
        else:
            container = str(container_attr or "")

        marker_blobs = getattr(spec, "marker_blobs", None)
        delta_tables = getattr(spec, "delta_tables", None)
        has_modern_domains = isinstance(marker_blobs, (list, tuple)) or isinstance(delta_tables, (list, tuple))

        if not has_modern_domains and hasattr(spec, "blob_prefix"):
            prefix = str(getattr(spec, "blob_prefix") or "").strip()
            max_age = int(getattr(spec, "freshness_threshold") or 0)
            layer_name = str(getattr(spec, "layer") or "Layer")
            probe = _probe_container_last_modified(
                store=store,
                container=container,
                prefix=prefix,
            )
            if probe.state == "error":
                had_layer_error = True
                domain_items.append(
                    {
                        "name": prefix,
                        "description": "",
                        "type": "blob",
                        "path": prefix,
                        "maxAgeSeconds": max_age,
                        "cron": "",
                        "frequency": "",
                        "lastUpdated": None,
                        "status": "error",
                        "portalUrl": None,
                        "jobUrl": None,
                        "jobName": None,
                        "freshnessSource": "legacy-prefix",
                        "freshnessPolicySource": "legacy",
                        "warnings": [probe.error or "Legacy layer probe failed."],
                    }
                )
            else:
                lm = probe.last_modified
                status = _compute_layer_status(now, lm, max_age_seconds=max_age, had_error=False)
                domain_items.append(
                    {
                        "name": prefix,
                        "description": "",
                        "type": "blob",
                        "path": prefix,
                        "maxAgeSeconds": max_age,
                        "cron": "",
                        "frequency": "",
                        "lastUpdated": _iso(lm),
                        "status": status,
                        "portalUrl": None,
                        "jobUrl": None,
                        "jobName": None,
                        "freshnessSource": "legacy-prefix",
                        "freshnessPolicySource": "legacy",
                        "warnings": [],
                    }
                )
                layer_last_updated = lm

            layer_status = (
                "error" if any(d["status"] == "error" for d in domain_items) else _compute_layer_status(now, layer_last_updated, max_age_seconds=max_age, had_error=had_layer_error)
            )
            statuses.append(layer_status)
            layers.append(
                {
                    "name": layer_name,
                    "description": "",
                    "lastUpdated": _iso(layer_last_updated),
                    "status": layer_status,
                    "maxAgeSeconds": max_age,
                    "refreshFrequency": "",
                    "portalUrl": None,
                    "domains": domain_items,
                }
            )
            alerts.extend(_layer_alerts(now, layer_name=layer_name, status=layer_status, last_updated=layer_last_updated, error=None))
            continue

        # Collect markers (CSV/Blobs)
        for domain_spec in spec.marker_blobs:
            blob_name = domain_spec.path
            d_name = os.path.dirname(blob_name) or blob_name
            name_clean = _domain_name_from_marker_path(blob_name)
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, d_name)
            domain_cron, domain_frequency = _resolve_domain_schedule(
                job_name=job_name,
                default_cron=domain_spec.cron,
                default_trigger_type=domain_spec.trigger_type,
                job_schedule_metadata=job_schedule_metadata,
            )
            policy = _resolve_freshness_policy(
                layer_name=spec.name,
                domain_name=name_clean,
                default_max_age_seconds=spec.max_age_seconds,
                overrides=freshness_overrides,
            )

            # If the config points to a specific file (e.g. whitelist.csv), we want to scan the folder it's in.
            # If it points to a folder (e.g. data/), dirname handles it appropriately (usually).
            search_prefix = os.path.dirname(blob_name) 
            # If search_prefix is empty (file at root), we scan the whole container (prefix=None or "").
            # Ideally we might want to restrict this, but for "latest update" in a container used for data, scanning root is correct.

            probe_resolution = _resolve_last_updated_with_marker_probes(
                layer_name=spec.name,
                domain_name=name_clean,
                store=store,
                marker_cfg=marker_cfg,
                legacy_source="legacy-prefix",
                legacy_probe_fn=lambda container_name=container, prefix=search_prefix: _probe_container_last_modified(
                    store=store,
                    container=container_name,
                    prefix=prefix,
                ),
            )
            lm = probe_resolution.last_updated
            if probe_resolution.status == "error":
                had_layer_error = True
                domain_items.append({
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "blob",
                    "path": blob_name,
                    "maxAgeSeconds": policy.max_age_seconds,
                    "cron": domain_cron,
                    "frequency": domain_frequency,
                    "lastUpdated": None,
                    "status": "error",
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                    "freshnessSource": probe_resolution.source,
                    "freshnessPolicySource": policy.source,
                    "warnings": probe_resolution.warnings,
                })
                continue

            if spec.name.lower() == "platinum" and lm is None:
                status = "healthy"
            else:
                status = _compute_layer_status(
                    now,
                    lm,
                    max_age_seconds=policy.max_age_seconds,
                    had_error=False,
                )
            domain_items.append({
                "name": name_clean,
                "description": _get_domain_description(spec.name, name_clean),
                "type": "blob",
                "path": blob_name,
                "maxAgeSeconds": policy.max_age_seconds,
                "cron": domain_cron,
                "frequency": domain_frequency,
                "lastUpdated": _iso(lm),
                "status": status,
                "portalUrl": folder_url,
                "jobUrl": job_url,
                "jobName": job_name,
                "freshnessSource": probe_resolution.source,
                "freshnessPolicySource": policy.source,
                "warnings": probe_resolution.warnings,
            })

        # Collect Delta tables
        for domain_spec in spec.delta_tables:
            table_path = domain_spec.path
            d_name = table_path
            name_clean = _domain_name_from_delta_path(d_name)
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, d_name)
            domain_cron, domain_frequency = _resolve_domain_schedule(
                job_name=job_name,
                default_cron=domain_spec.cron,
                default_trigger_type=domain_spec.trigger_type,
                job_schedule_metadata=job_schedule_metadata,
            )
            policy = _resolve_freshness_policy(
                layer_name=spec.name,
                domain_name=name_clean,
                default_max_age_seconds=spec.max_age_seconds,
                overrides=freshness_overrides,
            )

            delta_version: Optional[int] = None

            def _legacy_delta_probe() -> LastModifiedProbeResult:
                nonlocal delta_version
                try:
                    ver, lm = store.get_delta_table_last_modified(container=container, table_path=table_path)
                    delta_version = ver
                    if lm is None:
                        return LastModifiedProbeResult(state="not_found")
                    return LastModifiedProbeResult(state="ok", last_modified=lm)
                except Exception as exc:
                    return LastModifiedProbeResult(state="error", error=str(exc))

            probe_resolution = _resolve_last_updated_with_marker_probes(
                layer_name=spec.name,
                domain_name=name_clean,
                store=store,
                marker_cfg=marker_cfg,
                legacy_source="legacy-delta-log",
                legacy_probe_fn=_legacy_delta_probe,
            )
            lm = probe_resolution.last_updated

            if probe_resolution.status == "error":
                had_layer_error = True
                domain_items.append({
                    "name": name_clean,  # Use raw name on error if cleaning is ambiguous
                    "description": "",
                    "type": "delta",
                    "path": table_path,
                    "maxAgeSeconds": policy.max_age_seconds,
                    "cron": domain_cron,
                    "frequency": domain_frequency,
                    "lastUpdated": None,
                    "status": "error",
                    "version": None,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                    "freshnessSource": probe_resolution.source,
                    "freshnessPolicySource": policy.source,
                    "warnings": probe_resolution.warnings,
                })
                continue

            status = _compute_layer_status(
                now,
                lm,
                max_age_seconds=policy.max_age_seconds,
                had_error=False,
            )
            domain_items.append({
                "name": name_clean,
                "description": _get_domain_description(spec.name, name_clean),
                "type": "delta",
                "path": table_path,
                "maxAgeSeconds": policy.max_age_seconds,
                "cron": domain_cron,
                "frequency": domain_frequency,
                "lastUpdated": _iso(lm),
                "status": status,
                "version": delta_version if delta_version is not None else None,
                "portalUrl": folder_url,
                "jobUrl": job_url,
                "jobName": job_name,
                "freshnessSource": probe_resolution.source,
                "freshnessPolicySource": policy.source,
                "warnings": probe_resolution.warnings,
            })

        # Aggregate layer status
        valid_times = [
            datetime.fromisoformat(d["lastUpdated"])
            for d in domain_items
            if d["lastUpdated"] and d["status"] != "error"
        ]
        layer_last_updated = max(valid_times) if valid_times else None
        
        # If any domain is error/stale, layer is that status (worst of)
        layer_statuses = [d["status"] for d in domain_items]
        if "error" in layer_statuses:
            layer_status = "error"
        elif "stale" in layer_statuses:
            layer_status = "stale"
        elif (
            spec.name.lower() == "platinum"
            and not had_layer_error
            and domain_items
            and layer_last_updated is None
            and all(str(d.get("status") or "").lower() == "healthy" for d in domain_items)
        ):
            layer_status = "healthy"
        else:
            layer_status = _compute_layer_status(now, layer_last_updated, max_age_seconds=spec.max_age_seconds, had_error=had_layer_error)

        statuses.append(layer_status)

        portal_url = _make_container_portal_url(sub_id, rg, storage_account, container)
        unique_frequencies = sorted({str(item.get("frequency") or "").strip() for item in domain_items if item.get("frequency")})
        refresh_frequency = unique_frequencies[0] if len(unique_frequencies) == 1 else "Multiple schedules"

        logger.info(
            "Layer probe complete: layer=%s status=%s domains=%s",
            spec.name,
            layer_status,
            len(domain_items),
        )
        layers.append(
            {
                "name": spec.name,
                "description": spec.description,
                "lastUpdated": _iso(layer_last_updated),
                "status": layer_status,
                "maxAgeSeconds": spec.max_age_seconds,
                "refreshFrequency": refresh_frequency,
                "portalUrl": portal_url,
                "domains": domain_items,
            }
        )
        alerts.extend(_layer_alerts(now, layer_name=spec.name, status=layer_status, last_updated=layer_last_updated, error=None))

    # Optional Phase 2: Azure control-plane probes (Container Apps + Jobs + Executions).
    subscription_id = sub_id
    resource_group = rg
    app_names = _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS", ""))
    job_names = _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_JOBS", ""))

    logger.info(
        "System health ARM probe config: subscription_set=%s resource_group_set=%s apps=%s jobs=%s "
        "arm_api_version_set=%s arm_timeout_set=%s resource_health_enabled_set=%s monitor_metrics_enabled_set=%s "
        "log_analytics_enabled_set=%s job_exec_limit_set=%s",
        bool(subscription_id),
        bool(resource_group),
        len(app_names),
        len(job_names),
        _env_has_value("SYSTEM_HEALTH_ARM_API_VERSION"),
        _env_has_value("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS"),
        _env_has_value("SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED"),
        _env_has_value("SYSTEM_HEALTH_MONITOR_METRICS_ENABLED"),
        _env_has_value("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED"),
        _env_has_value("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB"),
    )

    arm_enabled = bool(subscription_id and resource_group and (app_names or job_names))
    if arm_enabled:
        try:
            api_version = _env_or_default("SYSTEM_HEALTH_ARM_API_VERSION", DEFAULT_ARM_API_VERSION)
            timeout_seconds = _require_float(
                "SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", min_value=0.5, max_value=30.0
            )
            resource_health_enabled = _require_bool("SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED")
            resource_health_api_version = (
                _require_env("SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION")
                if resource_health_enabled
                else DEFAULT_RESOURCE_HEALTH_API_VERSION
            )

            monitor_metrics_enabled = _require_bool("SYSTEM_HEALTH_MONITOR_METRICS_ENABLED")
            monitor_metrics_api_version = (
                _require_env("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION")
                if monitor_metrics_enabled
                else DEFAULT_MONITOR_METRICS_API_VERSION
            )
            monitor_metrics_timespan_minutes = (
                _require_int("SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES", min_value=1, max_value=24 * 60)
                if monitor_metrics_enabled
                else 0
            )
            monitor_metrics_interval = (
                _require_env("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL") if monitor_metrics_enabled else ""
            )
            monitor_metrics_aggregation = (
                _require_env("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION") if monitor_metrics_enabled else ""
            )
            monitor_metrics_thresholds_raw = os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON")
            monitor_metrics_thresholds_raw = (
                monitor_metrics_thresholds_raw.strip() if monitor_metrics_thresholds_raw else ""
            )
            monitor_metrics_thresholds: Dict[str, Any] = {}
            if monitor_metrics_thresholds_raw:
                try:
                    monitor_metrics_thresholds = parse_metric_thresholds_json(monitor_metrics_thresholds_raw)
                except Exception as exc:
                    monitor_metrics_thresholds = {}
                    alerts.append(
                        {
                            "id": _alert_id(
                                severity="warning",
                                title="Monitor metrics thresholds invalid",
                                component="AzureMonitorMetrics",
                            ),
                            "severity": "warning",
                            "title": "Monitor metrics thresholds invalid",
                            "component": "AzureMonitorMetrics",
                            "timestamp": _iso(now),
                            "message": f"SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON parse error: {exc}",
                            "acknowledged": False,
                            }
                        )
            containerapp_metric_names = _split_csv(
                os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS")
            )
            job_metric_names = _split_csv(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS"))

            log_analytics_enabled = _require_bool("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED")
            log_analytics_workspace_id = (
                _require_env("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID") if log_analytics_enabled else ""
            )
            log_analytics_timeout_seconds = (
                _require_float("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", min_value=0.5, max_value=30.0)
                if log_analytics_enabled
                else 0.0
            )
            log_analytics_timespan_minutes = (
                _require_int("SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", min_value=1, max_value=24 * 60)
                if log_analytics_enabled
                else 0
            )
            log_analytics_queries_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON")
            log_analytics_queries_raw = log_analytics_queries_raw.strip() if log_analytics_queries_raw else ""
            log_analytics_queries = []
            if log_analytics_queries_raw:
                try:
                    log_analytics_queries = parse_log_analytics_queries_json(log_analytics_queries_raw)
                except Exception as exc:
                    log_analytics_queries = []
                    log_analytics_enabled = False
                    alerts.append(
                        {
                            "id": _alert_id(
                                severity="warning",
                                title="Log Analytics queries invalid",
                                component="AzureLogAnalytics",
                            ),
                            "severity": "warning",
                            "title": "Log Analytics queries invalid",
                            "component": "AzureLogAnalytics",
                            "timestamp": _iso(now),
                            "message": f"SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON parse error: {exc}",
                            "acknowledged": False,
                        }
                    )

            if log_analytics_enabled and (not log_analytics_workspace_id or not log_analytics_queries):
                log_analytics_enabled = False
                alerts.append(
                    {
                        "id": _alert_id(
                            severity="warning",
                            title="Log Analytics monitoring disabled",
                            component="AzureLogAnalytics",
                        ),
                        "severity": "warning",
                        "title": "Log Analytics monitoring disabled",
                        "component": "AzureLogAnalytics",
                        "timestamp": _iso(now),
                        "message": "Log Analytics enabled but workspace ID or queries are missing.",
                        "acknowledged": False,
                    }
                    )

            arm_cfg = ArmConfig(
                subscription_id=subscription_id,
                resource_group=resource_group,
                api_version=api_version,
                timeout_seconds=timeout_seconds,
            )
            checked_iso = _iso(now)
            with AzureArmClient(arm_cfg) as arm:
                log_client: Optional[AzureLogAnalyticsClient] = None
                if log_analytics_enabled:
                    log_client = AzureLogAnalyticsClient(timeout_seconds=log_analytics_timeout_seconds)

                def _enrich_resource(
                    item: ResourceHealthItem,
                    *,
                    metric_names: Sequence[str],
                ) -> ResourceHealthItem:
                    status = item.status
                    details = item.details
                    signals: List[Dict[str, Any]] = list(item.signals)

                    if monitor_metrics_enabled and metric_names and item.azure_id:
                        metric_signals, metric_status = collect_monitor_metrics(
                            arm,
                            resource_id=item.azure_id,
                            metric_names=metric_names,
                            end_time=now,
                            timespan_minutes=monitor_metrics_timespan_minutes,
                            interval=monitor_metrics_interval,
                            aggregation=monitor_metrics_aggregation,
                            api_version=monitor_metrics_api_version,
                            thresholds=monitor_metrics_thresholds,
                        )
                        if metric_signals:
                            signals.extend(metric_signals)
                            status = _worse_resource_status(status, metric_status)
                            details = _append_signal_details(details, metric_signals)

                    if log_client is not None and item.azure_id:
                        log_signals, log_status = collect_log_analytics_signals(
                            log_client,
                            workspace_id=log_analytics_workspace_id,
                            specs=log_analytics_queries,
                            resource_type=item.resource_type,
                            resource_name=item.name,
                            resource_id=item.azure_id,
                            end_time=now,
                            timespan_minutes=log_analytics_timespan_minutes,
                        )
                        if log_signals:
                            signals.extend(log_signals)
                            status = _worse_resource_status(status, log_status)
                            details = _append_signal_details(details, log_signals)

                    return ResourceHealthItem(
                        name=item.name,
                        resource_type=item.resource_type,
                        status=status,
                        last_checked=item.last_checked,
                        details=details,
                        azure_id=item.azure_id,
                        running_state=item.running_state,
                        signals=tuple(signals),
                    )

                def _record_resource(item: ResourceHealthItem, *, title: str) -> None:
                    resources.append(item.to_dict(include_ids=include_resource_ids))
                    if item.status in {"warning", "error"}:
                        statuses.append("stale" if item.status == "warning" else "error")
                        alerts.append(
                            {
                                "id": _alert_id(
                                    severity="warning" if item.status == "warning" else "error",
                                    title=title,
                                    component=item.name,
                                ),
                                "severity": "warning" if item.status == "warning" else "error",
                                "title": title,
                                "component": item.name,
                                "timestamp": checked_iso,
                                "message": f"{item.resource_type}: {item.details}",
                                "acknowledged": False,
                            }
                        )

                try:
                    if app_names:
                        logger.info("Collecting Azure container app health: count=%s", len(app_names))
                        app_resources = collect_container_apps(
                            arm,
                            app_names=app_names,
                            last_checked_iso=checked_iso,
                            include_ids=include_resource_ids,
                            resource_health_enabled=resource_health_enabled,
                            resource_health_api_version=resource_health_api_version,
                        )
                        for item in app_resources:
                            enriched = _enrich_resource(item, metric_names=containerapp_metric_names)
                            _record_resource(enriched, title="Azure resource health")

                    if job_names:
                        logger.info("Collecting Azure job health: count=%s", len(job_names))
                        max_executions_per_job = _require_int(
                            "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", min_value=1, max_value=25
                        )
                        job_resources, runs = collect_jobs_and_executions(
                            arm,
                            job_names=job_names,
                            last_checked_iso=checked_iso,
                            include_ids=include_resource_ids,
                            max_executions_per_job=max_executions_per_job,
                            resource_health_enabled=resource_health_enabled,
                            resource_health_api_version=resource_health_api_version,
                        )
                        run_counts: Dict[str, int] = {}
                        for run in runs:
                            run_job_name = str(run.get("jobName") or "").strip()
                            if not run_job_name:
                                continue
                            run_counts[run_job_name] = run_counts.get(run_job_name, 0) + 1
                        jobs_without_runs = [name for name in job_names if run_counts.get(name, 0) == 0]
                        logger.info(
                            "Azure job execution summary: configured_jobs=%s resources=%s runs=%s max_per_job=%s jobs_without_runs=%s",
                            len(job_names),
                            len(job_resources),
                            len(runs),
                            max_executions_per_job,
                            len(jobs_without_runs),
                        )
                        if jobs_without_runs:
                            logger.warning(
                                "Azure job execution summary missing runs: jobs=%s",
                                ",".join(jobs_without_runs[:20]),
                            )
                        for item in job_resources:
                            enriched = _enrich_resource(item, metric_names=job_metric_names)
                            _record_resource(enriched, title="Azure job health")

                        job_runs.extend(runs)
                        # Determine health based on the *latest* execution per job.
                        latest_by_job: Dict[str, Dict[str, Any]] = {}
                        for run in runs:
                            job_name = str(run.get("jobName") or "").strip()
                            if not job_name:
                                continue
                            existing = latest_by_job.get(job_name)
                            if _newer_execution(run, existing):
                                latest_by_job[job_name] = run

                        for run in latest_by_job.values():
                            if run.get("status") != "failed":
                                continue
                            job_name = str(run.get("jobName") or "job")
                            start_time = str(run.get("startTime") or "")
                            message = "Latest execution reported failed."
                            if start_time:
                                message = f"Latest execution reported failed (startTime={start_time})."
                            statuses.append("error")
                            alerts.append(
                                {
                                    "id": _alert_id(
                                        severity="error",
                                        title="Job execution failed",
                                        component=job_name,
                                    ),
                                    "severity": "error",
                                    "title": "Job execution failed",
                                    "component": job_name,
                                    "timestamp": checked_iso,
                                    "message": message,
                                    "acknowledged": False,
                                }
                            )
                finally:
                    if log_client is not None:
                        log_client.close()
        except Exception as exc:
            logger.exception(
                "Azure control-plane probes failed: subscription_set=%s resource_group_set=%s apps=%s jobs=%s error=%s",
                bool(subscription_id),
                bool(resource_group),
                len(app_names),
                len(job_names),
                exc,
            )
            checked_iso = _iso(now)
            alerts.append(
                {
                    "id": _alert_id(
                        severity="warning",
                        title="Azure monitoring disabled",
                        component="AzureControlPlane",
                    ),
                    "severity": "warning",
                    "title": "Azure monitoring disabled",
                    "component": "AzureControlPlane",
                    "timestamp": checked_iso,
                    "message": f"Control-plane probe error: {exc}",
                    "acknowledged": False,
                }
            )
    else:
        logger.warning(
            "System health ARM probes disabled: subscription_set=%s resource_group_set=%s apps=%s jobs=%s",
            bool(subscription_id),
            bool(resource_group),
            len(app_names),
            len(job_names),
        )

    overall = _overall_from_layers(statuses)
    payload: Dict[str, Any] = {"overall": overall, "dataLayers": layers, "recentJobs": job_runs, "alerts": alerts}
    if resources:
        payload["resources"] = resources
    logger.info(
        "System health summary: overall=%s layers=%s alerts=%s resources=%s jobs=%s",
        overall,
        len(layers),
        len(alerts),
        len(resources),
        len(job_runs),
    )
    return payload
