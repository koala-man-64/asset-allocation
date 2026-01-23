from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from monitoring.azure_blob_store import AzureBlobStore, AzureBlobStoreConfig
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> str:
    if not dt:
        return "1970-01-01T00:00:00+00:00"
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc).isoformat()
    return dt.astimezone(timezone.utc).isoformat()


def _is_truthy(raw: str) -> bool:
    value = (raw or "").strip().lower()
    return value in {"1", "true", "t", "yes", "y", "on"}


def _is_test_mode() -> bool:
    if _is_truthy(os.environ.get("SYSTEM_HEALTH_RUN_IN_TEST", "")):
        return False
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True
    return _is_truthy(os.environ.get("TEST_MODE", ""))


def _get_int(name: str, default: int, *, min_value: int = 1, max_value: int = 365 * 24 * 3600) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _env_or(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _split_csv(raw: str) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _get_float(name: str, default: float, *, min_value: float = 0.1, max_value: float = 120.0) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {name}={raw!r}") from exc
    if value < min_value or value > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {value}).")
    return value


def _worse_resource_status(primary: str, secondary: str) -> str:
    ranking = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3}
    return secondary if ranking.get(secondary, 0) > ranking.get(primary, 0) else primary


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


@dataclass(frozen=True)
class LayerProbeSpec:
    name: str
    description: str
    refresh_frequency: str
    container_env: str
    container_default: str
    max_age_seconds: int
    marker_blobs: Sequence[str] = ()
    delta_tables: Sequence[str] = ()

    def container_name(self) -> str:
        return _env_or(self.container_env, self.container_default)


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
            "severity": "warning",
            "title": "Layer stale",
            "component": layer_name,
            "timestamp": timestamp,
            "message": f"{layer_name} appears stale (lastUpdated={last_text}).",
            "acknowledged": False,
        }
    ]


def _default_layer_specs() -> List[LayerProbeSpec]:
    max_age_default = _get_int("SYSTEM_HEALTH_MAX_AGE_SECONDS", 36 * 3600)
    max_age_ranking = _get_int("SYSTEM_HEALTH_RANKING_MAX_AGE_SECONDS", 72 * 3600)

    return [
        LayerProbeSpec(
            name="Bronze",
            description="Raw ingestion (blob snapshots + whitelists)",
            refresh_frequency="Daily",
            container_env="AZURE_CONTAINER_BRONZE",
            container_default="bronze",
            max_age_seconds=max_age_default,
            marker_blobs=(
                "market-data/whitelist.csv",
                "finance-data/whitelist.csv",
                "earnings-data/whitelist.csv",
                "price-target-data/whitelist.csv",
            ),
        ),
        LayerProbeSpec(
            name="Silver",
            description="Normalized Delta tables (by-date aggregates)",
            refresh_frequency="Daily",
            container_env="AZURE_CONTAINER_SILVER",
            container_default="silver",
            max_age_seconds=max_age_default,
            delta_tables=(
                "market-data-by-date",
                "finance-data-by-date",
                "earnings-data-by-date",
                "price-target-data-by-date",
            ),
        ),
        LayerProbeSpec(
            name="Gold",
            description="Feature store Delta tables (by-date aggregates)",
            refresh_frequency="Daily",
            container_env="AZURE_CONTAINER_GOLD",
            container_default="gold",
            max_age_seconds=max_age_default,
            delta_tables=(
                "market_by_date",
                "finance_by_date",
                "earnings_by_date",
                "targets_by_date",
            ),
        ),
        LayerProbeSpec(
            name="Ranking",
            description="Platinum rankings + derived signals",
            refresh_frequency="Daily",
            container_env="AZURE_CONTAINER_RANKING",
            container_default="ranking-data",
            max_age_seconds=max_age_ranking,
            delta_tables=(
                "platinum/rankings",
                "platinum/signals/daily",
            ),
        ),
    ]


def collect_system_health_snapshot(
    *,
    now: Optional[datetime] = None,
    include_resource_ids: bool = False,
) -> Dict[str, Any]:
    """
    Returns a UI-friendly SystemHealth payload.

    Phase 1 scope:
    - ADLS/Blob data-layer freshness probes (no ARM/resource health yet)
    - No persisted alert acknowledgements (all alerts unacknowledged)
    - recentJobs left empty (Container App Job executions are Phase 2)
    """
    now = now or _utc_now()
    if _is_test_mode():
        return {"overall": "healthy", "dataLayers": [], "recentJobs": [], "alerts": []}

    cfg = AzureBlobStoreConfig.from_env()
    store = AzureBlobStore(cfg)

    layers: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []
    resources: List[Dict[str, Any]] = []
    job_runs: List[Dict[str, Any]] = []
    statuses: List[str] = []

    for spec in _default_layer_specs():
        last_updated: Optional[datetime] = None
        had_error = False
        err_text: Optional[str] = None
        container = spec.container_name()

        try:
            candidate_times: List[datetime] = []
            for blob_name in spec.marker_blobs:
                lm = store.get_blob_last_modified(container=container, blob_name=blob_name)
                if lm is not None:
                    candidate_times.append(lm)
            for table_path in spec.delta_tables:
                lm = store.get_delta_table_last_modified(container=container, table_path=table_path)
                if lm is not None:
                    candidate_times.append(lm)
            last_updated = max(candidate_times) if candidate_times else None
        except Exception as exc:
            had_error = True
            err_text = str(exc)
            last_updated = None

        status = _compute_layer_status(now, last_updated, max_age_seconds=spec.max_age_seconds, had_error=had_error)
        statuses.append(status)

        layers.append(
            {
                "name": spec.name,
                "description": spec.description,
                "lastUpdated": _iso(last_updated),
                "status": status,
                "refreshFrequency": spec.refresh_frequency,
            }
        )
        alerts.extend(_layer_alerts(now, layer_name=spec.name, status=status, last_updated=last_updated, error=err_text))

    # Optional Phase 2: Azure control-plane probes (Container Apps + Jobs + Executions).
    subscription_id = os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "").strip()
    resource_group = os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "").strip()
    app_names = _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_CONTAINERAPPS", ""))
    job_names = _split_csv(os.environ.get("SYSTEM_HEALTH_ARM_JOBS", ""))

    arm_enabled = bool(subscription_id and resource_group and (app_names or job_names))
    if arm_enabled:
        try:
            api_version = os.environ.get("SYSTEM_HEALTH_ARM_API_VERSION", "").strip() or "2023-05-01"
            timeout_seconds = _get_float("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", 5.0, min_value=0.5, max_value=30.0)
            resource_health_enabled = _is_truthy(os.environ.get("SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED", ""))
            resource_health_api_version = (
                os.environ.get("SYSTEM_HEALTH_RESOURCE_HEALTH_API_VERSION", "").strip()
                or DEFAULT_RESOURCE_HEALTH_API_VERSION
            )

            monitor_metrics_enabled = _is_truthy(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_ENABLED", ""))
            monitor_metrics_api_version = (
                os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_API_VERSION", "").strip()
                or DEFAULT_MONITOR_METRICS_API_VERSION
            )
            monitor_metrics_timespan_minutes = _get_int(
                "SYSTEM_HEALTH_MONITOR_METRICS_TIMESPAN_MINUTES", 15, min_value=1, max_value=24 * 60
            )
            monitor_metrics_interval = os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_INTERVAL", "").strip() or "PT1M"
            monitor_metrics_aggregation = os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_AGGREGATION", "").strip() or "Average"
            monitor_metrics_thresholds_raw = os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON", "").strip()
            monitor_metrics_thresholds: Dict[str, Any] = {}
            if monitor_metrics_thresholds_raw:
                try:
                    monitor_metrics_thresholds = parse_metric_thresholds_json(monitor_metrics_thresholds_raw)
                except Exception as exc:
                    monitor_metrics_thresholds = {}
                    alerts.append(
                        {
                            "severity": "warning",
                            "title": "Monitor metrics thresholds invalid",
                            "component": "AzureMonitorMetrics",
                            "timestamp": _iso(now),
                            "message": f"SYSTEM_HEALTH_MONITOR_METRICS_THRESHOLDS_JSON parse error: {exc}",
                            "acknowledged": False,
                        }
                    )
            containerapp_metric_names = _split_csv(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_CONTAINERAPP_METRICS", ""))
            job_metric_names = _split_csv(os.environ.get("SYSTEM_HEALTH_MONITOR_METRICS_JOB_METRICS", ""))

            log_analytics_enabled = _is_truthy(os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED", ""))
            log_analytics_workspace_id = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "").strip()
            log_analytics_timeout_seconds = _get_float(
                "SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", 5.0, min_value=0.5, max_value=30.0
            )
            log_analytics_timespan_minutes = _get_int(
                "SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", 15, min_value=1, max_value=24 * 60
            )
            log_analytics_queries_raw = os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON", "").strip()
            log_analytics_queries = []
            if log_analytics_queries_raw:
                try:
                    log_analytics_queries = parse_log_analytics_queries_json(log_analytics_queries_raw)
                except Exception as exc:
                    log_analytics_queries = []
                    log_analytics_enabled = False
                    alerts.append(
                        {
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
                        signals=tuple(signals),
                    )

                def _record_resource(item: ResourceHealthItem, *, title: str) -> None:
                    resources.append(item.to_dict(include_ids=include_resource_ids))
                    if item.status in {"warning", "error"}:
                        statuses.append("stale" if item.status == "warning" else "error")
                        alerts.append(
                            {
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
                        job_resources, runs = collect_jobs_and_executions(
                            arm,
                            job_names=job_names,
                            last_checked_iso=checked_iso,
                            include_ids=include_resource_ids,
                            max_executions_per_job=_get_int(
                                "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", 3, min_value=1, max_value=25
                            ),
                            resource_health_enabled=resource_health_enabled,
                            resource_health_api_version=resource_health_api_version,
                        )
                        for item in job_resources:
                            enriched = _enrich_resource(item, metric_names=job_metric_names)
                            _record_resource(enriched, title="Azure job health")

                        job_runs.extend(runs)
                        for run in runs:
                            if run.get("status") == "failed":
                                statuses.append("error")
                                alerts.append(
                                    {
                                        "severity": "error",
                                        "title": "Job execution failed",
                                        "component": str(run.get("jobName") or "job"),
                                        "timestamp": checked_iso,
                                        "message": "Latest execution reported failed.",
                                        "acknowledged": False,
                                    }
                                )
                finally:
                    if log_client is not None:
                        log_client.close()
        except Exception as exc:
            checked_iso = _iso(now)
            alerts.append(
                {
                    "severity": "warning",
                    "title": "Azure monitoring disabled",
                    "component": "AzureControlPlane",
                    "timestamp": checked_iso,
                    "message": f"Control-plane probe error: {exc}",
                    "acknowledged": False,
                }
            )

    overall = _overall_from_layers(statuses)
    payload: Dict[str, Any] = {"overall": overall, "dataLayers": layers, "recentJobs": job_runs, "alerts": alerts}
    if resources:
        payload["resources"] = resources
    return payload
