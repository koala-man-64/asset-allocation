from __future__ import annotations

import logging
import os
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from core.data_contract import CANONICAL_COMPOSITE_SIGNALS_PATH, CANONICAL_RANKINGS_PATH
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

logger = logging.getLogger("backtest.system_health")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime]) -> str:
    if not dt:
        return "1970-01-01T00:00:00+00:00"
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
class DomainSpec:
    path: str
    cron: str = "0 0 * * *"  # Default Daily


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
    max_age_ranking = _require_int("SYSTEM_HEALTH_RANKING_MAX_AGE_SECONDS")

    # Deployed job schedules (see deploy/job_*.yaml)
    CRON_BRONZE_MARKET = "0 14-22 * * *"
    CRON_BRONZE_PRICE_TARGET = "0 12 * * *"
    CRON_BRONZE_FINANCE = "0 22 * * *"
    CRON_BRONZE_EARNINGS = "0 23 * * *"

    CRON_SILVER_MARKET = "30 14-23 * * *"
    CRON_SILVER_FINANCE = "30 0 * * *"
    CRON_SILVER_PRICE_TARGET = "30 1 * * *"
    CRON_SILVER_EARNINGS = "30 23 * * *"

    CRON_GOLD_MARKET = "30 14-22 * * *"
    CRON_GOLD_FINANCE = "30 22 * * *"
    CRON_GOLD_EARNINGS = "30 23 * * *"
    CRON_GOLD_PRICE_TARGET = "30 12 * * *"

    CRON_PLATINUM_RANKING = "0 5 * * *"

    return [
        LayerProbeSpec(
            name="Bronze",
            description="Landing zone for raw data. Immutable source of truth for replayability.",
            container_env="AZURE_CONTAINER_BRONZE",
            max_age_seconds=max_age_default,
            marker_blobs=(
                DomainSpec("market-data/whitelist.csv", CRON_BRONZE_MARKET),
                DomainSpec("finance-data/whitelist.csv", CRON_BRONZE_FINANCE),
                DomainSpec("earnings-data/whitelist.csv", CRON_BRONZE_EARNINGS),
                DomainSpec("price-target-data/whitelist.csv", CRON_BRONZE_PRICE_TARGET),
            ),
        ),
        LayerProbeSpec(
            name="Silver",
            description="Cleaned, standardized tabular data. Enforced schemas for reliable querying.",
            container_env="AZURE_CONTAINER_SILVER",
            max_age_seconds=max_age_default,
            delta_tables=(
                DomainSpec("market-data-by-date", CRON_SILVER_MARKET),
                DomainSpec("finance-data-by-date", CRON_SILVER_FINANCE),
                DomainSpec("earnings-data-by-date", CRON_SILVER_EARNINGS),
                DomainSpec("price-target-data-by-date", CRON_SILVER_PRICE_TARGET),
            ),
        ),
        LayerProbeSpec(
            name="Gold",
            description="Entity-resolved feature store. Financial metrics ready for modeling.",
            container_env="AZURE_CONTAINER_GOLD",
            max_age_seconds=max_age_default,
            delta_tables=(
                DomainSpec("market_by_date", CRON_GOLD_MARKET),
                DomainSpec("finance_by_date", CRON_GOLD_FINANCE),
                DomainSpec("earnings_by_date", CRON_GOLD_EARNINGS),
                DomainSpec("targets_by_date", CRON_GOLD_PRICE_TARGET),
            ),
        ),
        LayerProbeSpec(
            name="Platinum",
            description="Platinum rankings + derived signals",
            container_env="AZURE_CONTAINER_RANKING",
            max_age_seconds=max_age_ranking,
            delta_tables=(
                DomainSpec(CANONICAL_RANKINGS_PATH, CRON_PLATINUM_RANKING),
                DomainSpec(CANONICAL_COMPOSITE_SIGNALS_PATH, CRON_PLATINUM_RANKING),
            ),
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

    if "ranking" in n:
        return "Composite scores for universe selection"
    if "signal" in n or "daily" in n:
        return "Daily trade signals and portfolio adjustments"
    
    return ""  # Fallback empty


def _describe_cron(expression: str) -> str:
    # Frequent mappings for this system
    mapping = {
        "0 12 * * *": "Daily at 12:00 PM UTC",
        "30 12 * * *": "Daily at 12:30 PM UTC",
        "0 14-22 * * *": "Daily, hourly 2:00–10:00 PM UTC",
        "30 14-22 * * *": "Daily, hourly 2:30–10:30 PM UTC",
        "30 14-23 * * *": "Daily, hourly 2:30–11:30 PM UTC",
        "30 0 * * *": "Daily at 12:30 AM UTC",
        "30 1 * * *": "Daily at 1:30 AM UTC",
        "0 22 * * *": "Daily at 10:00 PM UTC",
        "30 22 * * *": "Daily at 10:30 PM UTC",
        "0 23 * * *": "Daily at 11:00 PM UTC",
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
        return "platinum-ranking-job"
    
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
    # Emit a terse config snapshot so "missing links / missing jobs" reports are debuggable from logs.
    # Avoid printing secrets (connection strings, tokens, etc).
    logger.info(
        "System health config: test_mode=%s storage_account=%s conn_string=%s arm_sub=%s arm_rg=%s",
        _is_test_mode(),
        bool(cfg.account_name),
        bool(cfg.connection_string),
        bool(os.environ.get("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "").strip()),
        bool(os.environ.get("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "").strip()),
    )
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
    if not (sub_id and rg and storage_account):
        missing = []
        if not sub_id:
            missing.append("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID")
        if not rg:
            missing.append("SYSTEM_HEALTH_ARM_RESOURCE_GROUP")
        if not storage_account:
            missing.append("AZURE_STORAGE_ACCOUNT_NAME")
        logger.warning(
            "Portal link URL generation disabled (missing %s). "
            "Storage container/folder and job portal links will be omitted.",
            ",".join(missing),
        )

    for spec in _default_layer_specs():
        layer_last_updated: Optional[datetime] = None
        had_layer_error = False
        container = spec.container_name()
        logger.info(
            "Layer probe config: layer=%s container_env=%s container=%s markers=%s delta_tables=%s",
            spec.name,
            spec.container_env,
            container,
            len(spec.marker_blobs),
            len(spec.delta_tables),
        )
        domain_items: List[Dict[str, Any]] = []

        # Collect markers (CSV/Blobs)
        for domain_spec in spec.marker_blobs:
            blob_name = domain_spec.path
            d_name = os.path.dirname(blob_name) or blob_name
            name_clean = d_name.replace("/whitelist.csv", "").replace("-data", "")
            
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, d_name)

            # If the config points to a specific file (e.g. whitelist.csv), we want to scan the folder it's in.
            # If it points to a folder (e.g. data/), dirname handles it appropriately (usually).
            search_prefix = os.path.dirname(blob_name) 
            # If search_prefix is empty (file at root), we scan the whole container (prefix=None or "").
            # Ideally we might want to restrict this, but for "latest update" in a container used for data, scanning root is correct.
            
            try:
                lm = store.get_container_last_modified(container=container, prefix=search_prefix)
                status = _compute_layer_status(now, lm, max_age_seconds=spec.max_age_seconds, had_error=False)
                domain_items.append({
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "blob",
                    "path": blob_name,
                    "maxAgeSeconds": spec.max_age_seconds,
                    "cron": domain_spec.cron,
                    "frequency": _describe_cron(domain_spec.cron),
                    "lastUpdated": _iso(lm),
                    "status": status,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                })
            except Exception:
                logger.warning(
                    "Layer marker probe failed: layer=%s domain=%s container=%s",
                    spec.name,
                    name_clean,
                    container,
                    exc_info=True,
                )
                had_layer_error = True
                domain_items.append({
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "blob",
                    "path": blob_name,
                    "maxAgeSeconds": spec.max_age_seconds,
                    "cron": domain_spec.cron,
                    "frequency": _describe_cron(domain_spec.cron),
                    "lastUpdated": None,
                    "status": "error",
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                })

        # Collect Delta tables
        for domain_spec in spec.delta_tables:
            table_path = domain_spec.path
            d_name = table_path
            name_clean = d_name.split("/")[-1].replace("_by_date", "").replace("-by-date", "").replace("-data", "")
            if "/signals/" in d_name:
                name_clean = "signals"
            if name_clean == "targets":
                name_clean = "price-target"
            
            job_name = _derive_job_name(spec.name, name_clean)
            job_url = _make_job_portal_url(sub_id, rg, job_name)
            folder_url = _make_folder_portal_url(sub_id, rg, storage_account, container, d_name)

            try:
                ver, lm = store.get_delta_table_last_modified(container=container, table_path=table_path)
                status = _compute_layer_status(now, lm, max_age_seconds=spec.max_age_seconds, had_error=False)
                domain_items.append({
                    "name": name_clean,
                    "description": _get_domain_description(spec.name, name_clean),
                    "type": "delta",
                    "path": table_path,
                    "maxAgeSeconds": spec.max_age_seconds,
                    "cron": domain_spec.cron,
                    "frequency": _describe_cron(domain_spec.cron),
                    "lastUpdated": _iso(lm),
                    "status": status,
                    "version": ver if ver is not None else None,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
                })
            except Exception:
                logger.warning(
                    "Layer delta probe failed: layer=%s domain=%s container=%s path=%s",
                    spec.name,
                    name_clean,
                    container,
                    table_path,
                    exc_info=True,
                )
                had_layer_error = True
                domain_items.append({
                    "name": name_clean,  # Use raw name on error if cleaning is ambiguous
                    "description": "",
                    "type": "delta",
                    "path": table_path,
                    "maxAgeSeconds": spec.max_age_seconds,
                    "cron": domain_spec.cron,
                    "frequency": _describe_cron(domain_spec.cron),
                    "lastUpdated": None,
                    "status": "error",
                    "version": None,
                    "portalUrl": folder_url,
                    "jobUrl": job_url,
                    "jobName": job_name,
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

    arm_enabled = bool(subscription_id and resource_group and (app_names or job_names))
    logger.info(
        "Azure ARM probes: enabled=%s sub=%s rg=%s containerapps=%s jobs=%s",
        arm_enabled,
        bool(subscription_id),
        bool(resource_group),
        len(app_names),
        len(job_names),
    )
    if not arm_enabled and (subscription_id or resource_group):
        logger.info(
            "Azure ARM probes disabled: set SYSTEM_HEALTH_ARM_CONTAINERAPPS and/or SYSTEM_HEALTH_ARM_JOBS to enable "
            "resource and recent job execution monitoring."
        )
    if arm_enabled:
        try:
            api_version = _require_env("SYSTEM_HEALTH_ARM_API_VERSION")
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
                        job_resources, runs = collect_jobs_and_executions(
                            arm,
                            job_names=job_names,
                            last_checked_iso=checked_iso,
                            include_ids=include_resource_ids,
                            max_executions_per_job=_require_int(
                                "SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", min_value=1, max_value=25
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
                                        "id": _alert_id(
                                            severity="error",
                                            title="Job execution failed",
                                            component=str(run.get("jobName") or "job"),
                                        ),
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
            logger.exception("Azure control-plane probes failed.")
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
