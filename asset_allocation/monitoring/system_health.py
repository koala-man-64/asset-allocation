from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from asset_allocation.monitoring.azure_blob_store import AzureBlobStore, AzureBlobStoreConfig


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


def collect_system_health_snapshot(*, now: Optional[datetime] = None) -> Dict[str, Any]:
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

    overall = _overall_from_layers(statuses)
    return {"overall": overall, "dataLayers": layers, "recentJobs": [], "alerts": alerts}
