from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from fastapi.testclient import TestClient

from api.service.app import create_app
from monitoring.delta_log import find_latest_delta_version
from monitoring import system_health
from monitoring.ttl_cache import TtlCache


def test_find_latest_delta_version_finds_highest_contiguous() -> None:
    def exists(version: int) -> bool:
        return 0 <= version <= 9

    assert find_latest_delta_version(exists, start_version=0) == 9
    assert find_latest_delta_version(exists, start_version=6) == 9


def test_find_latest_delta_version_returns_none_when_missing() -> None:
    def exists(_version: int) -> bool:
        return False

    assert find_latest_delta_version(exists, start_version=0) is None


def test_ttl_cache_returns_stale_value_on_refresh_error() -> None:
    now = 0.0

    def time_fn() -> float:
        return now

    cache: TtlCache[str] = TtlCache(ttl_seconds=10.0, time_fn=time_fn)

    calls = {"count": 0}

    def refresh_ok() -> str:
        calls["count"] += 1
        return "value-1"

    first = cache.get(refresh_ok)
    assert first.value == "value-1"
    assert first.cache_hit is False
    assert first.refresh_error is None
    assert calls["count"] == 1

    now = 5.0
    second = cache.get(lambda: "value-2")
    assert second.value == "value-1"
    assert second.cache_hit is True
    assert second.refresh_error is None
    assert calls["count"] == 1

    now = 15.0

    def refresh_fail() -> str:
        raise RuntimeError("boom")

    third = cache.get(refresh_fail)
    assert third.value == "value-1"
    assert third.cache_hit is True
    assert third.refresh_error is not None


def test_make_job_portal_url_uses_resource_anchor() -> None:
    url = system_health._make_job_portal_url("sub", "rg", "myjob")
    assert url == (
        "https://portal.azure.com/#resource/subscriptions/sub"
        "/resourceGroups/rg/providers/Microsoft.App/jobs/myjob/overview"
    )


def test_system_health_public_when_no_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "none")
    monkeypatch.delenv("BACKTEST_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_AUDIENCE", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/system/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert set(payload.keys()) >= {"overall", "dataLayers", "recentJobs", "alerts"}


def test_system_health_requires_api_key_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_API_KEY", "secret")
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "api_key")
    monkeypatch.delenv("BACKTEST_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_AUDIENCE", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/system/health")
        assert resp.status_code == 401

        resp2 = client.get("/api/system/health", headers={"X-API-Key": "secret"})
        assert resp2.status_code == 200


def test_system_health_control_plane_redacts_resource_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    checked_iso = now.isoformat()

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"

    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Succeeded",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:00:05Z",
                    }
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "healthy"
    assert len(payload["resources"]) == 2
    assert all("azureId" not in item for item in payload["resources"])
    assert len(payload["recentJobs"]) == 1
    assert payload["recentJobs"][0]["status"] == "success"
    assert payload["recentJobs"][0]["triggeredBy"] == "azure"
    assert payload["alerts"] == []

    verbose = system_health.collect_system_health_snapshot(now=now, include_resource_ids=True)
    assert all("azureId" in item for item in verbose["resources"])


def test_system_health_degraded_on_warning_resource(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": ""},
        }
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "degraded"
    assert payload["resources"][0]["status"] == "warning"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "warning" for alert in payload["alerts"])


def test_system_health_critical_on_failed_job_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "myjob")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    job_url = "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob"
    responses: Dict[str, Dict[str, Any]] = {
        job_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/jobs/myjob",
            "properties": {"provisioningState": "Succeeded"},
        },
        f"{job_url}/executions": {
            "value": [
                {
                    "properties": {
                        "status": "Failed",
                        "startTime": "2024-01-01T00:00:00Z",
                        "endTime": "2024-01-01T00:00:05Z",
                    }
                }
            ]
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert any(alert["title"] == "Job execution failed" and alert["severity"] == "error" for alert in payload["alerts"])


def test_system_health_critical_on_resource_health_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_RUN_IN_TEST", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "myapp")
    monkeypatch.setenv("SYSTEM_HEALTH_RESOURCE_HEALTH_ENABLED", "true")

    monkeypatch.setattr(system_health, "_default_layer_specs", lambda: [])

    now = datetime(2024, 1, 1, tzinfo=timezone.utc)

    app_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
    )
    resource_health_url = (
        "https://management.azure.com/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp"
        "/providers/Microsoft.ResourceHealth/availabilityStatuses/current"
    )
    responses: Dict[str, Dict[str, Any]] = {
        app_url: {
            "id": "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.App/containerApps/myapp",
            "properties": {"provisioningState": "Succeeded", "latestReadyRevisionName": "rev1"},
        },
        resource_health_url: {
            "properties": {"availabilityState": "Unavailable", "summary": "Outage", "reasonType": "Incident"}
        },
    }

    class FakeAzureArmClient:
        def __init__(self, cfg: Any) -> None:
            self._cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            sub = self._cfg.subscription_id
            rg = self._cfg.resource_group
            return (
                f"https://management.azure.com/subscriptions/{sub}"
                f"/resourceGroups/{rg}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def get_json(self, url: str, *, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
            return responses[url]

    monkeypatch.setattr(system_health, "AzureArmClient", FakeAzureArmClient)

    payload = system_health.collect_system_health_snapshot(now=now, include_resource_ids=False)
    assert payload["overall"] == "critical"
    assert payload["resources"][0]["status"] == "error"
    assert any(alert["title"] == "Azure resource health" and alert["severity"] == "error" for alert in payload["alerts"])
