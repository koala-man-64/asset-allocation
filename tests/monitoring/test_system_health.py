from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from asset_allocation.backtest.service.app import create_app
from asset_allocation.monitoring.delta_log import find_latest_delta_version
from asset_allocation.monitoring.ttl_cache import TtlCache


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


def test_system_health_public_when_no_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.delenv("BACKTEST_AUTH_MODE", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_AUDIENCE", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/system/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert set(payload.keys()) >= {"overall", "dataLayers", "recentJobs", "alerts"}


def test_system_health_requires_api_key_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_API_KEY", "secret")
    monkeypatch.delenv("BACKTEST_AUTH_MODE", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_ISSUER", raising=False)
    monkeypatch.delenv("BACKTEST_OIDC_AUDIENCE", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/system/health")
        assert resp.status_code == 401

        resp2 = client.get("/system/health", headers={"X-API-Key": "secret"})
        assert resp2.status_code == 200

