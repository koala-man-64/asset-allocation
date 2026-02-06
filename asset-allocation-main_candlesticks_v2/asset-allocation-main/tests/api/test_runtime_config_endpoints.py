from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from api.service.app import create_app
from core.runtime_config import RuntimeConfigItem
from tests.api._client import get_test_client


def _item(*, key: str, value: str, enabled: bool = True, scope: str = "global") -> RuntimeConfigItem:
    return RuntimeConfigItem(
        scope=scope,
        key=key,
        enabled=enabled,
        value=value,
        description="desc",
        updated_at=datetime.now(timezone.utc),
        updated_by="tester",
    )


@pytest.mark.asyncio
async def test_runtime_config_catalog(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/runtime-config/catalog")
    assert resp.status_code == 200
    payload = resp.json()
    assert "items" in payload
    keys = [item.get("key") for item in payload["items"]]
    assert "BACKFILL_START_DATE" in keys


@pytest.mark.asyncio
async def test_get_runtime_config_requires_postgres(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/system/runtime-config?scope=global")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_get_runtime_config_returns_items(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    rows = [_item(key="BACKFILL_START_DATE", value="2024-01-01")]
    with patch("api.endpoints.system.list_runtime_config", return_value=rows):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/runtime-config?scope=global")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["scope"] == "global"
    assert payload["items"][0]["key"] == "BACKFILL_START_DATE"
    assert payload["items"][0]["value"] == "2024-01-01"


@pytest.mark.asyncio
async def test_set_runtime_config_rejects_forbidden_key(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/system/runtime-config",
            json={"key": "POSTGRES_DSN", "enabled": True, "value": "nope"},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_runtime_config_rejects_invalid_value(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.post(
            "/api/system/runtime-config",
            json={"key": "SYSTEM_HEALTH_TTL_SECONDS", "enabled": True, "value": ""},
        )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_set_runtime_config_normalizes_value_before_upsert(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    def _fake_upsert(**kwargs):
        # Ensure value has been normalized to an integer string.
        assert kwargs["value"] == "3"
        return _item(key=kwargs["key"], value=kwargs["value"], enabled=kwargs["enabled"], scope=kwargs["scope"])

    with patch("api.endpoints.system.upsert_runtime_config", side_effect=_fake_upsert):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post(
                "/api/system/runtime-config",
                json={
                    "key": "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS",
                    "scope": "global",
                    "enabled": True,
                    "value": " 3 ",
                },
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["key"] == "TRIGGER_NEXT_JOB_RETRY_ATTEMPTS"
    assert payload["value"] == "3"
