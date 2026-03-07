from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from api.service.app import create_app
from core.debug_symbols import DebugSymbolsState
from tests.api._client import get_test_client


def _state(*, enabled: bool, symbols_raw: str, updated_by: str | None = "tester") -> DebugSymbolsState:
    return DebugSymbolsState(
        enabled=enabled,
        symbols_raw=symbols_raw,
        symbols=[],
        updated_at=datetime.now(timezone.utc),
        updated_by=updated_by,
    )


@pytest.mark.asyncio
async def test_get_debug_symbols_returns_runtime_config_backed_state(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch(
        "api.endpoints.system.read_debug_symbols_state",
        return_value=_state(enabled=True, symbols_raw="AAPL,MSFT"),
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.get("/api/system/debug-symbols")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["enabled"] is True
    assert payload["symbols"] == "AAPL,MSFT"
    assert payload["updatedBy"] == "tester"


@pytest.mark.asyncio
async def test_set_debug_symbols_updates_runtime_config_backed_state(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch(
        "api.endpoints.system.read_debug_symbols_state",
        return_value=_state(enabled=False, symbols_raw=""),
    ), patch(
        "api.endpoints.system.update_debug_symbols_state",
        return_value=_state(enabled=True, symbols_raw="AAPL,MSFT"),
    ) as update_mock:
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post(
                "/api/system/debug-symbols",
                json={"enabled": True, "symbols": '["aapl", "msft"]'},
            )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["enabled"] is True
    assert payload["symbols"] == "AAPL,MSFT"
    assert update_mock.call_args.kwargs["dsn"] == "postgresql://user:pass@localhost/db"
    assert update_mock.call_args.kwargs["enabled"] is True
    assert update_mock.call_args.kwargs["symbols"] == '["aapl", "msft"]'


@pytest.mark.asyncio
async def test_set_debug_symbols_rejects_empty_value_when_enabled(monkeypatch):
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch(
        "api.endpoints.system.read_debug_symbols_state",
        return_value=_state(enabled=False, symbols_raw=""),
    ):
        app = create_app()
        async with get_test_client(app) as client:
            resp = await client.post(
                "/api/system/debug-symbols",
                json={"enabled": True, "symbols": "   "},
            )

    assert resp.status_code == 400
    assert "required" in resp.json()["detail"].lower()
