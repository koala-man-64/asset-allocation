from __future__ import annotations

import json

import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


def _parse_window_assignment(body: str, window_key: str) -> dict:
    prefix = f"window.{window_key} ="
    for line in body.splitlines():
        if not line.startswith(prefix):
            continue
        payload = line.split("=", 1)[1].strip()
        if payload.endswith(";"):
            payload = payload[:-1].strip()
        return json.loads(payload)
    raise AssertionError(f"Missing {prefix} assignment in /config.js response.")


@pytest.mark.asyncio
async def test_config_js_emits_api_base_url_from_root_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    monkeypatch.delenv("UI_API_BASE_URL", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/config.js")

    assert resp.status_code == 200
    assert "application/javascript" in resp.headers.get("content-type", "")
    assert "no-store" in resp.headers.get("cache-control", "").lower()

    cfg_backtest = _parse_window_assignment(resp.text, "__BACKTEST_UI_CONFIG__")
    cfg_api = _parse_window_assignment(resp.text, "__API_UI_CONFIG__")
    assert cfg_backtest == cfg_api

    assert cfg_backtest["apiBaseUrl"] == "/asset-allocation/api"
    assert cfg_backtest["backtestApiBaseUrl"] == "/asset-allocation/api"


@pytest.mark.asyncio
async def test_config_js_honors_ui_api_base_url_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("API_ROOT_PREFIX", "asset-allocation")
    monkeypatch.setenv("UI_API_BASE_URL", "/api")

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/config.js")

    assert resp.status_code == 200
    cfg = _parse_window_assignment(resp.text, "__BACKTEST_UI_CONFIG__")
    assert cfg["apiBaseUrl"] == "/api"
    assert cfg["backtestApiBaseUrl"] == "/api"

