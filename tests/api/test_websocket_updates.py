from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.service.app import create_app


def _set_required_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_MAX_CONCURRENT", "1")
    monkeypatch.setenv("BACKTEST_API_KEY_HEADER", "X-API-Key")
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "none")
    monkeypatch.setenv("BACKTEST_ALLOW_LOCAL_DATA", "false")
    monkeypatch.setenv(
        "BACKTEST_ADLS_CONTAINER_ALLOWLIST",
        "bronze,silver,gold,platinum,ranking-data,common,test-container",
    )
    monkeypatch.setenv("BACKTEST_RUN_STORE_MODE", "sqlite")
    monkeypatch.setenv("BACKTEST_CSP", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'")

    monkeypatch.delenv("BACKTEST_UI_DIST_DIR", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)


def test_websocket_updates_endpoint_accepts_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    app = create_app()

    with TestClient(app) as client:
        with client.websocket_connect("/api/ws/updates") as websocket:
            websocket.send_text("ping")
