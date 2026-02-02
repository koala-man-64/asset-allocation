from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.service.app import create_app


def _set_required_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY_HEADER", "X-API-Key")
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("API_CSP", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'")

    monkeypatch.delenv("UI_DIST_DIR", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)


def test_websocket_updates_endpoint_accepts_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    app = create_app()

    with TestClient(app) as client:
        with client.websocket_connect("/api/ws/updates") as websocket:
            websocket.send_text("ping")
            websocket.receive_text()

def test_websocket_pubsub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    
    # Mock the manager used by app
    from api.service.app import realtime_manager
    import json
    
    app = create_app()
    
    with TestClient(app) as client:
        with client.websocket_connect("/api/ws/updates") as ws:
            # 1. Subscribe to "test-topic"
            ws.send_text(json.dumps({"action": "subscribe", "topics": ["test-topic"]}))
            
            # 2. Broadcast to "test-topic" (need async context usually, but Starlette TestClient runs sync)
            # We can invoke the manager directly if we assume it's the same instance
            import asyncio
            asyncio.run(realtime_manager.broadcast("test-topic", {"status": "ok"}))
            
            # 3. Verify receipt
            msg = ws.receive_json()
            assert msg["topic"] == "test-topic"
            assert msg["data"]["status"] == "ok"
            
            # 4. Broadcast to other topic
            asyncio.run(realtime_manager.broadcast("other-topic", {"status": "ignored"}))
            
            # 5. Verify no message (ping/pong to ensure connection is alive and empty)
            ws.send_text("ping")
            pong = ws.receive_text()
            assert pong == "pong"
