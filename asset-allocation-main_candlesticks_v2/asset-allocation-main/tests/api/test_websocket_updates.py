from __future__ import annotations

from pathlib import Path

import pytest
import anyio

from api.service.app import create_app
from tests.api._websocket import connect_websocket


def _set_required_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_KEY_HEADER", "X-API-Key")
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setenv("API_CSP", "default-src 'self'; base-uri 'none'; frame-ancestors 'none'")

    monkeypatch.delenv("UI_DIST_DIR", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)


@pytest.mark.asyncio
async def test_websocket_updates_endpoint_accepts_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    app = create_app()

    async with connect_websocket(app, "/api/ws/updates") as websocket:
        await websocket.send_text("ping")
        assert await websocket.receive_text() == "pong"

@pytest.mark.asyncio
async def test_websocket_pubsub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    
    # Mock the manager used by app
    from api.service.app import realtime_manager
    import json
    
    app = create_app()
    
    # Ensure a clean manager across tests.
    realtime_manager.active_connections.clear()
    realtime_manager.subscriptions.clear()

    async with connect_websocket(app, "/api/ws/updates") as ws:
        # 1. Subscribe to "test-topic"
        await ws.send_text(json.dumps({"action": "subscribe", "topics": ["test-topic"]}))

        # Give the server loop a chance to process the subscribe message.
        with anyio.fail_after(2):
            while len(realtime_manager.subscriptions.get("test-topic", set())) < 1:
                await anyio.sleep(0)

        # 2. Broadcast to "test-topic"
        await realtime_manager.broadcast("test-topic", {"status": "ok"})

        # 3. Verify receipt
        msg = await ws.receive_json()
        assert msg["topic"] == "test-topic"
        assert msg["data"]["status"] == "ok"

        # 4. Broadcast to other topic
        await realtime_manager.broadcast("other-topic", {"status": "ignored"})

        # 5. Verify connection stays alive (and no unexpected queued messages)
        await ws.send_text("ping")
        assert await ws.receive_text() == "pong"
