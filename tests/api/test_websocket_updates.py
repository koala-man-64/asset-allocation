from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import anyio
import pytest

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


class _FakeStreamingLogAnalyticsClient:
    def __init__(self, *, timeout_seconds: float = 5.0) -> None:
        self.timeout_seconds = timeout_seconds
        self.queries: list[tuple[str, str, str | None]] = []

    def __enter__(self) -> "_FakeStreamingLogAnalyticsClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def query(self, *, workspace_id: str, query: str, timespan: str | None = None):
        self.queries.append((workspace_id, query, timespan))
        return {
            "tables": [
                {
                    "columns": [
                        {"name": "TimeGenerated", "type": "datetime"},
                        {"name": "executionName", "type": "string"},
                        {"name": "stream_s", "type": "string"},
                        {"name": "msg", "type": "string"},
                    ],
                    "rows": [
                        [
                            "2026-02-10T00:00:00Z",
                            "bronze-market-job-exec-001",
                            "stderr",
                            "stream log line",
                        ],
                    ],
                }
            ]
        }


@pytest.mark.asyncio
async def test_websocket_job_log_stream(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(tmp_path, monkeypatch)
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED", "true")
    monkeypatch.setenv("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID", "workspace-id")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "bronze-market-job")
    monkeypatch.setenv("REALTIME_LOG_STREAM_POLL_SECONDS", "1")
    monkeypatch.setenv("REALTIME_LOG_STREAM_LOOKBACK_SECONDS", "30")
    monkeypatch.setenv("REALTIME_LOG_STREAM_BATCH_SIZE", "20")

    from api.service.app import realtime_manager

    fake_logs = _FakeStreamingLogAnalyticsClient()
    with patch("api.service.log_streaming.AzureLogAnalyticsClient", return_value=fake_logs):
        app = create_app()
        realtime_manager.active_connections.clear()
        realtime_manager.subscriptions.clear()

        async with connect_websocket(app, "/api/ws/updates") as ws:
            await ws.send_text(json.dumps({"action": "subscribe", "topics": ["job-logs:bronze-market-job"]}))

            with anyio.fail_after(2):
                msg = await ws.receive_json()

    assert msg["topic"] == "job-logs:bronze-market-job"
    assert msg["data"]["type"] == "CONSOLE_LOG_STREAM"
    payload = msg["data"]["payload"]
    assert payload["resourceType"] == "job"
    assert payload["resourceName"] == "bronze-market-job"
    assert payload["lines"][0]["message"] == "stream log line"
    assert payload["lines"][0]["timestamp"] == "2026-02-10T00:00:00Z"
    assert payload["lines"][0]["executionName"] == "bronze-market-job-exec-001"
    assert payload["lines"][0]["stream_s"] == "stderr"
    assert fake_logs.queries
