from __future__ import annotations

from fastapi.testclient import TestClient


def test_backtest_service_exports_uvicorn_app() -> None:
    # Ensures `uvicorn services.backtest_api.app:app` works (used by Dockerfile.backtest_api).
    from services.backtest_api import app as app_module

    with TestClient(app_module.app) as client:
        resp = client.get("/healthz")
        assert resp.status_code == 200
