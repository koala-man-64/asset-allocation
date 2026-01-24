from __future__ import annotations

from fastapi.testclient import TestClient


def test_backtest_service_exports_uvicorn_app() -> None:
    # Ensures `uvicorn api.service.app:app` works (used by docs + Dockerfile.backtest_api).
    from api.service import app as app_module

    with TestClient(app_module.app) as client:
        resp = client.get("/healthz")
        assert resp.status_code == 200
