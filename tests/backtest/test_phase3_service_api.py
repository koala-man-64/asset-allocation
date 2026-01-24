from __future__ import annotations

import json
import os
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from fastapi.testclient import TestClient

from api.service.app import create_app


def _make_config_dict(tmp_path: Path, *, run_id: str, adls_dir: Optional[str] = None) -> Dict[str, Any]:
    config: Dict[str, Any] = {
        "run_name": "phase3_service",
        "start_date": "2020-01-01",
        "end_date": "2020-01-03",
        "initial_cash": 1000.0,
        "universe": {"symbols": ["AAA", "BBB"]},
        "data": {
            "price_source": "local",
            "price_path": str(tmp_path / "prices.csv"),
            "signal_path": str(tmp_path / "signals.csv"),
        },
        "strategy": {"class": "TopNSignalStrategy", "parameters": {"signal_column": "composite_percentile", "top_n": 1}},
        "sizing": {"class": "EqualWeightSizer", "parameters": {"max_positions": 10}},
        "constraints": {"max_leverage": 1.0, "max_position_size": 1.0, "allow_short": False},
        "broker": {"slippage_bps": 0.0, "commission": 0.0, "fill_policy": "next_open"},
        "output": {"local_dir": str(tmp_path), "adls_dir": adls_dir},
    }
    return config


def _write_inputs(tmp_path: Path) -> None:
    prices = pd.DataFrame(
        {
            "date": [
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 2),
                date(2020, 1, 2),
                date(2020, 1, 3),
                date(2020, 1, 3),
            ],
            "symbol": ["AAA", "BBB"] * 3,
            "open": [100.0, 200.0, 110.0, 210.0, 120.0, 220.0],
            "close": [101.0, 201.0, 111.0, 211.0, 121.0, 221.0],
        }
    )
    signals = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 2)],
            "symbol": ["AAA", "BBB"] * 2,
            "composite_percentile": [1.0, 0.0, 1.0, 0.0],
        }
    )
    prices.to_csv(tmp_path / "prices.csv", index=False)
    signals.to_csv(tmp_path / "signals.csv", index=False)


def _poll_status(client: TestClient, run_id: str, *, timeout_s: float = 10.0, api_key: Optional[str] = None) -> Dict[str, Any]:
    deadline = time.time() + timeout_s
    headers = {"X-API-Key": api_key} if api_key else {}
    while time.time() < deadline:
        resp = client.get(f"/api/backtests/{run_id}/status", headers=headers)
        assert resp.status_code == 200
        payload = resp.json()
        if payload["status"] in {"completed", "failed"}:
            return payload
        time.sleep(0.05)
    raise TimeoutError(f"Run did not finish in {timeout_s}s: {run_id}")


def test_service_rejects_local_data_when_disabled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_ALLOW_LOCAL_DATA", "false")
    monkeypatch.delenv("BACKTEST_ALLOWED_DATA_DIRS", raising=False)

    app = create_app()
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        payload = {"config": _make_config_dict(tmp_path, run_id="RUNTEST-SVC-LOCAL")}
        resp = client.post("/api/backtests", json=payload)
        assert resp.status_code == 400
        assert "price_source=local" in resp.json()["detail"]


def test_service_runs_backtest_and_serves_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    out_dir = tmp_path / "out"
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(out_dir))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_ALLOW_LOCAL_DATA", "true")
    monkeypatch.setenv("BACKTEST_ALLOWED_DATA_DIRS", str(tmp_path))

    app = create_app()
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        run_id = "RUNTEST-SVC-0001"
        config = _make_config_dict(tmp_path, run_id=run_id)
        resp = client.post("/api/backtests", json={"config": config, "run_id": run_id})
        assert resp.status_code == 200
        assert resp.json()["run_id"] == run_id

        status = _poll_status(client, run_id)
        assert status["status"] == "completed"

        summary = client.get(f"/api/backtests/{run_id}/summary")
        assert summary.status_code == 200
        assert summary.json()["run_id"] == run_id

        artifacts = client.get(f"/api/backtests/{run_id}/artifacts")
        assert artifacts.status_code == 200
        names = {item["name"] for item in artifacts.json()["local"]}
        assert "summary.json" in names
        assert "config.yaml" in names

        summary_blob = client.get(f"/api/backtests/{run_id}/artifacts/summary.json")
        assert summary_blob.status_code == 200


def test_service_serves_ui_when_dist_dir_present(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "oidc")
    monkeypatch.setenv("BACKTEST_OIDC_ISSUER", "https://login.microsoftonline.com/tenant/v2.0")
    monkeypatch.setenv("BACKTEST_OIDC_AUDIENCE", "api://backtest-api")
    monkeypatch.setenv("BACKTEST_UI_OIDC_CLIENT_ID", "ui-client-id")
    monkeypatch.setenv("BACKTEST_UI_OIDC_SCOPES", "api://backtest-api/backtests.read api://backtest-api/backtests.write")

    ui_dist = tmp_path / "ui-dist"
    assets_dir = ui_dist / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)
    (ui_dist / "index.html").write_text("<!doctype html><html><body>ok</body></html>", encoding="utf-8")
    (assets_dir / "app.js").write_text("console.log('ok')", encoding="utf-8")
    monkeypatch.setenv("BACKTEST_UI_DIST_DIR", str(ui_dist))

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
        assert resp.headers.get("cache-control") == "no-store"
        assert resp.headers.get("content-security-policy")
        assert resp.headers.get("x-frame-options") == "DENY"
        assert "ok" in resp.text

        config_js = client.get("/config.js")
        assert config_js.status_code == 200
        assert "application/javascript" in config_js.headers.get("content-type", "")
        assert config_js.headers.get("cache-control") == "no-store"
        assert "window.__BACKTEST_UI_CONFIG__" in config_js.text
        assert '"authMode": "oidc"' in config_js.text
        assert '"oidcClientId": "ui-client-id"' in config_js.text

        asset = client.get("/assets/app.js")
        assert asset.status_code == 200
        assert asset.headers.get("cache-control") == "public, max-age=31536000, immutable"

        fallback = client.get("/some/deep/link")
        assert fallback.status_code == 200
        assert "text/html" in fallback.headers.get("content-type", "")

        redirect = client.get("/backtests/", follow_redirects=False)
        assert redirect.status_code == 307
        assert redirect.headers.get("location", "").endswith("/backtests")


def test_service_requires_api_key_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "api_key")
    monkeypatch.setenv("BACKTEST_API_KEY", "secret")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/backtests")
        assert resp.status_code == 401
        resp2 = client.get("/api/backtests", headers={"X-API-Key": "secret"})
        assert resp2.status_code == 200


def test_service_honors_custom_api_key_header(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_AUTH_MODE", "api_key")
    monkeypatch.setenv("BACKTEST_API_KEY", "secret")
    monkeypatch.setenv("BACKTEST_API_KEY_HEADER", "X-Backtest-Key")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/backtests", headers={"X-API-Key": "secret"})
        assert resp.status_code == 401

        resp2 = client.get("/api/backtests", headers={"X-Backtest-Key": "secret"})
        assert resp2.status_code == 200


class _FakeHttpResponse:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Dict[str, Any]:
        return self._payload


def test_service_accepts_oidc_bearer_tokens_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)

    issuer = "https://issuer.example"
    audience = "api://backtest-api"
    jwks_url = "https://issuer.example/jwks"

    monkeypatch.setenv("BACKTEST_AUTH_MODE", "oidc")
    monkeypatch.setenv("BACKTEST_OIDC_ISSUER", issuer)
    monkeypatch.setenv("BACKTEST_OIDC_AUDIENCE", audience)
    monkeypatch.setenv("BACKTEST_OIDC_JWKS_URL", jwks_url)

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_jwk = json.loads(RSAAlgorithm.to_jwk(private_key.public_key()))
    public_jwk.update({"kid": "test-kid", "use": "sig", "alg": "RS256"})
    jwks = {"keys": [public_jwk]}

    token = jwt.encode(
        {
            "iss": issuer,
            "aud": audience,
            "sub": "user-123",
            "exp": int(time.time()) + 3600,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "test-kid"},
    )

    def _fake_requests_get(url: str, timeout=None):
        assert url == jwks_url
        return _FakeHttpResponse(jwks)

    monkeypatch.setattr("api.service.auth.requests.get", _fake_requests_get)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/api/backtests")
        assert resp.status_code == 401

        resp2 = client.get("/api/backtests", headers={"Authorization": f"Bearer {token}"})
        assert resp2.status_code == 200


class _LocalBlobStorageClient:
    def __init__(self, *, container_name: str, ensure_container_exists: bool = True):
        self._container = container_name
        self._root = Path.cwd() / ".pytest_blob_store"
        (self._root / self._container).mkdir(parents=True, exist_ok=True)

    def _atomic_write_bytes(self, dest: Path, data: bytes) -> None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(prefix=f"{dest.name}.", suffix=".tmp", dir=str(dest.parent))
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
            os.replace(tmp_path, dest)
        finally:
            try:
                os.remove(tmp_path)
            except FileNotFoundError:
                pass

    def file_exists(self, remote_path: str) -> bool:
        path = self._root / self._container / remote_path
        return path.exists()

    def upload_file(self, local_path: str, remote_path: str):
        self.upload_data(remote_path, Path(local_path).read_bytes(), overwrite=True)

    def upload_data(self, remote_path: str, data: bytes, overwrite: bool = True):
        dest = self._root / self._container / remote_path
        self._atomic_write_bytes(dest, data)

    def download_data(self, remote_path: str) -> Optional[bytes]:
        path = self._root / self._container / remote_path
        # Be resilient to brief filesystem-level replace windows (e.g., atomic writes on mounted filesystems).
        for _ in range(5):
            if path.exists():
                try:
                    return path.read_bytes()
                except FileNotFoundError:
                    pass
            time.sleep(0.005)
        return None

    def list_blob_infos(self, name_starts_with: Optional[str] = None) -> List[Dict[str, Any]]:
        base = self._root / self._container
        out: List[Dict[str, Any]] = []
        prefix = (name_starts_with or "").lstrip("/")
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            rel = str(path.relative_to(base)).replace("\\", "/")
            if prefix and not rel.startswith(prefix):
                continue
            stat = path.stat()
            out.append({"name": rel, "last_modified": None, "size": stat.st_size})
        return out


def test_service_uploads_artifacts_when_adls_dir_set(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    out_dir = tmp_path / "out"
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(out_dir))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_ALLOW_LOCAL_DATA", "true")
    monkeypatch.setenv("BACKTEST_ALLOWED_DATA_DIRS", str(tmp_path))

    # Patch Azure client to a local fake to avoid network.
    monkeypatch.setattr("api.service.adls_uploader.BlobStorageClient", _LocalBlobStorageClient)
    monkeypatch.setattr("api.service.artifacts.BlobStorageClient", _LocalBlobStorageClient)

    app = create_app()
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        run_id = "RUNTEST-SVC-ADLS"
        from core import config_shared as cfg
        silver_container = cfg.AZURE_CONTAINER_SILVER or "silver"
        config = _make_config_dict(tmp_path, run_id=run_id, adls_dir=f"{silver_container}/backtest-api-results")
        resp = client.post("/api/backtests", json={"config": config, "run_id": run_id})
        assert resp.status_code == 200

        status = _poll_status(client, run_id)
        assert status["status"] == "completed"
        from core import config_shared as cfg
        assert status["adls_container"] == (cfg.AZURE_CONTAINER_SILVER or "silver")
        assert status["adls_prefix"].endswith(f"backtest-api-results/{run_id}")

        artifacts = client.get(f"/api/backtests/{run_id}/artifacts", params={"remote": "true"})
        assert artifacts.status_code == 200
        payload = artifacts.json()
        assert payload["remote"] is not None
        remote_names = {item["name"] for item in payload["remote"]}
        assert "summary.json" in remote_names
        assert "artifacts_manifest.json" in remote_names

        remote_summary = client.get(f"/api/backtests/{run_id}/artifacts/summary.json", params={"source": "adls"})
        assert remote_summary.status_code == 200
        summary_data = json.loads(remote_summary.content.decode("utf-8"))
        assert summary_data["run_id"] == run_id


def test_service_adls_run_store_persists_runs_and_serves_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)

    out_dir = tmp_path / "out"
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(out_dir))
    monkeypatch.setenv("BACKTEST_RUN_STORE_MODE", "adls")
    from core import config_shared as cfg
    silver_container = cfg.AZURE_CONTAINER_SILVER or "silver"
    monkeypatch.setenv("BACKTEST_ADLS_RUNS_DIR", f"{silver_container}/backtest-run-store")
    monkeypatch.delenv("BACKTEST_API_KEY", raising=False)
    monkeypatch.setenv("BACKTEST_ALLOW_LOCAL_DATA", "true")
    monkeypatch.setenv("BACKTEST_ALLOWED_DATA_DIRS", str(tmp_path))

    monkeypatch.setattr("api.service.adls_run_store.BlobStorageClient", _LocalBlobStorageClient)
    monkeypatch.setattr("api.service.adls_uploader.BlobStorageClient", _LocalBlobStorageClient)
    monkeypatch.setattr("api.service.artifacts.BlobStorageClient", _LocalBlobStorageClient)

    app = create_app()
    run_id = "RUNTEST-SVC-ADLS-STORE"
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        config = _make_config_dict(tmp_path, run_id=run_id, adls_dir=None)
        resp = client.post("/api/backtests", json={"config": config, "run_id": run_id})
        assert resp.status_code == 200

        status = _poll_status(client, run_id)
        assert status["status"] == "completed"
        from core import config_shared as cfg
        assert status["adls_container"] == (cfg.AZURE_CONTAINER_SILVER or "silver")
        assert status["adls_prefix"].endswith(f"backtest-run-store/{run_id}")

        runs = client.get("/api/backtests")
        assert runs.status_code == 200
        run_ids = {r["run_id"] for r in runs.json()["runs"]}
        assert run_id in run_ids

        summary = client.get(f"/api/backtests/{run_id}/summary", params={"source": "adls"})
        assert summary.status_code == 200
        assert summary.json()["run_id"] == run_id

        ts = client.get(f"/api/backtests/{run_id}/metrics/timeseries", params={"source": "adls", "max_points": 1000})
        assert ts.status_code == 200
        assert ts.json()["total_points"] >= 1
        assert len(ts.json()["points"]) >= 1

        rolling = client.get(
            f"/api/backtests/{run_id}/metrics/rolling",
            params={"source": "adls", "window_days": 21, "max_points": 1000},
        )
        assert rolling.status_code == 200

        trades = client.get(f"/api/backtests/{run_id}/trades", params={"source": "adls"})
        assert trades.status_code == 200
        assert trades.json()["total"] >= 0

    # Simulate restart: change output dir so local artifacts are missing and rely on ADLS reads.
    out_dir_2 = tmp_path / "out2"
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(out_dir_2))
    app2 = create_app()
    with TestClient(app2) as client2:
        summary2 = client2.get(f"/api/backtests/{run_id}/summary")
        assert summary2.status_code == 200
        assert summary2.json()["run_id"] == run_id


def test_service_job_trigger_requires_arm_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", raising=False)
    monkeypatch.delenv("SYSTEM_HEALTH_ARM_JOBS", raising=False)

    app = create_app()
    with TestClient(app) as client:
        resp = client.post("/api/system/jobs/platinum-ranking-job/run")
        assert resp.status_code == 503
        assert "not configured" in resp.json()["detail"].lower()


def test_service_job_trigger_rejects_unknown_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub-123")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg-123")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "platinum-ranking-job")

    app = create_app()
    with TestClient(app) as client:
        resp = client.post("/api/system/jobs/unknown-job/run")
        assert resp.status_code == 404


def test_service_job_trigger_starts_allowed_job(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub-123")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg-123")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_JOBS", "platinum-ranking-job")

    calls: List[str] = []

    class FakeAzureArmClient:
        def __init__(self, cfg):
            self.cfg = cfg

        def __enter__(self) -> "FakeAzureArmClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
            return None

        def resource_url(self, *, provider: str, resource_type: str, name: str) -> str:
            return (
                f"https://management.azure.com/subscriptions/{self.cfg.subscription_id}"
                f"/resourceGroups/{self.cfg.resource_group}"
                f"/providers/{provider}/{resource_type}/{name}"
            )

        def post_json(self, url: str, *, params=None, json_body=None):  # type: ignore[no-untyped-def]
            calls.append(url)
            return {"id": "execution-id", "name": "execution-name"}

    monkeypatch.setattr("api.endpoints.system.AzureArmClient", FakeAzureArmClient)

    app = create_app()
    with TestClient(app) as client:
        resp = client.post("/api/system/jobs/platinum-ranking-job/run")
        assert resp.status_code == 202
        payload = resp.json()
        assert payload["jobName"] == "platinum-ranking-job"
        assert payload["status"] == "queued"
        assert payload["executionId"] == "execution-id"
        assert payload["executionName"] == "execution-name"

    assert calls == [
        "https://management.azure.com/subscriptions/sub-123/resourceGroups/rg-123/providers/Microsoft.App/jobs/platinum-ranking-job/start"
    ]
