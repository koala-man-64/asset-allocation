from __future__ import annotations

import json
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from asset_allocation.backtest.service.app import create_app


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
        resp = client.get(f"/backtests/{run_id}/status", headers=headers)
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
    monkeypatch.delenv("BACKTEST_ALLOW_LOCAL_DATA", raising=False)
    monkeypatch.delenv("BACKTEST_ALLOWED_DATA_DIRS", raising=False)

    app = create_app()
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        payload = {"config": _make_config_dict(tmp_path, run_id="RUNTEST-SVC-LOCAL")}
        resp = client.post("/backtests", json=payload)
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
        resp = client.post("/backtests", json={"config": config, "run_id": run_id})
        assert resp.status_code == 200
        assert resp.json()["run_id"] == run_id

        status = _poll_status(client, run_id)
        assert status["status"] == "completed"

        summary = client.get(f"/backtests/{run_id}/summary")
        assert summary.status_code == 200
        assert summary.json()["run_id"] == run_id

        artifacts = client.get(f"/backtests/{run_id}/artifacts")
        assert artifacts.status_code == 200
        names = {item["name"] for item in artifacts.json()["local"]}
        assert "summary.json" in names
        assert "config.yaml" in names

        summary_blob = client.get(f"/backtests/{run_id}/artifacts/summary.json")
        assert summary_blob.status_code == 200


def test_service_requires_api_key_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_API_KEY", "secret")

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/backtests")
        assert resp.status_code == 401
        resp2 = client.get("/backtests", headers={"X-API-Key": "secret"})
        assert resp2.status_code == 200


class _LocalBlobStorageClient:
    def __init__(self, *, container_name: str, ensure_container_exists: bool = True):
        self._container = container_name
        self._root = Path.cwd() / ".pytest_blob_store"
        (self._root / self._container).mkdir(parents=True, exist_ok=True)

    def upload_file(self, local_path: str, remote_path: str):
        src = Path(local_path)
        dest = self._root / self._container / remote_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(src.read_bytes())

    def download_data(self, remote_path: str) -> Optional[bytes]:
        path = self._root / self._container / remote_path
        if not path.exists():
            return None
        return path.read_bytes()

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
    monkeypatch.setattr("asset_allocation.backtest.service.adls_uploader.BlobStorageClient", _LocalBlobStorageClient)
    monkeypatch.setattr("asset_allocation.backtest.service.artifacts.BlobStorageClient", _LocalBlobStorageClient)

    app = create_app()
    with TestClient(app) as client:
        _write_inputs(tmp_path)
        run_id = "RUNTEST-SVC-ADLS"
        config = _make_config_dict(tmp_path, run_id=run_id, adls_dir="silver/backtest-api-results")
        resp = client.post("/backtests", json={"config": config, "run_id": run_id})
        assert resp.status_code == 200

        status = _poll_status(client, run_id)
        assert status["status"] == "completed"
        assert status["adls_container"] == "silver"
        assert status["adls_prefix"].endswith(f"backtest-api-results/{run_id}")

        artifacts = client.get(f"/backtests/{run_id}/artifacts", params={"remote": "true"})
        assert artifacts.status_code == 200
        payload = artifacts.json()
        assert payload["remote"] is not None
        remote_names = {item["name"] for item in payload["remote"]}
        assert "summary.json" in remote_names
        assert "artifacts_manifest.json" in remote_names

        remote_summary = client.get(f"/backtests/{run_id}/artifacts/summary.json", params={"source": "adls"})
        assert remote_summary.status_code == 200
        summary_data = json.loads(remote_summary.content.decode("utf-8"))
        assert summary_data["run_id"] == run_id
