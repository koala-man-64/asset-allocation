from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.service.app import create_app
from api.service.settings import ServiceSettings


def test_settings_requires_postgres_dsn_when_mode_postgres(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_RUN_STORE_MODE", "postgres")
    monkeypatch.delenv("BACKTEST_POSTGRES_DSN", raising=False)

    with pytest.raises(ValueError, match="BACKTEST_POSTGRES_DSN"):
        ServiceSettings.from_env()

    monkeypatch.setenv(
        "BACKTEST_POSTGRES_DSN",
        "postgresql://backtest_service:pw@localhost:5432/asset_allocation?sslmode=require",
    )
    settings = ServiceSettings.from_env()
    assert settings.run_store_mode == "postgres"
    assert settings.postgres_dsn and "postgresql://" in settings.postgres_dsn


def test_create_app_uses_postgres_run_store_when_configured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BACKTEST_OUTPUT_DIR", str(tmp_path / "out"))
    monkeypatch.setenv("BACKTEST_DB_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("BACKTEST_RUN_STORE_MODE", "postgres")
    monkeypatch.setenv(
        "BACKTEST_POSTGRES_DSN",
        "postgresql://backtest_service:pw@localhost:5432/asset_allocation?sslmode=require",
    )

    class _FakePostgresRunStore:
        def __init__(self, dsn: str):
            self.dsn = dsn

        def init_db(self) -> None:
            return None

        def ping(self) -> None:
            return None

        def reconcile_incomplete_runs(self) -> int:
            return 0

        def create_run(self, **_: object) -> None:
            raise NotImplementedError

        def update_run(self, *_: object, **__: object) -> None:
            raise NotImplementedError

        def get_run(self, *_: object, **__: object):
            raise NotImplementedError

        def list_runs(self, **_: object):
            raise NotImplementedError

    monkeypatch.setattr("api.service.app.PostgresRunStore", _FakePostgresRunStore)

    app = create_app()
    with TestClient(app) as client:
        store = client.app.state.store
        assert isinstance(store, _FakePostgresRunStore)
        assert "postgresql://" in store.dsn

