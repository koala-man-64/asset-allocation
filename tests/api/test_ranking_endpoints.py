import pandas as pd
from fastapi.testclient import TestClient

from api import dependencies as deps
from api.service.app import app, create_app


class _DummyDeltaTable:
    def __init__(self, rows):
        self._df = pd.DataFrame(rows)

    def to_pandas(self):
        return self._df


def test_ranking_strategies_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"id": "strategy-1", "name": "Test"}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    with TestClient(app) as client:
        resp = client.get("/api/ranking/strategies")

    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "platinum", None),
        ("get_delta_table", "container:platinum:None", "strategies"),
    ]


def test_ranking_signals_requires_postgres_dsn(monkeypatch):
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    local_app = create_app()
    with TestClient(local_app) as client:
        resp = client.get("/api/ranking/signals")

    assert resp.status_code == 503
