import pandas as pd
from fastapi.testclient import TestClient

from asset_allocation.api import dependencies as deps
from asset_allocation.api.main import app


class _DummyDeltaTable:
    def __init__(self, rows):
        self._df = pd.DataFrame(rows)

    def to_pandas(self):
        return self._df


def test_market_alias_rejects_unknown_layer():
    client = TestClient(app)
    resp = client.get("/market/bronze/AAPL")
    assert resp.status_code == 400


def test_market_alias_silver_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"date": "2020-01-01", "open": 1.0}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    client = TestClient(app)
    resp = client.get("/market/silver/AAPL")
    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "silver", "market"),
        ("get_delta_table", "container:silver:market", "market-data/AAPL"),
    ]


def test_market_alias_gold_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"date": "2020-01-01", "close": 2.0}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    client = TestClient(app)
    resp = client.get("/market/gold/AAPL")
    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "gold", "market"),
        ("get_delta_table", "container:gold:market", "market/AAPL"),
    ]


def test_finance_alias_rejects_unknown_subdomain(monkeypatch):
    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        return f"container:{layer}:{domain}"

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)

    client = TestClient(app)
    resp = client.get("/finance/silver/not-a-domain/AAPL")
    assert resp.status_code == 400


def test_finance_alias_silver_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"date": "2020-01-01", "symbol": "AAPL"}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    client = TestClient(app)
    resp = client.get("/finance/silver/balance_sheet/AAPL")
    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "silver", "finance"),
        (
            "get_delta_table",
            "container:silver:finance",
            "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        ),
    ]


def test_finance_alias_gold_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"date": "2020-01-01", "symbol": "AAPL"}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    client = TestClient(app)
    resp = client.get("/finance/gold/all/AAPL")
    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "gold", "finance"),
        ("get_delta_table", "container:gold:finance", "finance/AAPL"),
    ]


def test_strategies_alias_calls_delta(monkeypatch):
    calls = []

    def fake_resolve_container(layer: str, domain: str | None = None) -> str:
        calls.append(("resolve_container", layer, domain))
        return f"container:{layer}:{domain}"

    def fake_get_delta_table(container: str, path: str):
        calls.append(("get_delta_table", container, path))
        return _DummyDeltaTable([{"id": "strategy-1", "name": "Test"}])

    monkeypatch.setattr(deps, "resolve_container", fake_resolve_container)
    monkeypatch.setattr(deps, "get_delta_table", fake_get_delta_table)

    client = TestClient(app)
    resp = client.get("/strategies")
    assert resp.status_code == 200
    assert calls == [
        ("resolve_container", "platinum", None),
        ("get_delta_table", "container:platinum:None", "strategies"),
    ]

