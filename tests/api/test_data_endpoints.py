from fastapi.testclient import TestClient

from api.endpoints import data as data_endpoints
from api.service.app import app


def test_data_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_data(layer: str, domain: str, ticker: str | None = None):
        calls.append((layer, domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    with TestClient(app) as client:
        resp = client.get("/api/data/silver/market?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "market", "AAPL")]


def test_data_endpoint_rejects_unknown_layer():
    with TestClient(app) as client:
        resp = client.get("/api/data/platinum/market?ticker=AAPL")

    assert resp.status_code == 400


def test_bronze_endpoint_requires_ticker_for_generic_domains():
    with TestClient(app) as client:
        resp = client.get("/api/data/bronze/market")

    assert resp.status_code == 400


def test_finance_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_finance_data(layer: str, sub_domain: str, ticker: str):
        calls.append((layer, sub_domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_finance_data", fake_get_finance_data)

    with TestClient(app) as client:
        resp = client.get("/api/data/silver/finance/balance_sheet?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "balance_sheet", "AAPL")]
