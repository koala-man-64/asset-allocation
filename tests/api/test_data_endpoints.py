import pytest

from api.endpoints import data as data_endpoints
from api.service.app import create_app
from tests.api._client import get_test_client


@pytest.mark.asyncio
async def test_data_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_data(layer: str, domain: str, ticker: str | None = None):
        calls.append((layer, domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/market?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "market", "AAPL")]


@pytest.mark.asyncio
async def test_data_endpoint_rejects_unknown_layer():
    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/platinum/market?ticker=AAPL")

    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_bronze_endpoint_allows_missing_ticker_for_generic_domains(monkeypatch):
    calls = []

    def fake_get_data(layer: str, domain: str, ticker: str | None = None):
        calls.append((layer, domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_data", fake_get_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/bronze/market")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("bronze", "market", None)]


@pytest.mark.asyncio
async def test_finance_endpoint_calls_service(monkeypatch):
    calls = []

    def fake_get_finance_data(layer: str, sub_domain: str, ticker: str | None = None):
        calls.append((layer, sub_domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_finance_data", fake_get_finance_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/silver/finance/balance_sheet?ticker=AAPL")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("silver", "balance_sheet", "AAPL")]


@pytest.mark.asyncio
async def test_bronze_finance_endpoint_allows_missing_ticker(monkeypatch):
    calls = []

    def fake_get_finance_data(layer: str, sub_domain: str, ticker: str | None = None):
        calls.append((layer, sub_domain, ticker))
        return [{"ok": True}]

    monkeypatch.setattr(data_endpoints.DataService, "get_finance_data", fake_get_finance_data)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/bronze/finance/balance_sheet")

    assert resp.status_code == 200
    assert resp.json() == [{"ok": True}]
    assert calls == [("bronze", "balance_sheet", None)]
