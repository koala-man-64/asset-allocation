import httpx

import core.alpha_vantage_gateway_client as alpha_vantage_gateway_client_module
from core.alpha_vantage_gateway_client import (
    AlphaVantageGatewayClient,
    AlphaVantageGatewayClientConfig,
)


def test_warmup_probe_retries_before_first_request(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            if counters["warmup"] < 2:
                return httpx.Response(503, text="warming")
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            return httpx.Response(200, text="symbol,name\nAAPL,Apple\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=600.0,
            warmup_enabled=True,
            warmup_max_attempts=3,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
    )
    try:
        first = client.get_listing_status_csv()
        second = client.get_listing_status_csv()
    finally:
        http_client.close()

    assert "symbol,name" in first
    assert "symbol,name" in second
    assert counters["warmup"] == 2
    assert counters["data"] == 2


def test_warmup_can_be_disabled(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/alpha-vantage/listing-status":
            counters["data"] += 1
            return httpx.Response(200, text="symbol,name\nAAPL,Apple\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(alpha_vantage_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=600.0,
            warmup_enabled=False,
        ),
        http_client=http_client,
    )
    try:
        csv = client.get_listing_status_csv()
    finally:
        http_client.close()

    assert "symbol,name" in csv
    assert counters["warmup"] == 0
    assert counters["data"] == 1


def test_get_earnings_calendar_csv_calls_calendar_route(monkeypatch):
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["path"] = request.url.path
        observed["query"] = str(request.url.query)
        return httpx.Response(
            200,
            text="symbol,name,reportDate,fiscalDateEnding,estimate,currency,timeOfTheDay\nAAPL,Apple,2026-05-01,2026-03-31,1.5,USD,post-market\n",
        )

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=600.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
    )
    try:
        csv = client.get_earnings_calendar_csv(symbol="AAPL", horizon="6month")
    finally:
        http_client.close()

    assert "reportDate" in csv
    assert observed["path"] == "/api/providers/alpha-vantage/earnings-calendar"
    assert "symbol=AAPL" in str(observed["query"])
    assert "horizon=6month" in str(observed["query"])
