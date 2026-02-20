import httpx

import core.massive_gateway_client as massive_gateway_client_module
from core.massive_gateway_client import MassiveGatewayClient, MassiveGatewayClientConfig


def test_build_headers_includes_caller_context(monkeypatch):
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-market-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-market-job-abc123")

    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key="test",
            api_key_header="X-API-Key",
            timeout_seconds=10.0,
        )
    )

    headers = client._build_headers()
    assert headers["X-API-Key"] == "test"
    assert headers["X-Caller-Job"] == "bronze-market-job"
    assert headers["X-Caller-Execution"] == "bronze-market-job-abc123"


def test_from_env_enforces_timeout_floor(monkeypatch):
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("ASSET_ALLOCATION_API_TIMEOUT_SECONDS", "5")

    client = MassiveGatewayClient.from_env()
    try:
        assert client.config.timeout_seconds >= 60.0
    finally:
        client.close()


def test_warmup_probe_retries_before_first_request(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            if counters["warmup"] < 3:
                return httpx.Response(503, text="warming")
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/massive/time-series/daily":
            counters["data"] += 1
            return httpx.Response(200, text="Date,Open,High,Low,Close,Volume\n2026-01-01,1,1,1,1,1\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(massive_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=60.0,
            warmup_enabled=True,
            warmup_max_attempts=3,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
    )
    try:
        first = client.get_daily_time_series_csv(symbol="AAPL")
        second = client.get_daily_time_series_csv(symbol="MSFT")
    finally:
        http_client.close()

    assert "Date,Open,High,Low,Close,Volume" in first
    assert "Date,Open,High,Low,Close,Volume" in second
    assert counters["warmup"] == 3
    assert counters["data"] == 2


def test_warmup_can_be_disabled(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(200, text="ok")
        if request.url.path == "/api/providers/massive/time-series/daily":
            counters["data"] += 1
            return httpx.Response(200, text="Date,Open,High,Low,Close,Volume\n2026-01-01,1,1,1,1,1\n")
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    monkeypatch.setattr(massive_gateway_client_module.time, "sleep", lambda _seconds: None)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=60.0,
            warmup_enabled=False,
        ),
        http_client=http_client,
    )
    try:
        csv = client.get_daily_time_series_csv(symbol="AAPL")
    finally:
        http_client.close()

    assert "Date,Open,High,Low,Close,Volume" in csv
    assert counters["warmup"] == 0
    assert counters["data"] == 1
