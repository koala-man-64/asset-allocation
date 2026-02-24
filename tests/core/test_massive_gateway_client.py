import httpx

import core.massive_gateway_client as massive_gateway_client_module
from core.massive_gateway_client import (
    MassiveGatewayClient,
    MassiveGatewayClientConfig,
    MassiveGatewayUnavailableError,
)


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


def test_public_warmup_gateway_reports_failure(monkeypatch):
    counters = {"warmup": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(503, text="warming")
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
            warmup_max_attempts=2,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
        ),
        http_client=http_client,
    )
    try:
        assert client.warm_up_gateway() is False
    finally:
        http_client.close()

    assert counters["warmup"] == 2


def test_request_fails_fast_when_readiness_never_recovers(monkeypatch):
    counters = {"warmup": 0, "data": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/healthz":
            counters["warmup"] += 1
            return httpx.Response(503, text="warming")
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
            warmup_max_attempts=1,
            warmup_base_delay_seconds=0.0,
            warmup_max_delay_seconds=0.0,
            warmup_probe_timeout_seconds=1.0,
            readiness_enabled=True,
            readiness_max_attempts=2,
            readiness_sleep_seconds=0.0,
        ),
        http_client=http_client,
    )
    try:
        try:
            client.get_daily_time_series_csv(symbol="AAPL")
            raise AssertionError("Expected MassiveGatewayUnavailableError")
        except MassiveGatewayUnavailableError:
            pass
    finally:
        http_client.close()

    assert counters["warmup"] == 2
    assert counters["data"] == 0


def test_unified_snapshot_uses_batch_api_route() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path == "/api/providers/massive/snapshot":
            return httpx.Response(200, json={"results": [{"ticker": "AAPL"}]})
        raise AssertionError(f"Unexpected path: {request.url.path}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), timeout=httpx.Timeout(5.0), trust_env=False)
    client = MassiveGatewayClient(
        MassiveGatewayClientConfig(
            base_url="http://asset-allocation-api",
            api_key=None,
            api_key_header="X-API-Key",
            timeout_seconds=60.0,
            warmup_enabled=False,
            readiness_enabled=False,
        ),
        http_client=http_client,
    )
    try:
        payload = client.get_unified_snapshot(symbols=["aapl", "MSFT", "AAPL"], asset_type="stocks")
    finally:
        http_client.close()

    assert payload["results"][0]["ticker"] == "AAPL"
    assert seen[0][0] == "/api/providers/massive/snapshot"
    assert seen[0][1].get("symbols") == "AAPL,MSFT"
    assert seen[0][1].get("type") == "stocks"
