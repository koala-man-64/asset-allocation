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
