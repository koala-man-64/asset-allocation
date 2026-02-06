from core.alpha_vantage_gateway_client import AlphaVantageGatewayClient, AlphaVantageGatewayClientConfig


def test_build_headers_includes_caller_context(monkeypatch):
    monkeypatch.setenv("CONTAINER_APP_JOB_NAME", "bronze-market-job")
    monkeypatch.setenv("CONTAINER_APP_JOB_EXECUTION_NAME", "bronze-market-job-abc123")

    client = AlphaVantageGatewayClient(
        AlphaVantageGatewayClientConfig(
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
