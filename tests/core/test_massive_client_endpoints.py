import httpx

from massive_provider import MassiveClient, MassiveConfig


def _build_client(handler, *, float_endpoint: str = "/stocks/vX/float") -> MassiveClient:
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(
        transport=transport,
        base_url="https://api.massive.com",
        headers={"Authorization": "Bearer test-key"},
    )
    cfg = MassiveConfig(
        api_key="test-key",
        base_url="https://api.massive.com",
        timeout_seconds=10.0,
        prefer_official_sdk=False,
        float_endpoint=float_endpoint,
    )
    return MassiveClient(cfg, http_client=http_client)


def test_massive_client_paths_align_with_docs() -> None:
    seen: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        if request.url.path.startswith("/v2/aggs/ticker/"):
            return httpx.Response(200, json={"results": []})
        return httpx.Response(200, json={"status": "OK", "results": []})

    client = _build_client(handler)
    try:
        client.get_daily_ticker_summary(ticker="AAPL", date="2026-02-09", adjusted=False)
        client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2026-02-01",
            to="2026-02-09",
            adjusted=True,
            sort="asc",
            limit=50000,
            pagination=False,
        )
        client.get_short_interest(ticker="AAPL")
        client.get_short_volume(ticker="AAPL")
        client.get_float(ticker="AAPL")
        client.get_income_statement(ticker="AAPL")
        client.get_cash_flow_statement(ticker="AAPL")
        client.get_balance_sheet(ticker="AAPL")
        client.get_ratios(ticker="AAPL")
    finally:
        client.close()

    assert seen[0][0] == "/v1/open-close/AAPL/2026-02-09"
    assert seen[0][1].get("adjusted") == "false"

    assert seen[1][0] == "/v2/aggs/ticker/AAPL/range/1/day/2026-02-01/2026-02-09"
    assert seen[1][1].get("adjusted") == "true"
    assert seen[1][1].get("sort") == "asc"
    assert seen[1][1].get("limit") == "50000"

    assert seen[2][0] == "/stocks/v1/short-interest"
    assert seen[3][0] == "/stocks/v1/short-volume"
    assert seen[4][0] == "/stocks/vX/float"
    assert seen[5][0] == "/stocks/financials/v1/income-statements"
    assert seen[6][0] == "/stocks/financials/v1/cash-flow-statements"
    assert seen[7][0] == "/stocks/financials/v1/balance-sheets"
    assert seen[8][0] == "/stocks/financials/v1/ratios"


def test_massive_client_float_endpoint_can_be_overridden() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"status": "OK", "results": []})

    client = _build_client(handler, float_endpoint="/stocks/v1/float")
    try:
        client.get_float(ticker="AAPL")
    finally:
        client.close()

    assert seen == ["/stocks/v1/float"]


def test_list_ohlcv_passes_adjusted_and_sort_to_sdk_when_supported(monkeypatch) -> None:
    import massive_provider.client as client_module

    class _FakeSDK:
        def __init__(self, *args, **kwargs) -> None:
            self.calls: list[dict[str, object]] = []

        def list_aggs(self, **kwargs):
            self.calls.append(dict(kwargs))
            return []

        def close(self) -> None:
            return None

    monkeypatch.setattr(client_module, "_SDKRestClient", _FakeSDK)

    cfg = MassiveConfig(
        api_key="test-key",
        base_url="https://api.massive.com",
        timeout_seconds=10.0,
        prefer_official_sdk=True,
    )
    client = MassiveClient(cfg)
    try:
        client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2026-02-01",
            to="2026-02-09",
            adjusted=False,
            sort="desc",
            limit=100,
            pagination=False,
        )
        assert client._sdk is not None
        assert client._sdk.calls[0]["adjusted"] is False
        assert client._sdk.calls[0]["sort"] == "desc"
    finally:
        client.close()


def test_list_ohlcv_falls_back_for_older_sdk_without_adjusted_sort(monkeypatch) -> None:
    import massive_provider.client as client_module

    class _LegacySDK:
        def __init__(self, *args, **kwargs) -> None:
            self.calls: list[dict[str, object]] = []

        def list_aggs(self, ticker, multiplier, timespan, from_, to, limit):
            self.calls.append(
                {
                    "ticker": ticker,
                    "multiplier": multiplier,
                    "timespan": timespan,
                    "from_": from_,
                    "to": to,
                    "limit": limit,
                }
            )
            return []

        def close(self) -> None:
            return None

    monkeypatch.setattr(client_module, "_SDKRestClient", _LegacySDK)

    cfg = MassiveConfig(
        api_key="test-key",
        base_url="https://api.massive.com",
        timeout_seconds=10.0,
        prefer_official_sdk=True,
    )
    client = MassiveClient(cfg)
    try:
        out = client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="2026-02-01",
            to="2026-02-09",
            adjusted=False,
            sort="desc",
            limit=100,
            pagination=False,
        )
        assert out == []
        assert client._sdk is not None
        assert client._sdk.calls[0]["ticker"] == "AAPL"
    finally:
        client.close()
