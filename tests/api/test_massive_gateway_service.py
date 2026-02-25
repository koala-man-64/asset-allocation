from api.service.massive_gateway import MassiveGateway
from massive_provider.errors import MassiveNotFoundError


def test_daily_time_series_uses_open_close_for_single_day() -> None:
    calls = {"summary": 0, "aggs": 0}

    class _FakeClient:
        def get_daily_ticker_summary(self, *, ticker, date, adjusted=True):
            calls["summary"] += 1
            assert ticker == "AAPL"
            assert date == "2026-02-09"
            assert adjusted is False
            return {
                "symbol": "AAPL",
                "from": "2026-02-09",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1234,
            }

        def list_ohlcv(self, **kwargs):
            calls["aggs"] += 1
            return []

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(
        symbol="AAPL",
        from_date="2026-02-09",
        to_date="2026-02-09",
        adjusted=False,
    )

    lines = csv_text.strip().splitlines()
    assert lines[0] == "Date,Open,High,Low,Close,Volume"
    assert lines[1] == "2026-02-09,10.0,11.0,9.0,10.5,1234.0"
    assert calls["summary"] == 1
    assert calls["aggs"] == 0


def test_daily_time_series_falls_back_to_aggs_when_open_close_not_found() -> None:
    calls = {"summary": 0, "aggs": 0}

    class _FakeClient:
        def get_daily_ticker_summary(self, *, ticker, date, adjusted=True):
            calls["summary"] += 1
            raise MassiveNotFoundError("not found")

        def list_ohlcv(self, **kwargs):
            calls["aggs"] += 1
            return [{"t": 1735776000000, "o": 10.0, "h": 11.0, "l": 9.0, "c": 10.5, "v": 1234}]

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(
        symbol="AAPL",
        from_date="2025-01-02",
        to_date="2025-01-02",
        adjusted=True,
    )

    lines = csv_text.strip().splitlines()
    assert lines[0] == "Date,Open,High,Low,Close,Volume"
    assert lines[1] == "2025-01-02,10.0,11.0,9.0,10.5,1234.0"
    assert calls["summary"] == 1
    assert calls["aggs"] == 1


def test_gateway_fundamentals_request_historical_defaults() -> None:
    class _FakeClient:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, object]]] = []

        def get_short_interest(self, **kwargs):
            self.calls.append(("short_interest", kwargs))
            return {"results": []}

        def get_short_volume(self, **kwargs):
            self.calls.append(("short_volume", kwargs))
            return {"results": []}

        def get_float(self, **kwargs):
            self.calls.append(("float", kwargs))
            return {"results": []}

    fake = _FakeClient()
    gateway = MassiveGateway()
    gateway.get_client = lambda: fake  # type: ignore[method-assign]

    gateway.get_short_interest(symbol="AAPL")
    gateway.get_short_volume(symbol="AAPL")
    gateway.get_float(symbol="AAPL")

    by_name = {name: kwargs for name, kwargs in fake.calls}
    assert by_name["short_interest"]["ticker"] == "AAPL"
    assert by_name["short_interest"]["params"]["sort"] == "settlement_date.asc"
    assert by_name["short_interest"]["params"]["limit"] == 50000
    assert by_name["short_interest"]["pagination"] is True

    assert by_name["short_volume"]["ticker"] == "AAPL"
    assert by_name["short_volume"]["params"]["sort"] == "date.asc"
    assert by_name["short_volume"]["params"]["limit"] == 50000
    assert by_name["short_volume"]["pagination"] is True

    assert by_name["float"]["ticker"] == "AAPL"
    assert by_name["float"]["params"]["sort"] == "effective_date.asc"
    assert by_name["float"]["params"]["limit"] == 5000
    assert by_name["float"]["pagination"] is True


def test_daily_time_series_defaults_to_full_history_window() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def list_ohlcv(self, **kwargs):
            captured.update(kwargs)
            return []

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    csv_text = gateway.get_daily_time_series_csv(symbol="AAPL")
    assert csv_text.splitlines()[0] == "Date,Open,High,Low,Close,Volume"
    assert captured["from_"] == "1970-01-01"
    assert captured["ticker"] == "AAPL"


def test_gateway_unified_snapshot_batches_symbols() -> None:
    captured: dict[str, object] = {}

    class _FakeClient:
        def get_unified_snapshot(self, **kwargs):
            captured.update(kwargs)
            return {"results": [{"ticker": "AAPL"}]}

    gateway = MassiveGateway()
    gateway.get_client = lambda: _FakeClient()  # type: ignore[method-assign]

    payload = gateway.get_unified_snapshot(symbols=["aapl", "MSFT", "AAPL"], asset_type="stocks")
    assert payload["results"][0]["ticker"] == "AAPL"
    assert captured["tickers"] == ["AAPL", "MSFT", "AAPL"]
    assert captured["asset_type"] == "stocks"
    assert captured["limit"] == 250
