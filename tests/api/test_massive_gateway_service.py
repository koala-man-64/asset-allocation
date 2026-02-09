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
