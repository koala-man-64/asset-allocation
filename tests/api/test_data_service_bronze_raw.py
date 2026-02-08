import api.data_service as data_service_module
from api.data_service import DataService


def test_bronze_market_reads_raw_csv(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: b"Date,Open,Close\n2025-01-01,1,2\n2025-01-02,3,4\n",
    )

    rows = DataService.get_data("bronze", "market", ticker="AAPL", limit=1)

    assert len(rows) == 1
    assert rows[0]["Date"] == "2025-01-01"


def test_bronze_earnings_reads_raw_json(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: b'[{"symbol":"AAPL","eps":1.23},{"symbol":"AAPL","eps":2.34}]',
    )

    rows = DataService.get_data("bronze", "earnings", ticker="AAPL", limit=1)

    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"


def test_bronze_earnings_missing_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(
        data_service_module.mdc,
        "read_raw_bytes",
        lambda _path, client=None: b'[{"symbol":"AAPL","eps":1.23},{"symbol":"AAPL"}]',
    )

    rows = DataService.get_data("bronze", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[1]["eps"] is None


def test_bronze_market_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    class StubClient:
        def list_files(self, name_starts_with=None):
            assert name_starts_with == "market-data/"
            return ["market-data/MSFT.csv", "market-data/AAPL.csv"]

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: StubClient())

    def fake_read_raw_bytes(path, client=None):
        assert path == "market-data/AAPL.csv"
        return b"Date,Open,Close\n2025-01-01,1,2\n"

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_data("bronze", "market", limit=1)

    assert len(rows) == 1
    assert rows[0]["Date"] == "2025-01-01"


def test_bronze_finance_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    class StubClient:
        def list_files(self, name_starts_with=None):
            assert name_starts_with == "finance-data/Valuation/"
            return [
                "finance-data/Valuation/MSFT_quarterly_valuation_measures.csv",
                "finance-data/Valuation/AAPL_quarterly_valuation_measures.csv",
            ]

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: StubClient())

    def fake_read_raw_bytes(path, client=None):
        assert path == "finance-data/Valuation/AAPL_quarterly_valuation_measures.csv"
        return b"Date,metric\n2025-01-01,123\n"

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_finance_data("bronze", "valuation", ticker=None, limit=1)

    assert len(rows) == 1
    assert rows[0]["metric"] == 123


def test_bronze_generic_finance_defaults_to_first_blob_when_ticker_missing(monkeypatch):
    class StubClient:
        def list_files(self, name_starts_with=None):
            assert name_starts_with == "finance-data/"
            return [
                "finance-data/valuation/MSFT_quarterly_valuation_measures.csv",
                "finance-data/valuation/AAPL_quarterly_valuation_measures.csv",
            ]

    monkeypatch.setattr(data_service_module.mdc, "get_storage_client", lambda _container: StubClient())

    def fake_read_raw_bytes(path, client=None):
        assert path == "finance-data/valuation/AAPL_quarterly_valuation_measures.csv"
        return b"Date,metric\n2025-01-01,123\n"

    monkeypatch.setattr(data_service_module.mdc, "read_raw_bytes", fake_read_raw_bytes)

    rows = DataService.get_data("bronze", "finance", ticker=None, limit=1)

    assert len(rows) == 1
    assert rows[0]["metric"] == 123
