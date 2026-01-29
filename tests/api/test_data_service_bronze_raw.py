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
