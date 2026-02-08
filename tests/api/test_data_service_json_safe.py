import pandas as pd

import api.data_service as data_service_module
from api.data_service import DataService


def test_delta_nan_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(
        data_service_module.delta_core,
        "load_delta",
        lambda _container, _path: pd.DataFrame([{"symbol": "AAPL", "eps": 1.23}, {"symbol": "AAPL", "eps": float("nan")}]),
    )

    rows = DataService.get_data("silver", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[1]["eps"] is None


def test_delta_inf_values_are_json_safe(monkeypatch):
    monkeypatch.setattr(
        data_service_module.delta_core,
        "load_delta",
        lambda _container, _path: pd.DataFrame(
            [{"symbol": "AAPL", "eps": float("inf")}, {"symbol": "AAPL", "eps": float("-inf")}]
        ),
    )

    rows = DataService.get_data("silver", "earnings", ticker="AAPL", limit=2)

    assert len(rows) == 2
    assert rows[0]["eps"] is None
    assert rows[1]["eps"] is None


def test_finance_by_date_is_supported_for_silver(monkeypatch):
    calls = []

    def fake_load_delta(_container, path):
        calls.append(path)
        return pd.DataFrame([{"symbol": "AAPL", "metric": 123}])

    monkeypatch.setattr(data_service_module.delta_core, "load_delta", fake_load_delta)

    rows = DataService.get_data("silver", "finance", limit=1)

    assert len(rows) == 1
    assert rows[0]["symbol"] == "AAPL"
    assert calls == ["finance-data-by-date"]
