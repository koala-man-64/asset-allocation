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
