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


def test_finance_regular_folders_are_supported_for_silver(monkeypatch):
    calls = []

    monkeypatch.setattr(
        DataService,
        "_discover_delta_table_paths",
        lambda _container, _prefix: [
            "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
            "finance-data/income_statement/MSFT_quarterly_financials",
        ],
    )

    def fake_load_delta(_container, path):
        calls.append(path)
        symbol = "AAPL" if "AAPL_" in path else "MSFT"
        return pd.DataFrame([{"symbol": symbol, "metric": 123}])

    monkeypatch.setattr(data_service_module.delta_core, "load_delta", fake_load_delta)

    rows = DataService.get_data("silver", "finance", limit=10)

    assert len(rows) == 2
    assert rows[0]["symbol"] == "AAPL"
    assert calls == [
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        "finance-data/income_statement/MSFT_quarterly_financials",
    ]
