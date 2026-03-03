from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import pytest

from core.pipeline import DataPaths
from tasks.earnings_data import gold_earnings_data as gold_earnings
from tasks.earnings_data import silver_earnings_data as silver_earnings
from tasks.finance_data import gold_finance_data as gold_finance
from tasks.finance_data import silver_finance_data as silver_finance
from tasks.market_data import gold_market_data as gold_market
from tasks.market_data import silver_market_data as silver_market
from tasks.price_target_data import gold_price_target_data as gold_price_target
from tasks.price_target_data import silver_price_target_data as silver_price_target


def _empty_cutoff_stats() -> Any:
    return type(
        "_Stats",
        (),
        {
            "tables_scanned": 0,
            "tables_rewritten": 0,
            "deleted_blobs": 0,
            "rows_dropped": 0,
            "errors": 0,
        },
    )()


def _patch_gold_market_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gold_market,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL"} if root_prefix == "market-data" else {"AAPL", "MSFT"},
    )


def _patch_gold_earnings_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gold_earnings,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL"} if root_prefix == "earnings-data" else {"AAPL", "MSFT"},
    )


def _patch_gold_price_target_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        gold_price_target,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL"} if root_prefix == "price-target-data" else {"AAPL", "MSFT"},
    )


def _patch_gold_finance_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(gold_finance, "collect_delta_silver_finance_symbols", lambda *, client: {"AAPL"})
    monkeypatch.setattr(gold_finance, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL", "MSFT"})


def _patch_silver_market_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(silver_market, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL", "MSFT"})
    monkeypatch.setattr(silver_market, "collect_bronze_market_symbols_from_blob_infos", lambda _blob_infos: {"AAPL"})


def _patch_silver_earnings_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        silver_earnings,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(
        silver_earnings,
        "collect_bronze_earnings_symbols_from_blob_infos",
        lambda _blob_infos: {"AAPL"},
    )


def _patch_silver_price_target_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        silver_price_target,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(
        silver_price_target,
        "collect_bronze_price_target_symbols_from_blob_infos",
        lambda _blob_infos: {"AAPL"},
    )


def _patch_silver_finance_symbols(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(silver_finance, "collect_delta_silver_finance_symbols", lambda *, client: {"AAPL", "MSFT"})
    monkeypatch.setattr(
        silver_finance,
        "collect_bronze_finance_symbols_from_blob_infos",
        lambda _blob_infos: {"AAPL"},
    )


GOLD_CASES: list[dict[str, Any]] = [
    {
        "id": "market",
        "module": gold_market,
        "run_name": "_run_market_reconciliation",
        "patch_symbols": _patch_gold_market_symbols,
        "deleted_paths": [DataPaths.get_gold_features_path("MSFT")],
    },
    {
        "id": "finance",
        "module": gold_finance,
        "run_name": "_run_finance_reconciliation",
        "patch_symbols": _patch_gold_finance_symbols,
        "deleted_paths": [DataPaths.get_gold_finance_path("MSFT")],
    },
    {
        "id": "earnings",
        "module": gold_earnings,
        "run_name": "_run_earnings_reconciliation",
        "patch_symbols": _patch_gold_earnings_symbols,
        "deleted_paths": [DataPaths.get_gold_earnings_path("MSFT")],
    },
    {
        "id": "price-target",
        "module": gold_price_target,
        "run_name": "_run_price_target_reconciliation",
        "patch_symbols": _patch_gold_price_target_symbols,
        "deleted_paths": [DataPaths.get_gold_price_targets_path("MSFT")],
    },
]


SILVER_CASES: list[dict[str, Any]] = [
    {
        "id": "market",
        "module": silver_market,
        "run_name": "_run_market_reconciliation",
        "patch_symbols": _patch_silver_market_symbols,
        "deleted_paths": [DataPaths.get_market_data_path("MSFT")],
        "bronze_blob_list": [{"name": "market-data/AAPL.csv"}],
    },
    {
        "id": "finance",
        "module": silver_finance,
        "run_name": "_run_finance_reconciliation",
        "patch_symbols": _patch_silver_finance_symbols,
        "deleted_paths": [
            DataPaths.get_finance_path("balance_sheet", "MSFT", "quarterly_balance-sheet"),
            DataPaths.get_finance_path("income_statement", "MSFT", "quarterly_financials"),
            DataPaths.get_finance_path("cash_flow", "MSFT", "quarterly_cash-flow"),
            DataPaths.get_finance_path("valuation", "MSFT", "quarterly_valuation_measures"),
        ],
        "bronze_blob_list": [{"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"}],
    },
    {
        "id": "earnings",
        "module": silver_earnings,
        "run_name": "_run_earnings_reconciliation",
        "patch_symbols": _patch_silver_earnings_symbols,
        "deleted_paths": [DataPaths.get_earnings_path("MSFT")],
        "bronze_blob_list": [{"name": "earnings-data/AAPL.json"}],
    },
    {
        "id": "price-target",
        "module": silver_price_target,
        "run_name": "_run_price_target_reconciliation",
        "patch_symbols": _patch_silver_price_target_symbols,
        "deleted_paths": [DataPaths.get_price_target_path("MSFT")],
        "bronze_blob_list": [{"name": "price-target-data/AAPL.parquet"}],
    },
]


def _patch_gold_storage_clients(monkeypatch: pytest.MonkeyPatch, fake_gold: Any) -> None:
    def _fake_get_storage_client(container: str) -> Any:
        if container == "silver":
            return object()
        if container == "gold":
            return fake_gold
        return None

    monkeypatch.setattr("core.core.get_storage_client", _fake_get_storage_client)


@pytest.mark.parametrize("case", GOLD_CASES, ids=[c["id"] for c in GOLD_CASES])
def test_gold_reconciliation_contract_purges_orphans(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    patch_symbols: Callable[[pytest.MonkeyPatch], None] = case["patch_symbols"]

    class _FakeGoldClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 1

    fake_gold = _FakeGoldClient()
    _patch_gold_storage_clients(monkeypatch, fake_gold)
    patch_symbols(monkeypatch)
    monkeypatch.setattr(module, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = run_fn(silver_container="silver", gold_container="gold")

    assert orphan_count == 1
    assert deleted_blobs == len(case["deleted_paths"])
    assert fake_gold.deleted_paths == case["deleted_paths"]


@pytest.mark.parametrize("case", GOLD_CASES, ids=[c["id"] for c in GOLD_CASES])
def test_gold_reconciliation_contract_applies_cutoff_sweep(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    patch_symbols: Callable[[pytest.MonkeyPatch], None] = case["patch_symbols"]
    captured: dict[str, Any] = {}

    class _FakeGoldClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    _patch_gold_storage_clients(monkeypatch, _FakeGoldClient())
    patch_symbols(monkeypatch)
    monkeypatch.setattr(
        module,
        "enforce_backfill_cutoff_on_tables",
        lambda **kwargs: captured.update(kwargs) or _empty_cutoff_stats(),
    )
    monkeypatch.setattr(module, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    run_fn(silver_container="silver", gold_container="gold")

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


@pytest.mark.parametrize("case", GOLD_CASES, ids=[c["id"] for c in GOLD_CASES])
def test_gold_reconciliation_contract_requires_silver_storage_client(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    monkeypatch.setattr("core.core.get_storage_client", lambda _container: None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        run_fn(silver_container="silver", gold_container="gold")


@pytest.mark.parametrize("case", SILVER_CASES, ids=[c["id"] for c in SILVER_CASES])
def test_silver_reconciliation_contract_purges_orphans(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    patch_symbols: Callable[[pytest.MonkeyPatch], None] = case["patch_symbols"]

    class _FakeSilverClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 1

    fake_silver = _FakeSilverClient()
    monkeypatch.setattr(module, "silver_client", fake_silver)
    patch_symbols(monkeypatch)
    monkeypatch.setattr(module, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = run_fn(bronze_blob_list=case["bronze_blob_list"])

    assert orphan_count == 1
    assert deleted_blobs == len(case["deleted_paths"])
    assert fake_silver.deleted_paths == case["deleted_paths"]


@pytest.mark.parametrize("case", SILVER_CASES, ids=[c["id"] for c in SILVER_CASES])
def test_silver_reconciliation_contract_applies_cutoff_sweep(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    patch_symbols: Callable[[pytest.MonkeyPatch], None] = case["patch_symbols"]
    captured: dict[str, Any] = {}

    class _FakeSilverClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    monkeypatch.setattr(module, "silver_client", _FakeSilverClient())
    patch_symbols(monkeypatch)
    monkeypatch.setattr(
        module,
        "enforce_backfill_cutoff_on_tables",
        lambda **kwargs: captured.update(kwargs) or _empty_cutoff_stats(),
    )
    monkeypatch.setattr(module, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    run_fn(bronze_blob_list=case["bronze_blob_list"])

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


@pytest.mark.parametrize("case", SILVER_CASES, ids=[c["id"] for c in SILVER_CASES])
def test_silver_reconciliation_contract_requires_storage_client(
    case: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = case["module"]
    run_fn = getattr(module, case["run_name"])
    monkeypatch.setattr(module, "silver_client", None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        run_fn(bronze_blob_list=case["bronze_blob_list"])
