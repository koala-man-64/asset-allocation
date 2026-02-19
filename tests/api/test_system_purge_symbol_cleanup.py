from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

from fastapi import HTTPException

from api.endpoints import system


class _DummyBlobClient:
    def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
        self.container_name = container_name


def test_remove_symbol_from_bronze_storage_covers_all_medallion_domain_folders(monkeypatch) -> None:
    deleted_paths: List[str] = []

    monkeypatch.setattr(
        system,
        "_delete_blob_if_exists",
        lambda client, path: deleted_paths.append(path) or 1,
    )
    monkeypatch.setattr(system.cfg, "EARNINGS_DATA_PREFIX", "earnings-data", raising=False)

    client = SimpleNamespace(container_name="bronze-container")
    outcomes = system._remove_symbol_from_bronze_storage(client, "AAPL")

    assert {str(item["domain"]) for item in outcomes} == {"market", "finance", "earnings", "price-target"}
    assert "market-data/AAPL.csv" in deleted_paths
    assert "earnings-data/AAPL.json" in deleted_paths
    assert "price-target-data/AAPL.parquet" in deleted_paths

    canonical_finance_paths = {
        "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json",
        "finance-data/Income Statement/AAPL_quarterly_financials.json",
        "finance-data/Cash Flow/AAPL_quarterly_cash-flow.json",
        "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
    }
    legacy_finance_paths = {
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet.json",
        "finance-data/income_statement/AAPL_quarterly_financials.json",
        "finance-data/cash_flow/AAPL_quarterly_cash-flow.json",
        "finance-data/valuation/AAPL_quarterly_valuation_measures.json",
    }
    deleted_paths_set = set(deleted_paths)
    assert canonical_finance_paths.issubset(deleted_paths_set)
    assert legacy_finance_paths.issubset(deleted_paths_set)


def test_remove_symbol_from_layer_storage_covers_all_medallion_domain_folders(monkeypatch) -> None:
    deleted_prefixes: List[str] = []

    monkeypatch.setattr(
        system,
        "_delete_prefix_if_exists",
        lambda client, path: deleted_prefixes.append(path) or 1,
    )
    monkeypatch.setattr(system.cfg, "EARNINGS_DATA_PREFIX", "earnings-data", raising=False)

    silver_outcomes = system._remove_symbol_from_layer_storage(
        client=_DummyBlobClient("silver-container"),
        container="silver-container",
        symbol="AAPL",
        layer="silver",
    )
    gold_outcomes = system._remove_symbol_from_layer_storage(
        client=_DummyBlobClient("gold-container"),
        container="gold-container",
        symbol="AAPL",
        layer="gold",
    )

    assert {str(item["domain"]) for item in silver_outcomes} == {"market", "finance", "earnings", "price-target"}
    assert {str(item["domain"]) for item in gold_outcomes} == {"market", "finance", "earnings", "price-target"}

    silver_paths = {str(item["path"]) for item in silver_outcomes}
    assert silver_paths == {
        "market-data/AAPL",
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        "finance-data/income_statement/AAPL_quarterly_financials",
        "finance-data/cash_flow/AAPL_quarterly_cash-flow",
        "finance-data/valuation/AAPL_quarterly_valuation_measures",
        "earnings-data/AAPL",
        "price-target-data/AAPL",
    }

    gold_paths = {str(item["path"]) for item in gold_outcomes}
    assert gold_paths == {
        "market/AAPL",
        "finance/AAPL",
        "earnings/AAPL",
        "targets/AAPL",
    }


def test_run_purge_symbol_operation_returns_regular_targets(monkeypatch) -> None:
    monkeypatch.setattr(system, "BlobStorageClient", _DummyBlobClient)
    monkeypatch.setattr(
        system,
        "_resolve_container",
        lambda layer: {
            "bronze": "bronze-container",
            "silver": "silver-container",
            "gold": "gold-container",
        }[layer],
    )
    monkeypatch.setattr(
        system,
        "_append_symbol_to_bronze_blacklists",
        lambda client, symbol: {"updated": 4, "paths": ["a.csv", "b.csv", "c.csv", "d.csv"]},
    )
    monkeypatch.setattr(
        system,
        "_remove_symbol_from_bronze_storage",
        lambda client, symbol: [{"layer": "bronze", "domain": "market", "deleted": 1}],
    )

    def fake_remove_symbol_from_layer_storage(client, container, symbol, layer):
        if layer == "silver":
            return [
                {"layer": "silver", "domain": "market", "deleted": 2},
                {"layer": "silver", "domain": "finance", "deleted": 0},
            ]
        return [
            {"layer": "gold", "domain": "earnings", "deleted": 1},
            {"layer": "gold", "domain": "price-target", "deleted": 0},
        ]

    monkeypatch.setattr(system, "_remove_symbol_from_layer_storage", fake_remove_symbol_from_layer_storage)

    result = system._run_purge_symbol_operation(system.PurgeSymbolRequest(symbol="AAPL", confirm=True))

    assert result["symbol"] == "AAPL"
    assert result["symbolVariants"] == ["AAPL"]
    assert result["totalDeleted"] == 4
    assert len(result["targets"]) == 6
    assert "affectedByDateTargets" not in result
    assert "byDatePurges" not in result


def test_execute_purge_symbols_operation_tracks_partial_failures(monkeypatch) -> None:
    run_calls: List[str] = []
    update_calls: List[Dict[str, Any]] = []

    def fake_run_purge_symbol_operation(payload):
        run_calls.append(payload.symbol)
        if payload.symbol == "BBB":
            raise HTTPException(status_code=400, detail="bad symbol")
        return {
            "totalDeleted": 3,
            "targets": [{"layer": "silver", "domain": "market", "deleted": 1}],
        }

    def fake_update_purge_operation(operation_id: str, patch: Dict[str, Any]) -> bool:
        update_calls.append({"operationId": operation_id, "patch": patch})
        return True

    monkeypatch.setattr(system, "_run_purge_symbol_operation", fake_run_purge_symbol_operation)
    monkeypatch.setattr(system, "_update_purge_operation", fake_update_purge_operation)

    system._execute_purge_symbols_operation(
        operation_id="op-123",
        symbols=["AAA", "BBB"],
        dry_run=False,
        scope_note="batch",
    )

    assert run_calls == ["AAA", "BBB"]
    assert len(update_calls) == 1
    patch = update_calls[0]["patch"]
    assert patch["status"] == "failed"
    assert patch["error"] == "One or more symbols failed."
    assert patch["result"]["totalDeleted"] == 3
    assert patch["result"]["succeeded"] == 1
    assert patch["result"]["failed"] == 1
    assert "byDatePurges" not in patch["result"]


def test_execute_purge_rule_runs_symbol_purges_without_extra_cleanup(monkeypatch) -> None:
    run_calls: List[str] = []

    monkeypatch.setattr(system, "_collect_rule_symbol_values", lambda rule: [("AAA", 1.0), ("BBB", 2.0)])

    def fake_run_purge_symbol_operation(payload):
        run_calls.append(payload.symbol)
        return {"totalDeleted": 2 if payload.symbol == "AAA" else 5}

    monkeypatch.setattr(system, "_run_purge_symbol_operation", fake_run_purge_symbol_operation)

    rule = SimpleNamespace(id=9, name="test-rule")
    result = system._execute_purge_rule(rule, actor="tester")

    assert run_calls == ["AAA", "BBB"]
    assert result["failedSymbols"] == []
    assert result["purgedCount"] == 7
    assert "byDateTargets" not in result
