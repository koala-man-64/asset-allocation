from __future__ import annotations

from collections import Counter
from types import SimpleNamespace
from typing import Any, Dict, List

from fastapi import HTTPException

from api.endpoints import system


class _DummyBlobClient:
    def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
        self.container_name = container_name


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


def test_run_purge_symbol_operation_covers_all_jobs(monkeypatch) -> None:
    blob_paths: List[str] = []
    prefix_paths: List[str] = []

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
    monkeypatch.setattr(system, "_delete_blob_if_exists", lambda client, path: blob_paths.append(path) or 1)
    monkeypatch.setattr(system, "_delete_prefix_if_exists", lambda client, path: prefix_paths.append(path) or 1)

    result = system._run_purge_symbol_operation(system.PurgeSymbolRequest(symbol="AAPL", confirm=True))

    data_targets = [item for item in result["targets"] if item.get("operation") != "blacklist"]
    counts = Counter((item["layer"], item["domain"]) for item in data_targets)
    assert counts == Counter(
        {
            ("bronze", "market"): 1,
            ("bronze", "finance"): 4,
            ("bronze", "earnings"): 1,
            ("bronze", "price-target"): 1,
            ("silver", "market"): 1,
            ("silver", "finance"): 4,
            ("silver", "earnings"): 1,
            ("silver", "price-target"): 1,
            ("gold", "market"): 1,
            ("gold", "finance"): 1,
            ("gold", "earnings"): 1,
            ("gold", "price-target"): 1,
        }
    )

    assert result["totalDeleted"] == 18
    assert len(blob_paths) == 7
    assert len(prefix_paths) == 11

    bronze_finance_paths = sorted(
        item["path"]
        for item in data_targets
        if item["layer"] == "bronze" and item["domain"] == "finance"
    )
    assert bronze_finance_paths == sorted(
        [
            "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json",
            "finance-data/Income Statement/AAPL_quarterly_financials.json",
            "finance-data/Cash Flow/AAPL_quarterly_cash-flow.json",
            "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
        ]
    )


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
