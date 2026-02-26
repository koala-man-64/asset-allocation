from __future__ import annotations

from collections import Counter
import json
from types import SimpleNamespace
from typing import Any, Dict, List

from fastapi import HTTPException

from api.endpoints import system


class _DummyBlobClient:
    def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
        self.container_name = container_name


def test_load_symbols_from_bronze_blacklists_merges_and_warns(monkeypatch) -> None:
    monkeypatch.setattr(system.cfg, "EARNINGS_DATA_PREFIX", "earnings-data", raising=False)

    rows_by_path = {
        "market-data/blacklist.csv": ["aaa", "BBB", ""],
        "finance-data/blacklist.csv": ["bbb", " ccc ", "", None],
        "earnings-data/blacklist.csv": ["ddd"],
    }

    def fake_load_ticker_list(path: str, client: Any) -> List[str]:
        if path == "price-target-data/blacklist.csv":
            raise RuntimeError("missing file")
        return rows_by_path.get(path, [])

    monkeypatch.setattr(system.mdc, "load_ticker_list", fake_load_ticker_list)

    payload = system._load_symbols_from_bronze_blacklists(SimpleNamespace(container_name="bronze-container"))

    assert payload["container"] == "bronze-container"
    assert payload["symbolCount"] == 4
    assert payload["symbols"] == ["AAA", "BBB", "CCC", "DDD"]
    assert payload["sources"] == [
        {"path": "market-data/blacklist.csv", "symbolCount": 2},
        {"path": "finance-data/blacklist.csv", "symbolCount": 2},
        {"path": "earnings-data/blacklist.csv", "symbolCount": 1},
        {
            "path": "price-target-data/blacklist.csv",
            "symbolCount": 0,
            "warning": "RuntimeError: missing file",
        },
    ]


def test_get_blacklist_symbols_for_purge_returns_payload(monkeypatch) -> None:
    validate_calls: List[Any] = []
    load_calls: List[Any] = []

    monkeypatch.setattr(system, "validate_auth", lambda request: validate_calls.append(request))
    monkeypatch.setattr(system, "_resolve_container", lambda layer: "bronze-container")

    class _FakeBlobClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobClient)

    def fake_load_symbols_from_bronze_blacklists(client: Any) -> Dict[str, Any]:
        load_calls.append(client)
        return {
            "container": client.container_name,
            "symbolCount": 2,
            "symbols": ["AAA", "BBB"],
            "sources": [{"path": "market-data/blacklist.csv", "symbolCount": 2}],
        }

    monkeypatch.setattr(system, "_load_symbols_from_bronze_blacklists", fake_load_symbols_from_bronze_blacklists)
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-26T00:00:00Z")

    response = system.get_blacklist_symbols_for_purge(SimpleNamespace())

    assert len(validate_calls) == 1
    assert len(load_calls) == 1
    assert load_calls[0].container_name == "bronze-container"
    assert load_calls[0].ensure_container_exists is False
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert json.loads(response.body.decode("utf-8")) == {
        "container": "bronze-container",
        "symbolCount": 2,
        "symbols": ["AAA", "BBB"],
        "sources": [{"path": "market-data/blacklist.csv", "symbolCount": 2}],
        "loadedAt": "2026-02-26T00:00:00Z",
    }


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
            ("bronze", "finance"): 8,
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

    assert result["totalDeleted"] == 22
    assert len(blob_paths) == 11
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
            "finance-data/balance_sheet/AAPL_quarterly_balance-sheet.json",
            "finance-data/income_statement/AAPL_quarterly_financials.json",
            "finance-data/cash_flow/AAPL_quarterly_cash-flow.json",
            "finance-data/valuation/AAPL_quarterly_valuation_measures.json",
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

    monkeypatch.setenv("PURGE_SYMBOL_MAX_WORKERS", "2")
    monkeypatch.setattr(system, "_run_purge_symbol_operation", fake_run_purge_symbol_operation)
    monkeypatch.setattr(system, "_update_purge_operation", fake_update_purge_operation)

    system._execute_purge_symbols_operation(
        operation_id="op-123",
        symbols=["AAA", "BBB"],
        dry_run=False,
        scope_note="batch",
    )

    assert sorted(run_calls) == ["AAA", "BBB"]
    assert len(update_calls) >= 3

    running_patches = [entry["patch"] for entry in update_calls if entry["patch"].get("status") == "running"]
    assert running_patches
    assert any(int(patch["result"]["completed"]) >= 1 for patch in running_patches)

    final_patch = update_calls[-1]["patch"]
    assert final_patch["status"] == "failed"
    assert final_patch["error"] == "One or more symbols failed."
    assert final_patch["result"]["totalDeleted"] == 3
    assert final_patch["result"]["succeeded"] == 1
    assert final_patch["result"]["failed"] == 1
    assert final_patch["result"]["completed"] == 2
    assert final_patch["result"]["pending"] == 0
    assert final_patch["result"]["inProgress"] == 0
    assert "byDatePurges" not in final_patch["result"]


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


def test_resolve_domain_list_paths_for_medallion_domain() -> None:
    assert system._resolve_domain_list_paths("silver", "market") == [
        {"listType": "whitelist", "path": "market-data/whitelist.csv"},
        {"listType": "blacklist", "path": "market-data/blacklist.csv"},
    ]


def test_reset_domain_lists_rewrites_whitelist_and_blacklist(monkeypatch) -> None:
    writes: List[Dict[str, Any]] = []

    class _FakeBlobClient:
        container_name = "silver-container"

        @staticmethod
        def file_exists(path: str) -> bool:
            return path.endswith("whitelist.csv")

    def fake_store_csv(df, path: str, client: Any) -> None:
        writes.append(
            {
                "path": path,
                "columns": list(df.columns),
                "isEmpty": bool(getattr(df, "empty", False)),
            }
        )

    monkeypatch.setattr(system.mdc, "store_csv", fake_store_csv)
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-26T00:00:00Z")

    payload = system._reset_domain_lists(_FakeBlobClient(), layer="silver", domain="market")

    assert payload["layer"] == "silver"
    assert payload["domain"] == "market"
    assert payload["container"] == "silver-container"
    assert payload["resetCount"] == 2
    assert payload["updatedAt"] == "2026-02-26T00:00:00Z"
    assert payload["targets"] == [
        {
            "listType": "whitelist",
            "path": "market-data/whitelist.csv",
            "status": "reset",
            "existed": True,
        },
        {
            "listType": "blacklist",
            "path": "market-data/blacklist.csv",
            "status": "reset",
            "existed": False,
        },
    ]
    assert writes == [
        {"path": "market-data/whitelist.csv", "columns": ["Symbol"], "isEmpty": True},
        {"path": "market-data/blacklist.csv", "columns": ["Symbol"], "isEmpty": True},
    ]


def test_reset_domain_lists_endpoint_validates_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(system, "validate_auth", lambda request: None)

    try:
        system.reset_domain_lists(
            system.DomainListResetRequest(layer="silver", domain="market", confirm=False),
            SimpleNamespace(),
        )
        raise AssertionError("Expected HTTPException for missing confirmation.")
    except HTTPException as exc:
        assert exc.status_code == 400
        assert "Confirmation required" in str(exc.detail)


def test_reset_domain_lists_endpoint_returns_payload(monkeypatch) -> None:
    validate_calls: List[Any] = []
    reset_calls: List[Dict[str, Any]] = []

    monkeypatch.setattr(system, "validate_auth", lambda request: validate_calls.append(request))
    monkeypatch.setattr(system, "_resolve_container", lambda layer: "silver-container")
    monkeypatch.setattr(system, "_get_actor", lambda request: "tester")

    class _FakeBlobClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False) -> None:
            self.container_name = container_name
            self.ensure_container_exists = ensure_container_exists

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobClient)

    def fake_reset(client: Any, *, layer: str, domain: str) -> Dict[str, Any]:
        reset_calls.append({"container": client.container_name, "layer": layer, "domain": domain})
        return {
            "layer": layer,
            "domain": domain,
            "container": client.container_name,
            "resetCount": 2,
            "targets": [
                {"listType": "whitelist", "path": "market-data/whitelist.csv", "status": "reset", "existed": True},
                {"listType": "blacklist", "path": "market-data/blacklist.csv", "status": "reset", "existed": False},
            ],
            "updatedAt": "2026-02-26T00:00:00Z",
        }

    monkeypatch.setattr(system, "_reset_domain_lists", fake_reset)

    response = system.reset_domain_lists(
        system.DomainListResetRequest(layer="silver", domain="market", confirm=True),
        SimpleNamespace(),
    )

    assert len(validate_calls) == 1
    assert reset_calls == [{"container": "silver-container", "layer": "silver", "domain": "market"}]
    assert response.status_code == 200
    assert response.headers.get("Cache-Control") == "no-store"
    assert json.loads(response.body.decode("utf-8")) == {
        "layer": "silver",
        "domain": "market",
        "container": "silver-container",
        "resetCount": 2,
        "targets": [
            {"listType": "whitelist", "path": "market-data/whitelist.csv", "status": "reset", "existed": True},
            {"listType": "blacklist", "path": "market-data/blacklist.csv", "status": "reset", "existed": False},
        ],
        "updatedAt": "2026-02-26T00:00:00Z",
    }
