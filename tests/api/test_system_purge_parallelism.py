import time

import pandas as pd
import pytest

from api.endpoints import system


def test_resolve_purge_preview_load_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", raising=False)
    assert system._resolve_purge_preview_load_workers(0) == 1
    assert system._resolve_purge_preview_load_workers(3) == 3

    monkeypatch.setenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", "2")
    assert system._resolve_purge_preview_load_workers(10) == 2

    monkeypatch.setenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", "999")
    assert system._resolve_purge_preview_load_workers(100) == system._MAX_PURGE_PREVIEW_LOAD_MAX_WORKERS

    monkeypatch.setenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", "not-a-number")
    assert system._resolve_purge_preview_load_workers(4) == 4


def test_resolve_purge_scope_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PURGE_SCOPE_MAX_WORKERS", raising=False)
    assert system._resolve_purge_scope_workers(0) == 1
    assert system._resolve_purge_scope_workers(3) == 3

    monkeypatch.setenv("PURGE_SCOPE_MAX_WORKERS", "2")
    assert system._resolve_purge_scope_workers(10) == 2

    monkeypatch.setenv("PURGE_SCOPE_MAX_WORKERS", "999")
    assert system._resolve_purge_scope_workers(100) == system._MAX_PURGE_SCOPE_MAX_WORKERS

    monkeypatch.setenv("PURGE_SCOPE_MAX_WORKERS", "invalid")
    assert system._resolve_purge_scope_workers(5) == 5


def test_resolve_purge_symbol_target_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", raising=False)
    assert system._resolve_purge_symbol_target_workers(0) == 1
    assert system._resolve_purge_symbol_target_workers(3) == 3

    monkeypatch.setenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", "2")
    assert system._resolve_purge_symbol_target_workers(10) == 2

    monkeypatch.setenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", "999")
    assert system._resolve_purge_symbol_target_workers(100) == system._MAX_PURGE_SYMBOL_TARGET_MAX_WORKERS

    monkeypatch.setenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", "bad")
    assert system._resolve_purge_symbol_target_workers(4) == 4


def test_resolve_purge_symbol_layer_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", raising=False)
    assert system._resolve_purge_symbol_layer_workers(0) == 1
    assert system._resolve_purge_symbol_layer_workers(3) == 3

    monkeypatch.setenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", "2")
    assert system._resolve_purge_symbol_layer_workers(3) == 2

    monkeypatch.setenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", "999")
    assert system._resolve_purge_symbol_layer_workers(10) == system._MAX_PURGE_SYMBOL_LAYER_MAX_WORKERS

    monkeypatch.setenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", "oops")
    assert system._resolve_purge_symbol_layer_workers(3) == 3


def test_load_rule_frame_parallel_preserves_table_order(monkeypatch: pytest.MonkeyPatch) -> None:
    table_paths = ["market-data/a", "market-data/b", "market-data/c"]
    delay_by_path = {
        "market-data/a": 0.03,
        "market-data/b": 0.01,
        "market-data/c": 0.0,
    }

    monkeypatch.setenv("PURGE_PREVIEW_LOAD_MAX_WORKERS", "3")
    monkeypatch.setattr(system, "_resolve_purge_rule_table", lambda layer, domain: ("silver-container", "market-data/"))
    monkeypatch.setattr(system, "_discover_delta_tables_for_prefix", lambda **kwargs: table_paths)

    def _fake_load_delta(*, container: str, path: str):
        assert container == "silver-container"
        time.sleep(delay_by_path[path])
        return pd.DataFrame([{"source": path, "symbol": "AAA", "value": 1.0}])

    monkeypatch.setattr(system, "load_delta", _fake_load_delta)

    frame = system._load_rule_frame("silver", "market")

    assert list(frame["source"]) == table_paths


def test_run_purge_operation_parallel_preserves_target_order(monkeypatch: pytest.MonkeyPatch) -> None:
    targets = [
        {"layer": "silver", "domain": "market", "container": "c", "prefix": "p1"},
        {"layer": "silver", "domain": "finance", "container": "c", "prefix": "p2"},
        {"layer": "silver", "domain": "earnings", "container": "c", "prefix": "p3"},
    ]

    delay_by_prefix = {"p1": 0.03, "p2": 0.01, "p3": 0.0}
    deleted_by_prefix = {"p1": 1, "p2": 2, "p3": 3}

    monkeypatch.setenv("PURGE_SCOPE_MAX_WORKERS", "3")
    monkeypatch.setattr(system, "_resolve_purge_targets", lambda scope, layer, domain: [dict(t) for t in targets])

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

        def has_blobs(self, prefix):
            return True

        def delete_prefix(self, prefix):
            time.sleep(delay_by_prefix[prefix])
            return deleted_by_prefix[prefix]

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)

    payload = system.PurgeRequest(scope="layer", layer="silver", confirm=True)
    result = system._run_purge_operation(payload)

    assert [entry.get("prefix") for entry in result["targets"]] == ["p1", "p2", "p3"]
    assert [entry.get("deleted") for entry in result["targets"]] == [1, 2, 3]
    assert result["totalDeleted"] == 6


def test_run_purge_symbol_operation_parallel_preserves_layer_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PURGE_SYMBOL_LAYER_MAX_WORKERS", "3")
    monkeypatch.setenv("PURGE_SYMBOL_TARGET_MAX_WORKERS", "1")
    monkeypatch.setattr(system, "_resolve_container", lambda layer: f"{layer}-container")

    class _FakeBlobStorageClient:
        def __init__(self, container_name: str, ensure_container_exists: bool = False):
            self.container_name = container_name

    monkeypatch.setattr(system, "BlobStorageClient", _FakeBlobStorageClient)
    monkeypatch.setattr(system, "_append_symbol_to_bronze_blacklists", lambda client, symbol: {"updated": 0, "paths": []})

    delays = {"bronze": 0.04, "silver": 0.02, "gold": 0.0}

    def _fake_bronze(client, symbol):
        time.sleep(delays["bronze"])
        return [{"layer": "bronze", "domain": "market", "container": client.container_name, "path": "b", "deleted": 1}]

    def _fake_layer(client, container, symbol, layer):
        time.sleep(delays[layer])
        return [{"layer": layer, "domain": "market", "container": container, "path": layer, "deleted": 1}]

    monkeypatch.setattr(system, "_remove_symbol_from_bronze_storage", _fake_bronze)
    monkeypatch.setattr(system, "_remove_symbol_from_layer_storage", _fake_layer)

    payload = system.PurgeSymbolRequest(symbol="AAPL", confirm=True)
    result = system._run_purge_symbol_operation(payload)

    target_layers = [entry.get("layer") for entry in result["targets"] if entry.get("operation") != "blacklist"]
    assert target_layers == ["bronze", "silver", "gold"]
