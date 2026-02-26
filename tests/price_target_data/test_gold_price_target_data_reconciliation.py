from __future__ import annotations

import pytest

from core.pipeline import DataPaths
from tasks.price_target_data import gold_price_target_data as gold


def test_run_price_target_reconciliation_purges_gold_orphans(monkeypatch):
    class _FakeGoldClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 2

    fake_gold = _FakeGoldClient()

    def _fake_get_storage_client(container: str):
        if container == "silver":
            return object()
        if container == "gold":
            return fake_gold
        return None

    monkeypatch.setattr("core.core.get_storage_client", _fake_get_storage_client)
    monkeypatch.setattr(
        gold,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL"} if root_prefix == "price-target-data" else {"AAPL", "MSFT"},
    )

    orphan_count, deleted_blobs = gold._run_price_target_reconciliation(
        silver_container="silver",
        gold_container="gold",
    )

    assert orphan_count == 1
    assert deleted_blobs == 2
    assert fake_gold.deleted_paths == [DataPaths.get_gold_price_targets_path("MSFT")]


def test_run_price_target_reconciliation_requires_storage_clients(monkeypatch):
    monkeypatch.setattr("core.core.get_storage_client", lambda _container: None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        gold._run_price_target_reconciliation(silver_container="silver", gold_container="gold")
