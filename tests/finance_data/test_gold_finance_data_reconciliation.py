from __future__ import annotations

import pytest

from core.pipeline import DataPaths
from tasks.finance_data import gold_finance_data as gold


def test_run_finance_reconciliation_purges_gold_orphans(monkeypatch):
    class _FakeGoldClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 4

    fake_gold = _FakeGoldClient()

    def _fake_get_storage_client(container: str):
        if container == "silver":
            return object()
        if container == "gold":
            return fake_gold
        return None

    monkeypatch.setattr("core.core.get_storage_client", _fake_get_storage_client)
    monkeypatch.setattr(gold, "collect_delta_silver_finance_symbols", lambda *, client: {"AAPL"})
    monkeypatch.setattr(
        gold,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"} if root_prefix == "finance" else set(),
    )

    orphan_count, deleted_blobs = gold._run_finance_reconciliation(
        silver_container="silver",
        gold_container="gold",
    )

    assert orphan_count == 1
    assert deleted_blobs == 4
    assert fake_gold.deleted_paths == [DataPaths.get_gold_finance_path("MSFT")]


def test_run_finance_reconciliation_requires_storage_clients(monkeypatch):
    monkeypatch.setattr("core.core.get_storage_client", lambda _container: None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        gold._run_finance_reconciliation(silver_container="silver", gold_container="gold")
