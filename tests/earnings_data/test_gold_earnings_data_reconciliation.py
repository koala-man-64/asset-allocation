from __future__ import annotations

import pandas as pd
import pytest

from core.pipeline import DataPaths
from tasks.earnings_data import gold_earnings_data as gold


def test_run_earnings_reconciliation_purges_gold_orphans(monkeypatch):
    class _FakeGoldClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 3

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
        lambda *, client, root_prefix: {"AAPL"} if root_prefix == "earnings-data" else {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = gold._run_earnings_reconciliation(
        silver_container="silver",
        gold_container="gold",
    )

    assert orphan_count == 1
    assert deleted_blobs == 3
    assert fake_gold.deleted_paths == [DataPaths.get_gold_earnings_path("MSFT")]


def test_run_earnings_reconciliation_applies_cutoff_sweep(monkeypatch):
    class _FakeGoldClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_gold = _FakeGoldClient()
    captured: dict = {}

    def _fake_get_storage_client(container: str):
        if container == "silver":
            return object()
        if container == "gold":
            return fake_gold
        return None

    monkeypatch.setattr("core.core.get_storage_client", _fake_get_storage_client)
    monkeypatch.setattr(gold, "collect_delta_market_symbols", lambda *, client, root_prefix: {"AAPL", "MSFT"})
    monkeypatch.setattr(
        gold,
        "enforce_backfill_cutoff_on_tables",
        lambda **kwargs: captured.update(kwargs)
        or type(
            "_Stats",
            (),
            {"tables_scanned": 0, "tables_rewritten": 0, "deleted_blobs": 0, "rows_dropped": 0, "errors": 0},
        )(),
    )
    monkeypatch.setattr(gold, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    gold._run_earnings_reconciliation(silver_container="silver", gold_container="gold")

    assert captured["symbols"] == {"AAPL", "MSFT"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


def test_run_earnings_reconciliation_requires_storage_clients(monkeypatch):
    monkeypatch.setattr("core.core.get_storage_client", lambda _container: None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        gold._run_earnings_reconciliation(silver_container="silver", gold_container="gold")
