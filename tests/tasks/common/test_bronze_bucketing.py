from __future__ import annotations

import pytest

from tasks.common import bronze_bucketing
from tasks.common import layer_bucketing


def test_bucket_letter_mapping_examples() -> None:
    assert bronze_bucketing.bucket_letter("AAPL") == "A"
    assert bronze_bucketing.bucket_letter("brk.b") == "B"
    assert bronze_bucketing.bucket_letter("^VIX") == "V"
    assert bronze_bucketing.bucket_letter("1INCH") == "I"
    assert bronze_bucketing.bucket_letter("$$$") == "X"


def test_all_bucket_blob_paths_returns_26_alpha_files() -> None:
    paths = bronze_bucketing.all_bucket_blob_paths("market-data")
    assert len(paths) == 26
    assert paths[0] == "market-data/buckets/A.parquet"
    assert paths[-1] == "market-data/buckets/Z.parquet"


def test_layout_modes_fail_fast_when_not_alpha26(monkeypatch) -> None:
    monkeypatch.setenv("BRONZE_LAYOUT_MODE", "legacy")
    monkeypatch.setenv("SILVER_LAYOUT_MODE", "legacy")
    monkeypatch.setenv("GOLD_LAYOUT_MODE", "legacy")

    with pytest.raises(ValueError, match="BRONZE_LAYOUT_MODE must be 'alpha26'"):
        bronze_bucketing.bronze_layout_mode()
    with pytest.raises(ValueError, match="SILVER_LAYOUT_MODE must be 'alpha26'"):
        layer_bucketing.silver_layout_mode()
    with pytest.raises(ValueError, match="GOLD_LAYOUT_MODE must be 'alpha26'"):
        layer_bucketing.gold_layout_mode()
