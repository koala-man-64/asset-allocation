from __future__ import annotations

from io import BytesIO

import pandas as pd
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

    with pytest.raises(ValueError, match="BRONZE_LAYOUT_MODE must be 'alpha26' when set."):
        bronze_bucketing.bronze_layout_mode()
    with pytest.raises(ValueError, match="SILVER_LAYOUT_MODE must be 'alpha26' when set."):
        layer_bucketing.silver_layout_mode()
    with pytest.raises(ValueError, match="GOLD_LAYOUT_MODE must be 'alpha26' when set."):
        layer_bucketing.gold_layout_mode()


def test_layout_modes_default_to_alpha26_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("BRONZE_LAYOUT_MODE", raising=False)
    monkeypatch.delenv("SILVER_LAYOUT_MODE", raising=False)
    monkeypatch.delenv("GOLD_LAYOUT_MODE", raising=False)

    assert bronze_bucketing.bronze_layout_mode() == "alpha26"
    assert layer_bucketing.silver_layout_mode() == "alpha26"
    assert layer_bucketing.gold_layout_mode() == "alpha26"


def test_write_layer_symbol_index_merges_target_sub_domain_only(monkeypatch) -> None:
    saved: dict[str, bytes] = {}

    existing = pd.DataFrame(
        [
            {"symbol": "AAPL", "bucket": "A", "updated_at": "2026-01-01T00:00:00+00:00", "sub_domain": "valuation"},
            {"symbol": "MSFT", "bucket": "M", "updated_at": "2026-01-01T00:00:00+00:00", "sub_domain": "cash_flow"},
        ]
    )
    existing_bytes = existing.to_parquet(index=False)

    monkeypatch.setattr(layer_bucketing.mdc, "common_storage_client", object())
    monkeypatch.setattr(
        layer_bucketing.mdc,
        "read_raw_bytes",
        lambda path, client=None: existing_bytes
        if path == "system/silver-index/finance/latest.parquet"
        else b"",
    )
    monkeypatch.setattr(
        layer_bucketing.mdc,
        "store_raw_bytes",
        lambda payload, path, client=None: saved.setdefault(path, payload),
    )

    out_path = layer_bucketing.write_layer_symbol_index(
        layer="silver",
        domain="finance",
        sub_domain="valuation",
        symbol_to_bucket={"NVDA": "N"},
    )

    assert out_path == "system/silver-index/finance/latest.parquet"
    assert out_path in saved
    written = pd.read_parquet(BytesIO(saved[out_path]))
    assert set(written["symbol"].astype(str)) == {"NVDA", "MSFT"}
    valuation_rows = written[written["sub_domain"].astype(str) == "valuation"]
    cash_flow_rows = written[written["sub_domain"].astype(str) == "cash_flow"]
    assert set(valuation_rows["symbol"].astype(str)) == {"NVDA"}
    assert set(cash_flow_rows["symbol"].astype(str)) == {"MSFT"}
