import pandas as pd
import pytest

from tasks.price_target_data import silver_price_target_data as silver
from core.pipeline import DataPaths


def _sample_price_target_frame(date_value: pd.Timestamp) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "obs_date": [date_value],
            "tp_mean_est": [150.0],
            "tp_std_dev_est": [7.5],
            "tp_high_est": [165.0],
            "tp_low_est": [135.0],
            "tp_cnt_est": [10.0],
            "tp_cnt_est_rev_up": [3.0],
            "tp_cnt_est_rev_down": [1.0],
        }
    )


def test_process_blob_migrates_legacy_date_schema_when_blob_is_unchanged(monkeypatch):
    today = pd.Timestamp.today().normalize()
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}
    watermarks = {blob_name: {"etag": "abc123"}}

    legacy_schema = [
        "symbol",
        "Date",
        "tp_mean_est",
        "tp_std_dev_est",
        "tp_high_est",
        "tp_low_est",
        "tp_cnt_est",
        "tp_cnt_est_rev_up",
        "tp_cnt_est_rev_down",
    ]
    legacy_history = _sample_price_target_frame(today).rename(columns={"obs_date": "Date"})
    legacy_history["symbol"] = "AAPL"

    captured: dict = {}

    monkeypatch.setattr(silver, "check_blob_unchanged", lambda _blob, _prior: (True, {"etag": "abc123"}))
    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: _sample_price_target_frame(today))
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: legacy_schema)
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: legacy_history.copy())
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda df, *_args, **_kwargs: captured.setdefault("df", df.copy()),
    )

    status = silver.process_blob(blob, watermarks=watermarks)

    assert status == "ok"
    assert "df" in captured
    assert "obs_date" in captured["df"].columns
    assert "Date" not in captured["df"].columns
    assert "updated_at" in watermarks[blob_name]


def test_process_blob_skips_unchanged_when_schema_is_already_obs_date(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}

    calls = {"read_raw_bytes": 0}

    monkeypatch.setattr(silver, "check_blob_unchanged", lambda _blob, _prior: (True, {"etag": "same"}))
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda *_args, **_kwargs: ["symbol", "obs_date", "tp_mean_est"],
    )

    def fake_read_raw_bytes(*_args, **_kwargs):
        calls["read_raw_bytes"] += 1
        return b"ignored"

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", fake_read_raw_bytes)

    status = silver.process_blob(blob, watermarks={blob_name: {"etag": "same"}})

    assert status == "skipped_unchanged"
    assert calls["read_raw_bytes"] == 0


def test_process_blob_applies_backfill_start_cutoff(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}

    source = pd.DataFrame(
        {
            "obs_date": [pd.Timestamp("2023-12-31"), pd.Timestamp("2024-01-02")],
            "tp_mean_est": [140.0, 150.0],
            "tp_std_dev_est": [7.0, 7.5],
            "tp_high_est": [155.0, 165.0],
            "tp_low_est": [125.0, 135.0],
            "tp_cnt_est": [9.0, 10.0],
            "tp_cnt_est_rev_up": [2.0, 3.0],
            "tp_cnt_est_rev_down": [1.0, 1.0],
        }
    )
    history = source.copy()
    history["obs_date"] = [pd.Timestamp("2023-12-30"), pd.Timestamp("2024-01-01")]
    history["symbol"] = "AAPL"

    captured: dict = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: source.copy())
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: history.copy())
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (pd.Timestamp("2024-01-01"), None))
    monkeypatch.setattr(silver.delta_core, "vacuum_delta_table", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda df, *_args, **_kwargs: captured.setdefault("df", df.copy()),
    )

    status = silver.process_blob(blob, watermarks={})

    assert status == "ok"
    assert "df" in captured
    assert pd.to_datetime(captured["df"]["obs_date"]).min().date().isoformat() >= "2024-01-01"


def test_process_blob_applies_price_target_precision_policy(monkeypatch):
    blob_name = "price-target-data/AAPL.parquet"
    blob = {"name": blob_name}
    today = pd.Timestamp.today().normalize()
    source = pd.DataFrame(
        {
            "obs_date": [today],
            "tp_mean_est": [100.005],
            "tp_std_dev_est": [1.23445],
            "tp_high_est": [120.005],
            "tp_low_est": [80.005],
            "tp_cnt_est": [10.125],
            "tp_cnt_est_rev_up": [2.0],
            "tp_cnt_est_rev_down": [1.0],
        }
    )

    captured: dict = {}

    monkeypatch.setattr(silver.mdc, "read_raw_bytes", lambda *_args, **_kwargs: b"ignored")
    monkeypatch.setattr(silver.pd, "read_parquet", lambda *_args, **_kwargs: source.copy())
    monkeypatch.setattr(silver.delta_core, "load_delta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda df, *_args, **_kwargs: captured.setdefault("df", df.copy()),
    )

    status = silver.process_blob(blob, watermarks={})

    assert status == "ok"
    row = captured["df"].iloc[0]
    assert row["tp_mean_est"] == pytest.approx(100.01)
    assert row["tp_high_est"] == pytest.approx(120.01)
    assert row["tp_low_est"] == pytest.approx(80.01)
    assert row["tp_std_dev_est"] == pytest.approx(1.2345)
    assert row["tp_cnt_est"] == pytest.approx(10.125)


def test_run_price_target_reconciliation_purges_silver_orphans(monkeypatch):
    class _FakeSilverClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 3

    fake_client = _FakeSilverClient()
    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(
        silver,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = silver._run_price_target_reconciliation(
        bronze_blob_list=[
            {"name": "price-target-data/AAPL.parquet"},
            {"name": "price-target-data/not_used.json"},
        ]
    )

    assert orphan_count == 1
    assert deleted_blobs == 3
    assert fake_client.deleted_paths == [DataPaths.get_price_target_path("MSFT")]


def test_run_price_target_reconciliation_applies_cutoff_sweep(monkeypatch):
    class _FakeSilverClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_client = _FakeSilverClient()
    captured: dict = {}

    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(
        silver,
        "collect_delta_market_symbols",
        lambda *, client, root_prefix: {"AAPL", "MSFT"},
    )
    monkeypatch.setattr(
        silver,
        "enforce_backfill_cutoff_on_tables",
        lambda **kwargs: captured.update(kwargs)
        or type(
            "_Stats",
            (),
            {"tables_scanned": 0, "tables_rewritten": 0, "deleted_blobs": 0, "rows_dropped": 0, "errors": 0},
        )(),
    )
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (pd.Timestamp("2016-01-01"), None))

    silver._run_price_target_reconciliation(bronze_blob_list=[{"name": "price-target-data/AAPL.parquet"}])

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


def test_run_price_target_reconciliation_requires_storage_client(monkeypatch):
    monkeypatch.setattr(silver, "silver_client", None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        silver._run_price_target_reconciliation(bronze_blob_list=[])
