import pandas as pd
import pytest

from core.pipeline import DataPaths
from tasks.price_target_data import silver_price_target_data as silver


def test_write_alpha26_price_target_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = DataPaths.get_silver_price_target_bucket_path("A")
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    def _fake_store(_df: pd.DataFrame, _container: str, _path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _fake_get_schema)
    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_price_target_buckets_writes_empty_bucket_when_schema_exists(monkeypatch):
    target_path = DataPaths.get_silver_price_target_bucket_path("A")
    existing_cols = ["obs_date", "symbol", "tp_mean_est"]
    captured: dict[str, object] = {"store_calls": 0}

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite", **_kwargs) -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 1
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == existing_cols


def test_write_alpha26_price_target_buckets_partial_update_preserves_untouched_symbol_index(monkeypatch):
    captured_index: dict = {}
    captured_paths: list[str] = []

    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame({"symbol": ["AAPL", "MSFT"], "bucket": ["A", "M"]}),
    )
    monkeypatch.setattr(
        silver.layer_bucketing,
        "write_layer_symbol_index",
        lambda **kwargs: captured_index.update(kwargs) or "index",
    )
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["obs_date", "symbol"])
    monkeypatch.setattr(
        silver.delta_core,
        "store_delta",
        lambda _df, _container, path, mode="overwrite", **_kwargs: captured_paths.append(path),
    )

    written_symbols, index_path, _column_count = silver._write_alpha26_price_target_buckets(
        {"A": [pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
        touched_buckets={"A"},
    )

    assert written_symbols == 2
    assert index_path == "index"
    assert captured_paths == [DataPaths.get_silver_price_target_bucket_path("A")]
    assert captured_index["symbol_to_bucket"] == {"AMZN": "A", "MSFT": "M"}


def test_write_alpha26_price_target_buckets_partial_update_fails_closed_without_prior_index(monkeypatch):
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A", "M"))
    monkeypatch.setattr(
        silver.layer_bucketing,
        "load_layer_symbol_index",
        lambda **_kwargs: pd.DataFrame(columns=["symbol", "bucket"]),
    )

    with pytest.raises(RuntimeError, match="incremental alpha26 write blocked"):
        silver._write_alpha26_price_target_buckets(
            {"A": [pd.DataFrame({"obs_date": [pd.Timestamp("2024-01-01")], "symbol": ["AMZN"]})]},
            touched_buckets={"A"},
        )


def test_main_runs_price_target_reconciliation_and_records_metadata(monkeypatch):
    saved_last_success: dict = {}
    reconciliation_calls: list[list[dict]] = []

    def _save_last_success(_name: str, metadata=None):
        if metadata:
            saved_last_success.update(metadata)

    def _run_reconciliation(*, bronze_blob_list):
        reconciliation_calls.append(list(bronze_blob_list))
        return 2, 5

    monkeypatch.setattr(silver, "bronze_client", object())
    monkeypatch.setattr(
        silver.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda _domain, _client: [],
    )
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver, "save_last_success", _save_last_success)
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_run_price_target_reconciliation", _run_reconciliation)
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.mdc, "write_error", lambda *_args, **_kwargs: None)

    assert silver.main() == 0
    assert reconciliation_calls == [[]]
    assert saved_last_success.get("reconciled_orphans") == 2
    assert saved_last_success.get("reconciliation_deleted_blobs") == 5


def test_main_fails_closed_when_price_target_reconciliation_fails(monkeypatch):
    save_last_success_calls = {"count": 0}

    monkeypatch.setattr(silver, "bronze_client", object())
    monkeypatch.setattr(
        silver.bronze_bucketing,
        "list_active_bucket_blob_infos",
        lambda _domain, _client: [],
    )
    monkeypatch.setattr(silver, "load_watermarks", lambda _name: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _name: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda *_args, **_kwargs: save_last_success_calls.__setitem__("count", save_last_success_calls["count"] + 1),
    )
    monkeypatch.setattr(silver.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(
        silver,
        "_run_price_target_reconciliation",
        lambda *, bronze_blob_list: (_ for _ in ()).throw(RuntimeError("reconciliation boom")),
    )
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver.mdc, "write_line", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(silver.mdc, "write_error", lambda *_args, **_kwargs: None)

    assert silver.main() == 1
    assert save_last_success_calls["count"] == 0
