import logging

import pandas as pd
import pytest

from core import delta_core


def _patch_delta_core_for_unit(monkeypatch, tmp_path):
    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(tmp_path / "table"))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})


def test_store_delta_defaults_merge_schema_to_merge(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    captured = {}

    def fake_write_deltalake(_uri, _df, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    delta_core.store_delta(
        pd.DataFrame({"a": [1]}),
        container="container",
        path="gold/test",
        mode="overwrite",
        merge_schema=True,
    )

    assert captured["schema_mode"] == "merge"


def test_store_delta_schema_mode_overrides_merge_schema(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    captured = {}

    def fake_write_deltalake(_uri, _df, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)

    delta_core.store_delta(
        pd.DataFrame({"a": [1]}),
        container="container",
        path="gold/test",
        mode="overwrite",
        merge_schema=True,
        schema_mode="overwrite",
    )

    assert captured["schema_mode"] == "overwrite"


def test_store_delta_triggers_schema_mismatch_diagnostics(monkeypatch, tmp_path):
    _patch_delta_core_for_unit(monkeypatch, tmp_path)
    called = {"count": 0}

    def fake_write_deltalake(_uri, _df, **_kwargs):
        raise Exception("Cannot cast schema, number of fields does not match: 35 vs 30")

    def fake_log_mismatch(_df, _container, _path):
        called["count"] += 1

    monkeypatch.setattr(delta_core, "write_deltalake", fake_write_deltalake)
    monkeypatch.setattr(delta_core, "_log_delta_schema_mismatch", fake_log_mismatch)

    with pytest.raises(Exception, match="Cannot cast schema"):
        delta_core.store_delta(
            pd.DataFrame({"a": [1]}),
            container="container",
            path="gold/test",
            mode="overwrite",
        )

    assert called["count"] == 1


def test_log_delta_schema_mismatch_emits_missing_extra_and_hint(monkeypatch, caplog):
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: "dummy-uri")
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})
    monkeypatch.setattr(
        delta_core,
        "_get_existing_delta_schema_columns",
        lambda _uri, _storage_options: ["date", "symbol", "drawdown"],
    )

    df = pd.DataFrame(columns=["date", "symbol", "drawdown_1y"])
    logger_name = delta_core.logger.name

    with caplog.at_level(logging.ERROR, logger=logger_name):
        delta_core._log_delta_schema_mismatch(df, container="market-data", path="gold/AAPL")

    assert "missing_in_df=['drawdown']" in caplog.text
    assert "extra_in_df=['drawdown_1y']" in caplog.text
    assert "existing table has 'drawdown' but DataFrame has 'drawdown_1y'" in caplog.text


def test_store_delta_schema_overwrite_migrates_local_table(monkeypatch, tmp_path):
    table_dir = tmp_path / "price_targets_gold"
    monkeypatch.setattr(delta_core, "_ensure_container_exists", lambda _container: None)
    monkeypatch.setattr(delta_core, "get_delta_table_uri", lambda _container, _path: str(table_dir))
    monkeypatch.setattr(delta_core, "get_delta_storage_options", lambda _container=None: {})

    from deltalake import DeltaTable

    df_old = pd.DataFrame(
        {
            "ticker": ["AAPL", "AAPL"],
            "obs_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            "tp_mean_est": [100.0, 101.0],
        }
    )

    delta_core.store_delta(df_old, container="price-targets", path="gold/AAPL", mode="overwrite")
    old_cols = [f.name for f in DeltaTable(str(table_dir)).schema().fields]
    assert "ticker" in old_cols
    assert "symbol" not in old_cols

    df_new = df_old.rename(columns={"ticker": "symbol"})
    delta_core.store_delta(df_new, container="price-targets", path="gold/AAPL", mode="overwrite", schema_mode="overwrite")
    new_cols = [f.name for f in DeltaTable(str(table_dir)).schema().fields]
    assert "symbol" in new_cols
    assert "ticker" not in new_cols
