import pandas as pd
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

from tasks.finance_data import silver_finance_data as silver
from core.pipeline import DataPaths


def test_silver_finance_processes_alpha_vantage_json_quarterly_reports():
    blob_name = "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.json"
    payload = {
        "symbol": "TEST",
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-01-01",
                "totalAssets": "1000",
                "totalLiabilities": "500",
            }
        ],
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    with (
        patch("core.core.read_raw_bytes") as mock_read,
        patch("core.delta_core.store_delta") as mock_store,
        patch("core.delta_core.get_delta_schema_columns", return_value=None),
    ):
        mock_read.return_value = raw_bytes

        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-01"), watermarks={})
        assert result.status == "ok"

        mock_store.assert_called_once()
        df = mock_store.call_args.args[0]
        assert "date" in df.columns
        assert "symbol" in df.columns
        assert "total_assets" in df.columns
        assert df.iloc[-1]["symbol"] == "TEST"


def test_silver_finance_applies_backfill_start_cutoff():
    blob_name = "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.json"
    payload = {
        "symbol": "TEST",
        "quarterlyReports": [
            {"fiscalDateEnding": "2023-12-31", "totalAssets": "900"},
            {"fiscalDateEnding": "2024-01-01", "totalAssets": "1000"},
        ],
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    with (
        patch("core.core.read_raw_bytes", return_value=raw_bytes),
        patch("core.delta_core.store_delta") as mock_store,
        patch("core.delta_core.get_delta_schema_columns", return_value=None),
        patch("core.delta_core.vacuum_delta_table", return_value=0),
    ):
        result = silver.process_blob(
            {"name": blob_name},
            desired_end=pd.Timestamp("2024-01-02"),
            backfill_start=pd.Timestamp("2024-01-01"),
            watermarks={},
        )
        assert result.status == "ok"

        df = mock_store.call_args.args[0]
        assert df["date"].min().date().isoformat() >= "2024-01-01"


def test_silver_finance_process_path_sanitizes_index_artifacts_before_delta_write(monkeypatch, tmp_path):
    blob_name = "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.json"
    payload = {
        "symbol": "TEST",
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-01-01",
                "totalAssets": "1000",
                "totalLiabilities": "500",
            }
        ],
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    def _inject_artifacts(df: pd.DataFrame, *_args, **_kwargs) -> pd.DataFrame:
        out = df.copy()
        out["__index_level_0__"] = 42
        out.index = pd.Index([7] * len(out))
        return out

    captured: dict[str, object] = {}

    def fake_write_deltalake(_uri, df, **kwargs):
        captured["df"] = df.copy()
        captured["kwargs"] = dict(kwargs)

    with (
        patch("core.core.read_raw_bytes", return_value=raw_bytes),
        patch(
            "tasks.finance_data.silver_finance_data._align_to_existing_schema",
            side_effect=_inject_artifacts,
        ),
        patch("core.delta_core._ensure_container_exists", return_value=None),
        patch("core.delta_core.get_delta_table_uri", return_value=str(tmp_path / "silver_finance")),
        patch("core.delta_core.get_delta_storage_options", return_value={}),
        patch("core.delta_core._get_existing_delta_schema_columns", return_value=None),
        patch("core.delta_core.write_deltalake", side_effect=fake_write_deltalake),
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-01"), watermarks={})
        assert result.status == "ok"

    df_written = captured["df"]
    assert "__index_level_0__" not in df_written.columns
    assert isinstance(df_written.index, pd.RangeIndex)
    assert df_written.index.start == 0
    assert df_written.index.step == 1


def test_silver_finance_repairs_legacy_symbol_suffix_columns_before_delta_write():
    blob_name = "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.json"
    payload = {
        "symbol": "TEST",
        "quarterlyReports": [
            {"fiscalDateEnding": "2024-01-01", "totalAssets": "1000"},
        ],
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    def _inject_legacy_symbol_column(df: pd.DataFrame, container: str, path: str) -> pd.DataFrame:
        assert container == silver.cfg.AZURE_CONTAINER_SILVER
        assert path == DataPaths.get_finance_path("Balance Sheet", "TEST", "quarterly_balance-sheet")
        out = df.copy()
        out["symbol_2"] = "TEST"
        return out

    with (
        patch("core.core.read_raw_bytes", return_value=raw_bytes),
        patch("core.delta_core.store_delta") as mock_store,
        patch(
            "tasks.finance_data.silver_finance_data._align_to_existing_schema",
            side_effect=_inject_legacy_symbol_column,
        ),
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-01"), watermarks={})
        assert result.status == "ok"

        df_saved = mock_store.call_args.args[0]
        assert "symbol_2" not in df_saved.columns
        assert "symbol" in df_saved.columns
        assert set(df_saved["symbol"].dropna().astype(str).unique()) == {"TEST"}


def test_silver_finance_builds_valuation_timeseries_from_overview_and_prices():
    blob_name = "finance-data/Valuation/TEST_quarterly_valuation_measures.json"
    payload = {
        "Symbol": "TEST",
        "MarketCapitalization": "1000",
        "PERatio": "10",
        "ForwardPE": "12",
        "EVToEBITDA": "8",
        "EVToRevenue": "4",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    df_prices = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "close": [50.0, 100.0],
        }
    )

    with (
        patch("core.core.read_raw_bytes", return_value=raw_bytes),
        patch("core.delta_core.load_delta", return_value=df_prices),
        patch("core.delta_core.store_delta") as mock_store,
        patch("core.delta_core.get_delta_schema_columns", return_value=None),
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-02"), watermarks={})
        assert result.status == "ok"

        mock_store.assert_called_once()
        df = mock_store.call_args.args[0].sort_values("date").reset_index(drop=True)
        assert df["date"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-01", "2024-01-02"]

        assert df.loc[0, "market_cap"] == pytest.approx(500.0)
        assert df.loc[1, "market_cap"] == pytest.approx(1000.0)
        assert df.loc[0, "pe_ratio"] == pytest.approx(5.0)
        assert df.loc[1, "pe_ratio"] == pytest.approx(10.0)
        assert df.loc[0, "forward_pe"] == pytest.approx(6.0)
        assert df.loc[1, "forward_pe"] == pytest.approx(12.0)
        assert df.loc[0, "ev_ebitda"] == pytest.approx(4.0)
        assert df.loc[1, "ev_ebitda"] == pytest.approx(8.0)
        assert df.loc[0, "ev_revenue"] == pytest.approx(2.0)
        assert df.loc[1, "ev_revenue"] == pytest.approx(4.0)

        assert df.loc[0, "shares_outstanding"] == pytest.approx(10.0)
        assert df.loc[1, "shares_outstanding"] == pytest.approx(10.0)


def test_silver_finance_rounds_valuation_calculated_columns_to_four_decimals():
    blob_name = "finance-data/Valuation/TEST_quarterly_valuation_measures.json"
    valuation_frame = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2024-01-01")],
            "Symbol": ["TEST"],
            "market_cap": [100.12345],
            "pe_ratio": [1.23445],
            "forward_pe": [2.34555],
            "ev_ebitda": [3.45665],
            "ev_revenue": [4.56775],
            "shares_outstanding": [5.67885],
        }
    )

    with (
        patch("core.core.read_raw_bytes", return_value=b"{}"),
        patch("tasks.finance_data.silver_finance_data._read_finance_json", return_value=valuation_frame),
        patch("core.delta_core.store_delta") as mock_store,
        patch("core.delta_core.get_delta_schema_columns", return_value=None),
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-02"), watermarks={})
        assert result.status == "ok"

    df = mock_store.call_args.args[0]
    row = df.iloc[0]
    assert row["market_cap"] == pytest.approx(100.1235)
    assert row["pe_ratio"] == pytest.approx(1.2345)
    assert row["forward_pe"] == pytest.approx(2.3456)
    assert row["ev_ebitda"] == pytest.approx(3.4567)
    assert row["ev_revenue"] == pytest.approx(4.5678)
    assert row["shares_outstanding"] == pytest.approx(5.6789)


def test_silver_finance_filters_to_supported_json_report_blobs():
    blobs = [
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv"},
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
        {"name": "finance-data/Cash Flow/MSFT_quarterly_cash-flow.csv"},
        {"name": "finance-data/whitelist.csv"},
    ]

    selected = silver._select_preferred_blob_candidates(blobs)
    selected_names = sorted(item["name"] for item in selected)

    assert "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json" in selected_names
    assert "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.csv" not in selected_names
    assert "finance-data/Cash Flow/MSFT_quarterly_cash-flow.csv" not in selected_names
    assert "finance-data/whitelist.csv" not in selected_names


def test_silver_finance_valuation_requires_silver_market_delta_no_fallback():
    blob_name = "finance-data/Valuation/TEST_quarterly_valuation_measures.json"
    payload = {
        "Symbol": "TEST",
        "MarketCapitalization": "1000",
        "PERatio": "10",
    }
    raw_bytes = json.dumps(payload).encode("utf-8")

    with (
        patch("core.core.read_raw_bytes", return_value=raw_bytes),
        patch("core.delta_core.load_delta", return_value=None) as mock_load_delta,
        patch("core.delta_core.store_delta") as mock_store,
    ):
        result = silver.process_blob({"name": blob_name}, desired_end=pd.Timestamp("2024-01-02"), watermarks={})

    assert result.status == "failed"
    assert result.error == f"Empty finance payload: {blob_name}"
    mock_store.assert_not_called()
    mock_load_delta.assert_called_once_with(
        silver.cfg.AZURE_CONTAINER_SILVER,
        DataPaths.get_silver_market_bucket_path("T"),
        columns=["date", "close", "symbol"],
    )


def test_silver_finance_rejects_legacy_csv_blob():
    result = silver.process_blob(
        {"name": "finance-data/Balance Sheet/TEST_quarterly_balance-sheet.csv"},
        desired_end=pd.Timestamp("2024-01-02"),
        watermarks={},
    )
    assert result.status == "failed"
    assert result.error is not None
    assert "Unsupported finance blob format" in result.error


def test_silver_finance_main_parallel_aggregates_failures_and_updates_watermarks(monkeypatch):
    blobs = [
        {
            "name": "finance-data/buckets/O.parquet",
            "etag": "etag-ok",
            "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
        },
        {
            "name": "finance-data/buckets/S.parquet",
            "etag": "etag-skip",
            "last_modified": datetime(2026, 1, 31, 0, 1, tzinfo=timezone.utc),
        },
        {
            "name": "finance-data/buckets/F.parquet",
            "etag": "etag-fail",
            "last_modified": datetime(2026, 1, 31, 0, 2, tzinfo=timezone.utc),
        },
    ]

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: (list(blobs), 0))
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 1)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(
        silver,
        "_write_alpha26_finance_silver_buckets",
        lambda _frames: (0, "system/silver-index/finance/latest.parquet"),
    )

    initial_watermarks = {"preexisting": {"etag": "keep"}}
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: dict(initial_watermarks))
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)

    saved = {}

    def fake_save_watermarks(key, items):
        saved["key"] = key
        saved["items"] = dict(items)

    monkeypatch.setattr(silver, "save_watermarks", fake_save_watermarks)

    def fake_process_alpha26(
        *,
        candidate_blobs,
        desired_end,
        backfill_start=None,
        watermarks=None,
        persist=True,
        alpha26_bucket_frames=None,
    ):
        del desired_end, backfill_start, persist, alpha26_bucket_frames
        results = []
        for blob in candidate_blobs:
            name = str(blob.get("name", ""))
            if name.endswith("/O.parquet"):
                watermarks[name] = {
                    "etag": "etag-ok",
                    "last_modified": "2026-01-31T00:00:00+00:00",
                    "updated_at": "2026-01-31T00:00:01+00:00",
                }
                results.append(
                    silver.BlobProcessResult(
                        blob_name=name,
                        silver_path="finance-data/balance_sheet/buckets/O",
                        ticker="OK",
                        status="ok",
                        rows_written=7,
                    )
                )
                continue
            if name.endswith("/S.parquet"):
                results.append(
                    silver.BlobProcessResult(
                        blob_name=name,
                        silver_path="finance-data/cash_flow/buckets/S",
                        ticker="SKIP",
                        status="skipped",
                    )
                )
                continue
            results.append(
                silver.BlobProcessResult(
                    blob_name=name,
                    silver_path="finance-data/valuation/buckets/F",
                    ticker="FAIL",
                    status="failed",
                    error="simulated failure",
                )
            )
        return results, 0.01

    monkeypatch.setattr(silver, "_process_alpha26_candidate_blobs", fake_process_alpha26)

    exit_code = silver.main()

    assert exit_code == 1
    assert saved["key"] == "bronze_finance_data"
    assert saved["items"]["preexisting"] == {"etag": "keep"}
    assert saved["items"]["finance-data/buckets/O.parquet"]["etag"] == "etag-ok"
    assert "finance-data/buckets/F.parquet" not in saved["items"]


def test_silver_finance_catchup_pass_processes_newly_discovered_blobs(monkeypatch):
    blob_a = {
        "name": "finance-data/buckets/A.parquet",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    blob_b = {
        "name": "finance-data/buckets/B.parquet",
        "etag": "etag-b",
        "last_modified": datetime(2026, 1, 31, 0, 1, tzinfo=timezone.utc),
    }
    listings = [
        ([blob_a], 0),  # initial
        ([blob_a, blob_b], 0),  # pass 2 discovers B
        ([blob_a, blob_b], 0),  # pass 3 converges (no candidates)
        ([blob_a, blob_b], 0),  # lag probe
    ]
    list_index = {"value": 0}

    def _fake_list():
        idx = min(list_index["value"], len(listings) - 1)
        list_index["value"] += 1
        return listings[idx]

    def _fake_process(*, candidate_blobs, desired_end, backfill_start=None, watermarks=None, **_kwargs):
        del desired_end, backfill_start
        out = []
        for blob in candidate_blobs:
            name = blob["name"]
            ticker = name.split("/")[-1].split(".", 1)[0]
            watermarks[name] = {
                "etag": blob["etag"],
                "last_modified": blob["last_modified"].isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            out.append(
                silver.BlobProcessResult(
                    blob_name=name,
                    silver_path=f"finance-data/balance_sheet/buckets/{ticker}",
                    ticker=ticker,
                    status="ok",
                    rows_written=1,
                )
            )
        return out, 0.01

    saved_last_success = {}
    saved_watermarks = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", _fake_list)
    monkeypatch.setattr(silver, "_process_alpha26_candidate_blobs", _fake_process)
    monkeypatch.setattr(silver, "_write_alpha26_finance_silver_buckets", lambda _frames: (2, "index"))
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 3)
    monkeypatch.setattr(silver.layer_bucketing, "silver_alpha26_force_rebuild", lambda: False)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )
    monkeypatch.setattr(
        silver,
        "save_watermarks",
        lambda key, items: saved_watermarks.update({"key": key, "items": dict(items)}),
    )

    exit_code = silver.main()
    assert exit_code == 0
    assert saved_last_success["key"] == "silver_finance_data"
    assert saved_last_success["metadata"]["new_blobs_discovered_after_first_pass"] == 1
    assert saved_last_success["metadata"]["lag_candidate_count"] == 0
    assert saved_last_success["metadata"]["catchup_passes"] >= 2
    assert saved_watermarks["key"] == "bronze_finance_data"


def test_silver_finance_main_records_alpha26_listing_source(monkeypatch):
    bucket_blob = {
        "name": "finance-data/buckets/A.parquet",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    saved_last_success = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: ([dict(bucket_blob)], 0))
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 1)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(
        silver,
        "_process_alpha26_candidate_blobs",
        lambda *, candidate_blobs, desired_end, backfill_start=None, watermarks=None, **_kwargs: (
            [
                silver.BlobProcessResult(
                    blob_name=candidate_blobs[0]["name"],
                    silver_path="finance-data/valuation/buckets/A",
                    ticker="A",
                    status="ok",
                    rows_written=1,
                    watermark_signature={
                        "etag": "etag-a",
                        "last_modified": bucket_blob["last_modified"].isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            ],
            0.01,
        ),
    )
    monkeypatch.setattr(silver, "_write_alpha26_finance_silver_buckets", lambda _frames: (1, "index"))
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )
    monkeypatch.setattr(silver, "save_watermarks", lambda *args, **kwargs: None)

    exit_code = silver.main()
    assert exit_code == 0
    assert saved_last_success["metadata"]["source"] == "alpha26-bucket-listing"
    assert saved_last_success["metadata"]["manifest_run_id"] is None
    assert saved_last_success["metadata"]["manifest_path"] is None


def test_write_alpha26_finance_silver_buckets_aligns_empty_bucket_to_existing_schema(monkeypatch):
    existing_cols = ["date", "symbol"] + [f"metric_{idx}" for idx in range(1, 38)]
    target_path = "finance-data/balance_sheet/buckets/A"
    captured: dict[str, object] = {}

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet",))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")
    monkeypatch.setattr(
        silver.delta_core,
        "get_delta_schema_columns",
        lambda _container, path: existing_cols if path == target_path else None,
    )

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite") -> None:
        captured["df"] = df.copy()
        captured["path"] = path
        captured["mode"] = mode

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path = silver._write_alpha26_finance_silver_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["path"] == target_path
    assert captured["mode"] == "overwrite"
    df_written = captured["df"]
    assert isinstance(df_written, pd.DataFrame)
    assert df_written.empty
    assert list(df_written.columns) == existing_cols


def test_write_alpha26_finance_silver_buckets_skips_empty_bucket_without_existing_schema(monkeypatch):
    target_path = "finance-data/balance_sheet/buckets/C"
    captured: dict[str, object] = {"store_calls": 0, "checked_paths": []}

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet",))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("C",))
    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", lambda **_kwargs: "index")

    def _fake_get_schema(_container: str, path: str):
        captured["checked_paths"].append(path)
        return None

    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", _fake_get_schema)

    def _fake_store(df: pd.DataFrame, _container: str, path: str, mode: str = "overwrite") -> None:
        captured["store_calls"] = int(captured["store_calls"]) + 1
        captured["path"] = path
        captured["mode"] = mode
        captured["df"] = df.copy()

    monkeypatch.setattr(silver.delta_core, "store_delta", _fake_store)

    written_symbols, index_path = silver._write_alpha26_finance_silver_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]
