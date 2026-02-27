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
        "market-data/TEST",
        columns=["date", "close"],
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
    class BronzeClientStub:
        def __init__(self, blobs):
            self._blobs = blobs

        def list_blob_infos(self, name_starts_with=None):
            assert name_starts_with == "finance-data/"
            return list(self._blobs)

    blobs = [
        {"name": "finance-data/Balance Sheet/OK_quarterly_balance-sheet.json"},
        {"name": "finance-data/Cash Flow/SKIP_quarterly_cash-flow.json"},
        {"name": "finance-data/Valuation/FAIL_quarterly_valuation_measures.json"},
    ]

    monkeypatch.setattr(silver, "bronze_client", BronzeClientStub(blobs))
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "_get_ingest_max_workers", lambda: 4)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))

    initial_watermarks = {"preexisting": {"etag": "keep"}}
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: dict(initial_watermarks))
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)

    saved = {}

    def fake_save_watermarks(key, items):
        saved["key"] = key
        saved["items"] = dict(items)

    monkeypatch.setattr(silver, "save_watermarks", fake_save_watermarks)

    def fake_process_blob(blob, *, desired_end, backfill_start=None, watermarks=None):
        name = blob["name"]
        if name.endswith("OK_quarterly_balance-sheet.json"):
            return silver.BlobProcessResult(
                blob_name=name,
                silver_path="finance-data/balance_sheet/OK_quarterly_balance-sheet",
                ticker="OK",
                status="ok",
                rows_written=7,
                watermark_signature={
                    "etag": "etag-ok",
                    "last_modified": "2026-01-31T00:00:00+00:00",
                    "updated_at": "2026-01-31T00:00:01+00:00",
                },
            )
        if name.endswith("SKIP_quarterly_cash-flow.json"):
            return silver.BlobProcessResult(
                blob_name=name,
                silver_path="finance-data/cash_flow/SKIP_quarterly_cash-flow",
                ticker="SKIP",
                status="skipped",
            )
        return silver.BlobProcessResult(
            blob_name=name,
            silver_path="finance-data/valuation/FAIL_quarterly_valuation_measures",
            ticker="FAIL",
            status="failed",
            error="simulated failure",
        )

    monkeypatch.setattr(silver, "process_blob", fake_process_blob)

    exit_code = silver.main()

    assert exit_code == 1
    assert saved["key"] == "bronze_finance_data"
    assert saved["items"]["preexisting"] == {"etag": "keep"}
    assert saved["items"]["finance-data/Balance Sheet/OK_quarterly_balance-sheet.json"]["etag"] == "etag-ok"
    assert "finance-data/Valuation/FAIL_quarterly_valuation_measures.json" not in saved["items"]


def test_silver_finance_catchup_pass_processes_newly_discovered_blobs(monkeypatch):
    blob_a = {
        "name": "finance-data/Balance Sheet/A_quarterly_balance-sheet.json",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    blob_b = {
        "name": "finance-data/Balance Sheet/B_quarterly_balance-sheet.json",
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

    def _fake_process(candidate_blobs, **_kwargs):
        out = []
        for blob in candidate_blobs:
            name = blob["name"]
            ticker = name.split("/")[-1].split("_", 1)[0]
            out.append(
                silver.BlobProcessResult(
                    blob_name=name,
                    silver_path=f"finance-data/balance_sheet/{ticker}_quarterly_balance-sheet",
                    ticker=ticker,
                    status="ok",
                    rows_written=1,
                    watermark_signature={
                        "etag": blob["etag"],
                        "last_modified": blob["last_modified"].isoformat(),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            )
        return out, 0.01

    saved_last_success = {}
    saved_watermarks = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "silver_manifest_consumption_enabled", lambda: False)
    monkeypatch.setattr(silver, "_list_bronze_finance_candidates", _fake_list)
    monkeypatch.setattr(silver, "_process_candidate_blobs", _fake_process)
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 3)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(silver, "_run_finance_reconciliation", lambda *, bronze_blob_list: (0, 0))
    monkeypatch.setattr(silver, "write_silver_finance_ack", lambda **_kwargs: None)
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


def test_silver_finance_manifest_mode_consumes_unacked_manifest_and_writes_ack(monkeypatch):
    manifest_blob = {
        "name": "finance-data/Valuation/A_quarterly_valuation_measures.json",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc).isoformat(),
    }
    live_blob = {
        "name": "finance-data/Valuation/A_quarterly_valuation_measures.json",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    manifest_payload = {
        "runId": "bronze-finance-20260131T000000000000Z-abcd1234",
        "manifestPath": "system/run-manifests/bronze_finance/bronze-finance-20260131T000000000000Z-abcd1234.json",
        "blobs": [manifest_blob],
    }
    ack_calls = {}
    saved_last_success = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(silver, "silver_manifest_consumption_enabled", lambda: True)
    monkeypatch.setattr(silver, "load_latest_bronze_finance_manifest", lambda: dict(manifest_payload))
    monkeypatch.setattr(silver, "silver_finance_ack_exists", lambda _run_id: False)
    monkeypatch.setattr(silver, "_list_bronze_finance_candidates", lambda: ([dict(live_blob)], 0))
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 2)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(silver, "_run_finance_reconciliation", lambda *, bronze_blob_list: (0, 0))
    monkeypatch.setattr(
        silver,
        "_process_candidate_blobs",
        lambda candidate_blobs, **_kwargs: (
            [
                silver.BlobProcessResult(
                    blob_name=candidate_blobs[0]["name"],
                    silver_path="finance-data/valuation/A_quarterly_valuation_measures",
                    ticker="A",
                    status="ok",
                    rows_written=1,
                    watermark_signature={
                        "etag": "etag-a",
                        "last_modified": manifest_blob["last_modified"],
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            ],
            0.01,
        ),
    )
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )
    monkeypatch.setattr(silver, "save_watermarks", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        silver,
        "write_silver_finance_ack",
        lambda **kwargs: ack_calls.update(kwargs) or "system/run-manifests/silver_finance/ack.json",
    )

    exit_code = silver.main()
    assert exit_code == 0
    assert saved_last_success["metadata"]["source"] == "bronze-manifest"
    assert saved_last_success["metadata"]["manifest_run_id"] == manifest_payload["runId"]
    assert ack_calls["run_id"] == manifest_payload["runId"]


def test_run_finance_reconciliation_purges_silver_orphans(monkeypatch):
    class _FakeSilverClient:
        def __init__(self) -> None:
            self.deleted_paths: list[str] = []

        def delete_prefix(self, path: str) -> int:
            self.deleted_paths.append(path)
            return 2

    fake_client = _FakeSilverClient()
    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(silver, "collect_delta_silver_finance_symbols", lambda *, client: {"AAPL", "MSFT"})
    monkeypatch.setattr(silver, "get_backfill_range", lambda: (None, None))

    orphan_count, deleted_blobs = silver._run_finance_reconciliation(
        bronze_blob_list=[
            {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
            {"name": "finance-data/Income Statement/AAPL_quarterly_financials.json"},
        ]
    )

    assert orphan_count == 1
    assert deleted_blobs == 8
    assert fake_client.deleted_paths == [
        DataPaths.get_finance_path("balance_sheet", "MSFT", "quarterly_balance-sheet"),
        DataPaths.get_finance_path("income_statement", "MSFT", "quarterly_financials"),
        DataPaths.get_finance_path("cash_flow", "MSFT", "quarterly_cash-flow"),
        DataPaths.get_finance_path("valuation", "MSFT", "quarterly_valuation_measures"),
    ]


def test_run_finance_reconciliation_applies_cutoff_sweep(monkeypatch):
    class _FakeSilverClient:
        def delete_prefix(self, _path: str) -> int:
            return 0

    fake_client = _FakeSilverClient()
    captured: dict = {}

    monkeypatch.setattr(silver, "silver_client", fake_client)
    monkeypatch.setattr(silver, "collect_delta_silver_finance_symbols", lambda *, client: {"AAPL", "MSFT"})
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

    silver._run_finance_reconciliation(
        bronze_blob_list=[
            {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
        ]
    )

    assert captured["symbols"] == {"AAPL"}
    assert captured["backfill_start"] == pd.Timestamp("2016-01-01")


def test_run_finance_reconciliation_requires_storage_client(monkeypatch):
    monkeypatch.setattr(silver, "silver_client", None)

    with pytest.raises(RuntimeError, match="requires silver storage client"):
        silver._run_finance_reconciliation(bronze_blob_list=[])
