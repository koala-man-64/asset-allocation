import json
from datetime import datetime, timezone

import pandas as pd

from tasks.finance_data import silver_finance_data as silver


def test_read_finance_json_projects_only_piotroski_columns() -> None:
    payload = {
        "quarterlyReports": [
            {
                "fiscalDateEnding": "2024-03-31",
                "totalAssets": "1000",
                "totalCurrentAssets": "250",
                "totalCurrentLiabilities": "125",
                "commonStockSharesOutstanding": "50",
                "longTermDebt": "300",
                "reportedCurrency": "USD",
            }
        ]
    }

    out = silver._read_finance_json(
        json.dumps(payload).encode("utf-8"),
        ticker="AAPL",
        suffix="quarterly_balance-sheet",
    )

    assert list(out.columns) == [
        "Date",
        "Symbol",
        "long_term_debt",
        "total_assets",
        "current_assets",
        "current_liabilities",
        "shares_outstanding",
    ]
    assert out.loc[0, "Symbol"] == "AAPL"
    assert out.loc[0, "total_assets"] == 1000.0
    assert "reportedCurrency" not in out.columns


def test_process_alpha26_bucket_blob_skips_overview_rows_for_piotroski_only_contract(monkeypatch) -> None:
    blob_name = "finance-data/buckets/A.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-a",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "report_type": "overview",
                "payload_json": json.dumps({"Symbol": "AAPL", "MarketCapitalization": "100"}),
            }
        ]
    )
    watermarks: dict[str, dict[str, str]] = {}

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert blob_name in watermarks


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
    monkeypatch.setattr(
        silver,
        "_select_initial_alpha26_source",
        lambda: silver._ManifestSelection(source="alpha26-bucket-listing", blobs=list(blobs), deduped=0),
    )
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: (list(blobs), 0))
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 1)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-01-31"))
    monkeypatch.setattr(
        silver,
        "_write_alpha26_finance_silver_buckets",
        lambda _frames: (0, "system/silver-index/finance/latest.parquet", None),
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
                        silver_path="finance-data/cash_flow/buckets/F",
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
    monkeypatch.setattr(
        silver,
        "_select_initial_alpha26_source",
        lambda: silver._ManifestSelection(source="alpha26-bucket-listing", blobs=_fake_list()[0], deduped=0),
    )
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", _fake_list)
    monkeypatch.setattr(silver, "_process_alpha26_candidate_blobs", _fake_process)
    monkeypatch.setattr(silver, "_write_alpha26_finance_silver_buckets", lambda _frames: (2, "index", 11))
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
    assert saved_last_success["metadata"]["column_count"] == 11
    assert saved_watermarks["key"] == "bronze_finance_data"


def test_silver_finance_main_records_alpha26_listing_source(monkeypatch):
    bucket_blob = {
        "name": "finance-data/buckets/A.parquet",
        "etag": "etag-a",
        "last_modified": datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc),
    }
    saved_last_success = {}

    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(
        silver,
        "_select_initial_alpha26_source",
        lambda: silver._ManifestSelection(source="alpha26-bucket-listing", blobs=[dict(bucket_blob)], deduped=0),
    )
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
                    silver_path="finance-data/cash_flow/buckets/A",
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
    monkeypatch.setattr(silver, "_write_alpha26_finance_silver_buckets", lambda _frames: (1, "index", 11))
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
    assert saved_last_success["metadata"]["column_count"] == 11


def test_silver_finance_select_initial_source_uses_unacked_manifest(monkeypatch):
    manifest = {
        "runId": "bronze-finance-20260305T000000000000Z-abcd1234",
        "manifestPath": "system/run-manifests/bronze_finance/run.json",
        "blobs": [
            {"name": "finance-data/buckets/A.parquet"},
            {"name": "finance-data/Balance Sheet/A_quarterly_balance-sheet.json"},
        ],
    }
    monkeypatch.setattr(silver.run_manifests, "silver_manifest_consumption_enabled", lambda: True)
    monkeypatch.setattr(silver.run_manifests, "load_latest_bronze_finance_manifest", lambda: dict(manifest))
    monkeypatch.setattr(silver.run_manifests, "silver_finance_ack_exists", lambda _run_id: False)

    selection = silver._select_initial_alpha26_source()
    assert selection.source == "bronze-manifest"
    assert selection.manifest_run_id == manifest["runId"]
    assert selection.manifest_path == manifest["manifestPath"]
    assert selection.manifest_blob_count == 2
    assert selection.manifest_filtered_bucket_blob_count == 1
    assert [item["name"] for item in selection.blobs] == ["finance-data/buckets/A.parquet"]


def test_silver_finance_select_initial_source_falls_back_when_manifest_is_acked(monkeypatch):
    manifest = {
        "runId": "bronze-finance-20260305T000000000000Z-abcd1234",
        "manifestPath": "system/run-manifests/bronze_finance/run.json",
        "blobs": [{"name": "finance-data/buckets/A.parquet"}],
    }
    listed = [{"name": "finance-data/buckets/Z.parquet"}]
    monkeypatch.setattr(silver.run_manifests, "silver_manifest_consumption_enabled", lambda: True)
    monkeypatch.setattr(silver.run_manifests, "load_latest_bronze_finance_manifest", lambda: dict(manifest))
    monkeypatch.setattr(silver.run_manifests, "silver_finance_ack_exists", lambda _run_id: True)
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: (list(listed), 0))

    selection = silver._select_initial_alpha26_source()
    assert selection.source == "alpha26-bucket-listing"
    assert selection.manifest_run_id is None
    assert selection.blobs == listed


def test_silver_finance_main_acks_manifest_on_success(monkeypatch):
    saved_last_success = {}
    ack_calls: list[dict] = []
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(
        silver,
        "_select_initial_alpha26_source",
        lambda: silver._ManifestSelection(
            source="bronze-manifest",
            blobs=[],
            deduped=0,
            manifest_run_id="bronze-finance-20260305T000000000000Z-abcd1234",
            manifest_path="system/run-manifests/bronze_finance/run.json",
            manifest_blob_count=0,
            manifest_filtered_bucket_blob_count=0,
        ),
    )
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 1)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-03-05"))
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: ([], 0))
    monkeypatch.setattr(silver, "_write_alpha26_finance_silver_buckets", lambda _frames: (0, "index", 11))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        silver,
        "save_last_success",
        lambda key, when=None, metadata=None: saved_last_success.update(
            {"key": key, "when": when, "metadata": metadata}
        ),
    )
    monkeypatch.setattr(
        silver.run_manifests,
        "write_silver_finance_ack",
        lambda **kwargs: ack_calls.append(dict(kwargs)) or "system/run-manifests/silver_finance/ack.json",
    )

    exit_code = silver.main()
    assert exit_code == 0
    assert len(ack_calls) == 1
    assert ack_calls[0]["run_id"].endswith("abcd1234")
    assert ack_calls[0]["metadata"]["column_count"] == 11
    assert saved_last_success["metadata"]["source"] == "bronze-manifest"
    assert saved_last_success["metadata"]["manifest_run_id"].endswith("abcd1234")
    assert saved_last_success["metadata"]["manifest_path"].endswith("run.json")
    assert saved_last_success["metadata"]["column_count"] == 11


def test_silver_finance_main_does_not_ack_manifest_when_failed(monkeypatch):
    bucket_blob = {
        "name": "finance-data/buckets/A.parquet",
        "etag": "etag-a",
        "last_modified": datetime(2026, 3, 5, 0, 0, tzinfo=timezone.utc),
    }
    ack_calls: list[dict] = []
    monkeypatch.setattr(silver.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(
        silver,
        "_select_initial_alpha26_source",
        lambda: silver._ManifestSelection(
            source="bronze-manifest",
            blobs=[dict(bucket_blob)],
            deduped=0,
            manifest_run_id="bronze-finance-20260305T000000000000Z-abcd1234",
            manifest_path="system/run-manifests/bronze_finance/run.json",
            manifest_blob_count=1,
            manifest_filtered_bucket_blob_count=1,
        ),
    )
    monkeypatch.setattr(silver, "_get_catchup_max_passes", lambda: 1)
    monkeypatch.setattr(silver, "_utc_today", lambda: pd.Timestamp("2026-03-05"))
    monkeypatch.setattr(silver, "_list_alpha26_finance_bucket_candidates", lambda: ([dict(bucket_blob)], 0))
    monkeypatch.setattr(silver, "load_watermarks", lambda _key: {})
    monkeypatch.setattr(silver, "load_last_success", lambda _key: None)
    monkeypatch.setattr(silver, "save_watermarks", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        silver,
        "_process_alpha26_candidate_blobs",
        lambda **_kwargs: (
            [
                silver.BlobProcessResult(
                    blob_name=bucket_blob["name"],
                    silver_path=None,
                    ticker="A",
                    status="failed",
                    error="boom",
                )
            ],
            0.01,
        ),
    )
    monkeypatch.setattr(
        silver.run_manifests,
        "write_silver_finance_ack",
        lambda **kwargs: ack_calls.append(dict(kwargs)) or "unexpected",
    )

    exit_code = silver.main()
    assert exit_code == 1
    assert ack_calls == []


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

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets({})

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

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets({})

    assert written_symbols == 0
    assert index_path == "index"
    assert captured["store_calls"] == 0
    assert captured["checked_paths"] == [target_path]


def test_write_alpha26_finance_silver_buckets_writes_sub_domain_indexes(monkeypatch):
    balance_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["AAPL"]})
    cash_flow_df = pd.DataFrame({"date": [pd.Timestamp("2024-01-01")], "symbol": ["MSFT"]})
    bucket_frames = {
        ("balance_sheet", "A"): [balance_df],
        ("cash_flow", "A"): [cash_flow_df],
    }
    index_calls: list[dict] = []

    monkeypatch.setattr(silver, "_FINANCE_ALPHA26_SUBDOMAINS", ("balance_sheet", "cash_flow"))
    monkeypatch.setattr(silver.layer_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(silver.delta_core, "get_delta_schema_columns", lambda *_args, **_kwargs: ["date", "symbol"])
    monkeypatch.setattr(silver.delta_core, "store_delta", lambda *_args, **_kwargs: None)

    def _fake_index(**kwargs):
        index_calls.append(dict(kwargs))
        return "index"

    monkeypatch.setattr(silver.layer_bucketing, "write_layer_symbol_index", _fake_index)

    written_symbols, index_path, _column_count = silver._write_alpha26_finance_silver_buckets(bucket_frames)
    assert written_symbols == 2
    assert index_path == "index"
    assert len(index_calls) == 3
    aggregate = [call for call in index_calls if call.get("sub_domain") is None][0]
    assert aggregate["symbol_to_bucket"] == {"AAPL": "A", "MSFT": "A"}
    sub_domains = sorted(call.get("sub_domain") for call in index_calls if call.get("sub_domain"))
    assert sub_domains == ["balance_sheet", "cash_flow"]


def test_process_alpha26_bucket_blob_does_not_skip_when_signature_matches_watermark(monkeypatch):
    blob_name = "finance-data/buckets/M.parquet"
    blob = {
        "name": blob_name,
        "etag": "etag-m",
        "last_modified": datetime(2026, 3, 4, 1, 0, tzinfo=timezone.utc),
    }
    watermarks = {
        blob_name: {
            "etag": "etag-m",
            "last_modified": "2026-03-04T01:00:00+00:00",
        }
    }
    bucket_df = pd.DataFrame(
        [
            {
                "symbol": "MSFT",
                "report_type": "balance_sheet",
                "payload_json": json.dumps(
                    {"quarterlyReports": [{"fiscalDateEnding": "2024-01-01", "totalAssets": "100"}]}
                ),
            }
        ]
    )
    captured_tickers: list[str] = []

    monkeypatch.setattr(
        silver.mdc,
        "read_raw_bytes",
        lambda _name, client=None: bucket_df.to_parquet(index=False),
    )
    monkeypatch.setattr(
        silver,
        "_read_finance_json",
        lambda _raw, ticker, suffix: pd.DataFrame({"Date": [pd.Timestamp("2024-01-01")], "Symbol": [ticker]}),
    )

    def _fake_process_finance_frame(**kwargs):
        captured_tickers.append(str(kwargs["ticker"]))
        return silver.BlobProcessResult(
            blob_name=kwargs["blob_name"],
            silver_path=kwargs["silver_path"],
            ticker=kwargs["ticker"],
            status="ok",
            rows_written=1,
        )

    monkeypatch.setattr(silver, "_process_finance_frame", _fake_process_finance_frame)

    results = silver.process_alpha26_bucket_blob(
        blob,
        desired_end=pd.Timestamp("2026-03-04"),
        backfill_start=None,
        watermarks=watermarks,
        persist=False,
        alpha26_bucket_frames={},
    )

    assert len(results) == 1
    assert results[0].status == "ok"
    assert captured_tickers == ["MSFT"]
    assert blob_name in watermarks
    assert watermarks[blob_name]["etag"] == "etag-m"
