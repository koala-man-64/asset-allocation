from __future__ import annotations

import pandas as pd

from tasks.finance_data import reconcile_finance_coverage as reconcile


def test_collect_bronze_symbols_reads_alpha26_buckets(monkeypatch):
    monkeypatch.setattr(reconcile.mdc, "get_storage_client", lambda _container: object())
    monkeypatch.setattr(reconcile.bronze_bucketing, "ALPHABET_BUCKETS", ("A",))
    monkeypatch.setattr(
        reconcile.bronze_bucketing,
        "read_bucket_parquet",
        lambda **_kwargs: pd.DataFrame(
            [
                {"symbol": "AAPL", "report_type": "balance_sheet"},
                {"symbol": "AAPL", "report_type": "overview"},
                {"symbol": "MSFT", "report_type": "cash_flow"},
            ]
        ),
    )

    out = reconcile._collect_bronze_symbols()
    assert out["balance_sheet"] == {"AAPL"}
    assert out["valuation"] == {"AAPL"}
    assert out["cash_flow"] == {"MSFT"}
    assert out["income_statement"] == set()


def test_collect_silver_symbols_reads_sub_domain_indexes(monkeypatch):
    expected = {
        "balance_sheet": {"AAPL"},
        "income_statement": {"AAPL", "MSFT"},
        "cash_flow": {"MSFT"},
        "valuation": {"AAPL"},
    }
    monkeypatch.setattr(
        reconcile.layer_bucketing,
        "load_layer_symbol_set",
        lambda *, layer, domain, sub_domain=None: set(expected.get(str(sub_domain), set())),
    )

    out = reconcile._collect_silver_symbols()
    assert out == expected


def test_build_report_computes_lag_counts(monkeypatch):
    monkeypatch.setattr(
        reconcile,
        "_collect_bronze_symbols",
        lambda: {
            "balance_sheet": {"A", "B"},
            "income_statement": {"A"},
            "cash_flow": {"A"},
            "valuation": {"A", "B", "C"},
        },
    )
    monkeypatch.setattr(
        reconcile,
        "_collect_silver_symbols",
        lambda: {
            "balance_sheet": {"A"},
            "income_statement": {"A"},
            "cash_flow": {"A"},
            "valuation": {"A", "B", "Z"},
        },
    )

    report = reconcile._build_report()
    assert report["totalLagSymbolCount"] == 2
    assert report["totalBronzeOnlySymbolCount"] == 2
    assert report["totalSilverOnlySymbolCount"] == 1
    assert report["subfolders"]["balance_sheet"]["lagSymbolCount"] == 1
    assert report["subfolders"]["balance_sheet"]["silverOnlySymbolCount"] == 0
    assert report["subfolders"]["valuation"]["lagSymbolCount"] == 1
    assert report["subfolders"]["valuation"]["silverOnlySymbolCount"] == 1


def test_main_writes_report(monkeypatch):
    saved = {}
    monkeypatch.setattr(reconcile.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(reconcile.bronze_bucketing, "bronze_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(reconcile.layer_bucketing, "silver_layout_mode", lambda: "alpha26")
    monkeypatch.setattr(reconcile, "_build_report", lambda: {"totalLagSymbolCount": 0})
    monkeypatch.setattr(
        reconcile.mdc,
        "save_common_json_content",
        lambda payload, path: saved.update({"payload": payload, "path": path}),
    )
    monkeypatch.setattr(reconcile.mdc, "write_line", lambda _msg: None)

    exit_code = reconcile.main()
    assert exit_code == 0
    assert saved["path"] == "system/reconciliation/finance_coverage/latest.json"
