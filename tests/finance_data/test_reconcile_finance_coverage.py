from __future__ import annotations

from tasks.finance_data import reconcile_finance_coverage as reconcile


def test_extract_symbol_helpers():
    bronze = reconcile._extract_symbol_from_bronze_blob(
        "finance-data/Valuation/AAPL_quarterly_valuation_measures.json",
        folder="Valuation",
        suffix="quarterly_valuation_measures",
    )
    silver = reconcile._extract_symbol_from_silver_blob(
        "finance-data/valuation/AAPL_quarterly_valuation_measures/_delta_log/00000000000000000001.json",
        folder="valuation",
        suffix="quarterly_valuation_measures",
    )
    assert bronze == "AAPL"
    assert silver == "AAPL"


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
