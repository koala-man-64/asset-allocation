from __future__ import annotations

from typing import Any

from tasks.common.market_reconciliation import (
    collect_bronze_earnings_symbols_from_blob_infos,
    collect_bronze_finance_symbols_from_blob_infos,
    collect_delta_silver_finance_symbols,
    collect_bronze_price_target_symbols_from_blob_infos,
    collect_bronze_market_symbols_from_blob_infos,
    collect_delta_market_symbols,
    enforce_backfill_cutoff_on_tables,
    purge_orphan_market_tables,
)
import pandas as pd


def test_collect_bronze_market_symbols_ignores_non_symbol_files() -> None:
    blob_infos = [
        {"name": "market-data/AAPL.csv"},
        {"name": "market-data/MSFT.csv"},
        {"name": "market-data/whitelist.csv"},
        {"name": "market-data/blacklist.csv"},
        {"name": "market-data/README.txt"},
        {"name": "market-data/subdir/NVDA.csv"},
    ]

    symbols = collect_bronze_market_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_delta_market_symbols_extracts_from_delta_log_paths() -> None:
    class _Client:
        def list_blob_infos(self, *, name_starts_with: str) -> list[dict[str, Any]]:
            assert name_starts_with == "market-data/"
            return [
                {"name": "market-data/AAPL/_delta_log/00000000000000000000.json"},
                {"name": "market-data/AAPL/_delta_log/00000000000000000001.json"},
                {"name": "market-data/MSFT/_delta_log/00000000000000000000.json"},
                {"name": "market-data/README.txt"},
            ]

    symbols = collect_delta_market_symbols(client=_Client(), root_prefix="market-data")

    assert symbols == {"AAPL", "MSFT"}


def test_purge_orphan_market_tables_returns_deleted_blobs_total() -> None:
    deleted_paths: list[str] = []

    def _delete_prefix(path: str) -> int:
        deleted_paths.append(path)
        return 2

    orphan_symbols, deleted_blobs = purge_orphan_market_tables(
        upstream_symbols={"AAPL"},
        downstream_symbols={"AAPL", "MSFT", "NVDA"},
        downstream_path_builder=lambda symbol: f"market-data/{symbol}",
        delete_prefix=_delete_prefix,
    )

    assert orphan_symbols == ["MSFT", "NVDA"]
    assert deleted_paths == ["market-data/MSFT", "market-data/NVDA"]
    assert deleted_blobs == 4


def test_collect_delta_silver_finance_symbols_extracts_union_of_subfolders() -> None:
    class _Client:
        def list_blob_infos(self, *, name_starts_with: str) -> list[dict[str, Any]]:
            assert name_starts_with == "finance-data/"
            return [
                {"name": "finance-data/balance_sheet/AAPL_quarterly_balance-sheet/_delta_log/000.json"},
                {"name": "finance-data/income_statement/MSFT_quarterly_financials/_delta_log/000.json"},
                {"name": "finance-data/cash_flow/NVDA_quarterly_cash-flow/_delta_log/000.json"},
                {"name": "finance-data/valuation/TSLA_quarterly_valuation_measures/_delta_log/000.json"},
                {"name": "finance-data/valuation/README.md"},
                {"name": "finance-data/valuation/BADTABLE/_delta_log/000.json"},
                {"name": "finance-data/custom_folder/SHOP_custom/_delta_log/000.json"},
            ]

    symbols = collect_delta_silver_finance_symbols(client=_Client())

    assert symbols == {"AAPL", "MSFT", "NVDA", "TSLA"}


def test_collect_bronze_earnings_symbols_extracts_json_symbols() -> None:
    blob_infos = [
        {"name": "earnings-data/AAPL.json"},
        {"name": "earnings-data/MSFT.json"},
        {"name": "earnings-data/whitelist.csv"},
        {"name": "earnings-data/not_json.parquet"},
    ]

    symbols = collect_bronze_earnings_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_bronze_price_target_symbols_extracts_parquet_symbols() -> None:
    blob_infos = [
        {"name": "price-target-data/AAPL.parquet"},
        {"name": "price-target-data/MSFT.parquet"},
        {"name": "price-target-data/not_parquet.json"},
    ]

    symbols = collect_bronze_price_target_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT"}


def test_collect_bronze_finance_symbols_extracts_known_suffixes() -> None:
    blob_infos = [
        {"name": "finance-data/Balance Sheet/AAPL_quarterly_balance-sheet.json"},
        {"name": "finance-data/Income Statement/AAPL_quarterly_financials.json"},
        {"name": "finance-data/Cash Flow/MSFT_quarterly_cash-flow.json"},
        {"name": "finance-data/Valuation/NVDA_quarterly_valuation_measures.json"},
        {"name": "finance-data/Valuation/NVDA_other_suffix.json"},
        {"name": "finance-data/blacklist.csv"},
    ]

    symbols = collect_bronze_finance_symbols_from_blob_infos(blob_infos)

    assert symbols == {"AAPL", "MSFT", "NVDA"}


def test_enforce_backfill_cutoff_on_tables_rewrites_and_deletes() -> None:
    saved: dict[str, pd.DataFrame] = {}
    deleted_paths: list[str] = []
    vacuumed_paths: list[str] = []

    def _table_paths(symbol: str) -> list[str]:
        return [f"market-data/{symbol}"]

    def _load_table(path: str) -> pd.DataFrame | None:
        if path.endswith("AAPL"):
            return pd.DataFrame(
                {
                    "Date": [pd.Timestamp("2015-12-31"), pd.Timestamp("2016-01-02")],
                    "value": [1, 2],
                }
            )
        if path.endswith("MSFT"):
            return pd.DataFrame({"Date": [pd.Timestamp("2015-12-30")], "value": [7]})
        return None

    def _store_table(df: pd.DataFrame, path: str) -> None:
        saved[path] = df.copy()

    def _delete_prefix(path: str) -> int:
        deleted_paths.append(path)
        return 3

    def _vacuum(path: str) -> None:
        vacuumed_paths.append(path)

    stats = enforce_backfill_cutoff_on_tables(
        symbols={"AAPL", "MSFT"},
        table_paths_for_symbol=_table_paths,
        load_table=_load_table,
        store_table=_store_table,
        delete_prefix=_delete_prefix,
        date_column_candidates=("date", "obs_date"),
        backfill_start=pd.Timestamp("2016-01-01"),
        context="test cutoff",
        vacuum_table=_vacuum,
    )

    assert stats.tables_scanned == 2
    assert stats.tables_rewritten == 1
    assert stats.deleted_blobs == 3
    assert stats.rows_dropped == 2
    assert stats.errors == 0

    assert list(saved.keys()) == ["market-data/AAPL"]
    assert pd.to_datetime(saved["market-data/AAPL"]["Date"]).min() == pd.Timestamp("2016-01-02")
    assert deleted_paths == ["market-data/MSFT"]
    assert vacuumed_paths == ["market-data/AAPL"]


def test_enforce_backfill_cutoff_on_tables_handles_missing_date_column() -> None:
    stats = enforce_backfill_cutoff_on_tables(
        symbols={"AAPL"},
        table_paths_for_symbol=lambda _symbol: ["market-data/AAPL"],
        load_table=lambda _path: pd.DataFrame({"close": [1.0, 2.0]}),
        store_table=lambda _df, _path: None,
        delete_prefix=lambda _path: 0,
        date_column_candidates=("date", "obs_date"),
        backfill_start=pd.Timestamp("2016-01-01"),
        context="test cutoff",
        vacuum_table=None,
    )

    assert stats.tables_scanned == 1
    assert stats.tables_rewritten == 0
    assert stats.deleted_blobs == 0
    assert stats.rows_dropped == 0
    assert stats.errors == 0
