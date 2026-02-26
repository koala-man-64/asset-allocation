from __future__ import annotations

from typing import Any

from tasks.common.market_reconciliation import (
    collect_delta_silver_finance_symbols,
    collect_bronze_market_symbols_from_blob_infos,
    collect_delta_market_symbols,
    purge_orphan_market_tables,
)


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
