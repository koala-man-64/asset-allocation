from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence, Set, Tuple

import pandas as pd

from tasks.common.backfill import apply_backfill_start_cutoff

_FINANCE_BRONZE_SUBFOLDERS = {
    "Balance Sheet",
    "Income Statement",
    "Cash Flow",
    "Valuation",
}
_FINANCE_SUFFIXES = (
    "quarterly_balance-sheet",
    "quarterly_financials",
    "quarterly_cash-flow",
    "quarterly_valuation_measures",
)
_FINANCE_SILVER_SUBFOLDERS = {
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
}


@dataclass(frozen=True)
class CutoffSweepStats:
    tables_scanned: int
    tables_rewritten: int
    deleted_blobs: int
    rows_dropped: int
    errors: int


def _extract_bronze_market_symbol(blob_name: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) != 2 or parts[0] != "market-data":
        return None
    filename = parts[1].strip()
    if not filename.endswith(".csv"):
        return None
    if filename in {"whitelist.csv", "blacklist.csv"}:
        return None
    symbol = filename[: -len(".csv")].strip()
    return symbol or None


def _extract_bronze_earnings_symbol(blob_name: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) != 2 or parts[0] != "earnings-data":
        return None
    filename = parts[1].strip()
    if not filename.endswith(".json"):
        return None
    if filename in {"whitelist.csv", "blacklist.csv"}:
        return None
    symbol = filename[: -len(".json")].strip()
    return symbol or None


def _extract_bronze_price_target_symbol(blob_name: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) != 2 or parts[0] != "price-target-data":
        return None
    filename = parts[1].strip()
    if not filename.endswith(".parquet"):
        return None
    if filename in {"whitelist.csv", "blacklist.csv"}:
        return None
    symbol = filename[: -len(".parquet")].strip()
    return symbol or None


def _extract_bronze_finance_symbol(blob_name: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) != 3:
        return None
    if parts[0] != "finance-data":
        return None
    if parts[1] not in _FINANCE_BRONZE_SUBFOLDERS:
        return None
    filename = parts[2].strip()
    if not filename.endswith(".json"):
        return None
    stem = filename[: -len(".json")].strip()
    for suffix in _FINANCE_SUFFIXES:
        token = f"_{suffix}"
        if stem.endswith(token):
            symbol = stem[: -len(token)].strip()
            return symbol or None
    return None


def _extract_delta_symbol(blob_name: str, *, root_prefix: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) < 4:
        return None
    if parts[0] != str(root_prefix).strip("/"):
        return None
    if parts[2] != "_delta_log":
        return None
    symbol = parts[1].strip()
    return symbol or None


def _extract_silver_finance_symbol(blob_name: str) -> Optional[str]:
    parts = str(blob_name or "").strip("/").split("/")
    if len(parts) < 5:
        return None
    if parts[0] != "finance-data":
        return None
    if parts[1] not in _FINANCE_SILVER_SUBFOLDERS:
        return None
    if parts[3] != "_delta_log":
        return None
    table_name = parts[2].strip()
    if "_" not in table_name:
        return None
    symbol = table_name.split("_", 1)[0].strip()
    return symbol or None


def collect_bronze_market_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    symbols: Set[str] = set()
    for blob in blob_infos:
        symbol = _extract_bronze_market_symbol(str(blob.get("name") or ""))
        if symbol:
            symbols.add(symbol)
    return symbols


def collect_bronze_earnings_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    symbols: Set[str] = set()
    for blob in blob_infos:
        symbol = _extract_bronze_earnings_symbol(str(blob.get("name") or ""))
        if symbol:
            symbols.add(symbol)
    return symbols


def collect_bronze_price_target_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    symbols: Set[str] = set()
    for blob in blob_infos:
        symbol = _extract_bronze_price_target_symbol(str(blob.get("name") or ""))
        if symbol:
            symbols.add(symbol)
    return symbols


def collect_bronze_finance_symbols_from_blob_infos(blob_infos: Sequence[dict[str, Any]]) -> Set[str]:
    symbols: Set[str] = set()
    for blob in blob_infos:
        symbol = _extract_bronze_finance_symbol(str(blob.get("name") or ""))
        if symbol:
            symbols.add(symbol)
    return symbols


def collect_delta_symbols(*, client: Any, root_prefix: str) -> Set[str]:
    symbols: Set[str] = set()
    listing_prefix = f"{str(root_prefix).strip('/')}/"
    for blob in client.list_blob_infos(name_starts_with=listing_prefix):
        symbol = _extract_delta_symbol(str(blob.get("name") or ""), root_prefix=root_prefix)
        if symbol:
            symbols.add(symbol)
    return symbols


def collect_delta_market_symbols(*, client: Any, root_prefix: str) -> Set[str]:
    return collect_delta_symbols(client=client, root_prefix=root_prefix)


def collect_delta_silver_finance_symbols(*, client: Any) -> Set[str]:
    symbols: Set[str] = set()
    for blob in client.list_blob_infos(name_starts_with="finance-data/"):
        symbol = _extract_silver_finance_symbol(str(blob.get("name") or ""))
        if symbol:
            symbols.add(symbol)
    return symbols


def purge_orphan_tables(
    *,
    upstream_symbols: Set[str],
    downstream_symbols: Set[str],
    downstream_path_builder: Callable[[str], str],
    delete_prefix: Callable[[str], int],
) -> Tuple[list[str], int]:
    orphan_symbols = sorted(downstream_symbols - upstream_symbols)
    deleted_blobs = 0
    for symbol in orphan_symbols:
        deleted_blobs += int(delete_prefix(downstream_path_builder(symbol)) or 0)
    return orphan_symbols, deleted_blobs


def purge_orphan_market_tables(
    *,
    upstream_symbols: Set[str],
    downstream_symbols: Set[str],
    downstream_path_builder: Callable[[str], str],
    delete_prefix: Callable[[str], int],
) -> Tuple[list[str], int]:
    return purge_orphan_tables(
        upstream_symbols=upstream_symbols,
        downstream_symbols=downstream_symbols,
        downstream_path_builder=downstream_path_builder,
        delete_prefix=delete_prefix,
    )


def _resolve_date_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    by_normalized: dict[str, str] = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key and key not in by_normalized:
            by_normalized[key] = str(col)
    for candidate in candidates:
        key = str(candidate).strip().lower()
        if key in by_normalized:
            return by_normalized[key]
    return None


def enforce_backfill_cutoff_on_tables(
    *,
    symbols: Set[str],
    table_paths_for_symbol: Callable[[str], Sequence[str]],
    load_table: Callable[[str], Optional[pd.DataFrame]],
    store_table: Callable[[pd.DataFrame, str], None],
    delete_prefix: Callable[[str], int],
    date_column_candidates: Sequence[str],
    backfill_start: Optional[pd.Timestamp],
    context: str,
    vacuum_table: Optional[Callable[[str], None]] = None,
) -> CutoffSweepStats:
    if backfill_start is None:
        return CutoffSweepStats(
            tables_scanned=0,
            tables_rewritten=0,
            deleted_blobs=0,
            rows_dropped=0,
            errors=0,
        )

    tables_scanned = 0
    tables_rewritten = 0
    deleted_blobs = 0
    rows_dropped = 0
    errors = 0

    for symbol in sorted(symbols):
        for table_path in table_paths_for_symbol(symbol):
            tables_scanned += 1
            try:
                df = load_table(table_path)
            except Exception:
                errors += 1
                continue
            if df is None or df.empty:
                continue

            date_col = _resolve_date_column(df, date_column_candidates)
            if not date_col:
                continue

            try:
                filtered, dropped = apply_backfill_start_cutoff(
                    df,
                    date_col=date_col,
                    backfill_start=backfill_start,
                    context=f"{context} {symbol}",
                )
            except Exception:
                errors += 1
                continue

            if dropped <= 0:
                continue

            rows_dropped += int(dropped)
            if filtered is None or filtered.empty:
                try:
                    deleted_blobs += int(delete_prefix(table_path) or 0)
                except Exception:
                    errors += 1
                continue

            try:
                store_table(filtered, table_path)
                tables_rewritten += 1
                if vacuum_table is not None:
                    vacuum_table(table_path)
            except Exception:
                errors += 1

    return CutoffSweepStats(
        tables_scanned=tables_scanned,
        tables_rewritten=tables_rewritten,
        deleted_blobs=deleted_blobs,
        rows_dropped=rows_dropped,
        errors=errors,
    )
