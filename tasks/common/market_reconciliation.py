from __future__ import annotations

from typing import Any, Callable, Optional, Sequence, Set, Tuple

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
