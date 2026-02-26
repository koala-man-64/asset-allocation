from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Set, Tuple

from core import core as mdc
from tasks.finance_data import config as cfg


_FINANCE_SUBFOLDERS: Tuple[Tuple[str, str, str], ...] = (
    ("Balance Sheet", "balance_sheet", "quarterly_balance-sheet"),
    ("Income Statement", "income_statement", "quarterly_financials"),
    ("Cash Flow", "cash_flow", "quarterly_cash-flow"),
    ("Valuation", "valuation", "quarterly_valuation_measures"),
)
_COMMON_REPORT_PATH = "system/reconciliation/finance_coverage/latest.json"


def _extract_symbol_from_bronze_blob(blob_name: str, *, folder: str, suffix: str) -> str:
    parts = str(blob_name).strip("/").split("/")
    if len(parts) < 3 or parts[0] != "finance-data" or parts[1] != folder:
        return ""
    filename = parts[2]
    if not filename.endswith(".json"):
        return ""
    stem = filename[:-5]
    token = f"_{suffix}"
    if not stem.endswith(token):
        return ""
    return stem[: -len(token)].strip()


def _extract_symbol_from_silver_blob(blob_name: str, *, folder: str, suffix: str) -> str:
    parts = str(blob_name).strip("/").split("/")
    if len(parts) < 5 or parts[0] != "finance-data" or parts[1] != folder:
        return ""
    if parts[3] != "_delta_log":
        return ""
    table = parts[2]
    token = f"_{suffix}"
    if not table.endswith(token):
        return ""
    return table[: -len(token)].strip()


def _collect_bronze_symbols() -> Dict[str, Set[str]]:
    client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
    if client is None:
        raise RuntimeError("Unable to initialize Bronze storage client.")

    out: Dict[str, Set[str]] = {silver_folder: set() for _, silver_folder, _ in _FINANCE_SUBFOLDERS}
    for bronze_folder, silver_folder, suffix in _FINANCE_SUBFOLDERS:
        prefix = f"finance-data/{bronze_folder}/"
        for blob in client.list_blob_infos(name_starts_with=prefix):
            symbol = _extract_symbol_from_bronze_blob(
                str(blob.get("name", "")),
                folder=bronze_folder,
                suffix=suffix,
            )
            if symbol:
                out[silver_folder].add(symbol)
    return out


def _collect_silver_symbols() -> Dict[str, Set[str]]:
    client = mdc.get_storage_client(cfg.AZURE_CONTAINER_SILVER)
    if client is None:
        raise RuntimeError("Unable to initialize Silver storage client.")

    out: Dict[str, Set[str]] = {silver_folder: set() for _, silver_folder, _ in _FINANCE_SUBFOLDERS}
    for _, silver_folder, suffix in _FINANCE_SUBFOLDERS:
        prefix = f"finance-data/{silver_folder}/"
        for blob in client.list_blob_infos(name_starts_with=prefix):
            symbol = _extract_symbol_from_silver_blob(
                str(blob.get("name", "")),
                folder=silver_folder,
                suffix=suffix,
            )
            if symbol:
                out[silver_folder].add(symbol)
    return out


def _build_report() -> dict:
    generated_at = datetime.now(timezone.utc).isoformat()
    bronze = _collect_bronze_symbols()
    silver = _collect_silver_symbols()

    by_subfolder: Dict[str, dict] = {}
    total_bronze_only = 0
    total_silver_only = 0
    for _, silver_folder, _ in _FINANCE_SUBFOLDERS:
        bronze_symbols = bronze.get(silver_folder, set())
        silver_symbols = silver.get(silver_folder, set())
        bronze_only = sorted(bronze_symbols - silver_symbols)
        silver_only = sorted(silver_symbols - bronze_symbols)
        total_bronze_only += len(bronze_only)
        total_silver_only += len(silver_only)
        by_subfolder[silver_folder] = {
            "bronzeSymbolCount": len(bronze_symbols),
            "silverSymbolCount": len(silver_symbols),
            # Backward-compatible lag fields (bronze-only symbols).
            "lagSymbolCount": len(bronze_only),
            "lagSymbolSample": bronze_only[:100],
            # Explicit directional drift fields.
            "bronzeOnlySymbolCount": len(bronze_only),
            "bronzeOnlySymbolSample": bronze_only[:100],
            "silverOnlySymbolCount": len(silver_only),
            "silverOnlySymbolSample": silver_only[:100],
        }

    return {
        "version": 1,
        "generatedAt": generated_at,
        "domain": "finance",
        "layerPair": "bronze->silver",
        "totalLagSymbolCount": total_bronze_only,
        "totalBronzeOnlySymbolCount": total_bronze_only,
        "totalSilverOnlySymbolCount": total_silver_only,
        "subfolders": by_subfolder,
    }


def main() -> int:
    mdc.log_environment_diagnostics()
    try:
        report = _build_report()
        mdc.save_common_json_content(report, _COMMON_REPORT_PATH)
        mdc.write_line(
            "Finance coverage reconciliation updated: path={path} totalLagSymbolCount={lag}".format(
                path=_COMMON_REPORT_PATH,
                lag=report.get("totalLagSymbolCount"),
            )
        )
        return 0
    except Exception as exc:
        mdc.write_error(f"Finance coverage reconciliation failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
