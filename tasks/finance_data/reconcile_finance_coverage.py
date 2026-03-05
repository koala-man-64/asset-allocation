from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Set, Tuple

from core import core as mdc
from tasks.finance_data import config as cfg
from tasks.common import bronze_bucketing
from tasks.common import layer_bucketing


_FINANCE_SUBFOLDERS: Tuple[str, ...] = (
    "balance_sheet",
    "income_statement",
    "cash_flow",
    "valuation",
)
_REPORT_TYPE_TO_SUBFOLDER: dict[str, str] = {
    "balance_sheet": "balance_sheet",
    "income_statement": "income_statement",
    "cash_flow": "cash_flow",
    "overview": "valuation",
    "valuation": "valuation",
}
_COMMON_REPORT_PATH = "system/reconciliation/finance_coverage/latest.json"


def _empty_symbol_map() -> Dict[str, Set[str]]:
    return {subfolder: set() for subfolder in _FINANCE_SUBFOLDERS}


def _collect_bronze_symbols() -> Dict[str, Set[str]]:
    client = mdc.get_storage_client(cfg.AZURE_CONTAINER_BRONZE)
    if client is None:
        raise RuntimeError("Unable to initialize Bronze storage client.")

    out = _empty_symbol_map()
    for bucket in bronze_bucketing.ALPHABET_BUCKETS:
        try:
            df_bucket = bronze_bucketing.read_bucket_parquet(
                client=client,
                prefix="finance-data",
                bucket=bucket,
                columns=["symbol", "report_type"],
            )
        except Exception as exc:
            mdc.write_warning(f"Finance coverage bronze scan failed for bucket={bucket}: {exc}")
            continue
        if df_bucket is None or df_bucket.empty:
            continue
        for _, row in df_bucket.iterrows():
            symbol = str(row.get("symbol") or "").strip().upper()
            report_type = str(row.get("report_type") or "").strip().lower()
            subfolder = _REPORT_TYPE_TO_SUBFOLDER.get(report_type)
            if symbol and subfolder:
                out[subfolder].add(symbol)
    return out


def _collect_silver_symbols() -> Dict[str, Set[str]]:
    out = _empty_symbol_map()
    for subfolder in _FINANCE_SUBFOLDERS:
        out[subfolder] = layer_bucketing.load_layer_symbol_set(
            layer="silver",
            domain="finance",
            sub_domain=subfolder,
        )
    return out


def _build_report() -> dict:
    generated_at = datetime.now(timezone.utc).isoformat()
    bronze = _collect_bronze_symbols()
    silver = _collect_silver_symbols()

    by_subfolder: Dict[str, dict] = {}
    total_bronze_only = 0
    total_silver_only = 0
    for silver_folder in _FINANCE_SUBFOLDERS:
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
    bronze_bucketing.bronze_layout_mode()
    layer_bucketing.silver_layout_mode()
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
