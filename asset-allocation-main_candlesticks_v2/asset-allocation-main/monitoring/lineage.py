from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

logger = logging.getLogger("asset_allocation.monitoring.lineage")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_lineage_snapshot() -> Dict[str, Any]:
    logger.info("Generating lineage snapshot.")

    impacts_by_domain: Dict[str, List[str]] = {}

    # Strategies removed, so strategies_out is empty or could be removed if the schema allows.
    # Assuming 'strategies' key in output expects a list, we return empty.
    strategies_out: List[Dict[str, Any]] = []

    logger.info(
        "Lineage snapshot ready: strategies=%s domains=%s",
        len(strategies_out),
        len(impacts_by_domain.keys()),
    )
    return {
        "generatedAt": _utc_now_iso(),
        "layers": [
            {
                "layer": "bronze",
                "domains": [
                    {"domain": "market", "artifact": "market-data/*.csv"},
                    {"domain": "finance", "artifact": "finance-data/*.csv"},
                    {"domain": "earnings", "artifact": "earnings-data/*.csv"},
                    {"domain": "price-target", "artifact": "price-target-data/*.csv"},
                ],
            },
            {
                "layer": "silver",
                "domains": [
                    {"domain": "market", "artifact": "market-data-by-date (delta)"},
                    {"domain": "finance", "artifact": "finance-data-by-date (delta)"},
                    {"domain": "earnings", "artifact": "earnings-data-by-date (delta)"},
                    {"domain": "price-target", "artifact": "price-target-data-by-date (delta)"},
                ],
            },
            {
                "layer": "gold",
                "domains": [
                    {"domain": "market", "artifact": "market_by_date (delta)"},
                    {"domain": "finance", "artifact": "finance_by_date (delta)"},
                    {"domain": "earnings", "artifact": "earnings_by_date (delta)"},
                    {"domain": "price-target", "artifact": "targets_by_date (delta)"},
                ],
            },
            {
                "layer": "platinum",
                "domains": [
                    {"domain": "platinum", "artifact": "platinum/* (reserved)"},
                ],
            },
        ],
        "strategies": strategies_out,
        "impactsByDomain": impacts_by_domain,
    }

