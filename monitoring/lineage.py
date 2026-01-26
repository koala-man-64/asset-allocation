from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

from tasks.ranking.strategies import (
    BrokenGrowthImprovingInternalsStrategy,
    MomentumStrategy,
    ValueStrategy,
)

logger = logging.getLogger("asset_allocation.monitoring.lineage")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _strategy_inputs(strategy_name: str, sources_used: List[str]) -> List[str]:
    # Market features are implicit for all ranking strategies.
    normalized = [str(item).strip().lower() for item in (sources_used or []) if str(item).strip()]
    inputs = ["market"]
    for item in normalized:
        if item == "price_targets":
            inputs.append("price-target")
        else:
            inputs.append(item)
    # Stable order
    seen: Set[str] = set()
    ordered: List[str] = []
    for item in inputs:
        if item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def get_lineage_snapshot() -> Dict[str, Any]:
    logger.info("Generating lineage snapshot.")
    strategies = [
        MomentumStrategy(),
        ValueStrategy(),
        BrokenGrowthImprovingInternalsStrategy(),
    ]

    strategies_out: List[Dict[str, Any]] = []
    impacts_by_domain: Dict[str, List[str]] = {}

    for strat in strategies:
        sources = list(getattr(strat, "sources_used", []) or [])
        name = str(getattr(strat, "name", "")).strip() or "strategy"
        inputs = _strategy_inputs(name, sources)
        strategies_out.append(
            {
                "name": name,
                "sourcesUsed": sources,
                "inputs": inputs,
                "outputs": ["rankings", "signals"],
            }
        )
        for domain in inputs:
            impacts_by_domain.setdefault(domain, []).append(name)

    # Keep output stable for diffs
    for domain in list(impacts_by_domain.keys()):
        impacts_by_domain[domain] = sorted(set(impacts_by_domain[domain]))

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
                    {"domain": "rankings", "artifact": "platinum/rankings (delta)"},
                    {"domain": "signals", "artifact": "platinum/signals/daily (delta)"},
                ],
            },
        ],
        "strategies": strategies_out,
        "impactsByDomain": impacts_by_domain,
    }

