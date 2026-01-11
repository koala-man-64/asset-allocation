"""
Materialize derived ranking tables (signals + composite) from canonical rankings.

Usage:
  python -m scripts.ranking.materialize_signals --year-month 2026-01
"""

from __future__ import annotations

import argparse
import os
from typing import List

from scripts.common import config as cfg
from scripts.common.core import write_line
from scripts.ranking.signals import DEFAULT_TOP_N, materialize_signals_for_year_month


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize ranking signals/composites from canonical rankings.")
    parser.add_argument(
        "--year-month",
        dest="year_months",
        action="append",
        required=True,
        help="Year-month partition to materialize (YYYY-MM). Repeatable.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Top-N threshold used for strategies_hit (default: {DEFAULT_TOP_N}).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if "AZURE_CONTAINER_RANKING" not in os.environ:
        raise ValueError("Missing required environment variable: AZURE_CONTAINER_RANKING")
    container = cfg.AZURE_CONTAINER_RANKING

    year_months: List[str] = sorted({str(v).strip() for v in args.year_months if str(v).strip()})
    write_line(f"Materializing signals for {len(year_months)} month(s): {', '.join(year_months)}")

    for year_month in year_months:
        result = materialize_signals_for_year_month(container=container, year_month=year_month, top_n=args.top_n)
        write_line(
            f"{year_month}: rankings_rows={result.rankings_rows} signals_rows={result.signals_rows} "
            f"composite_rows={result.composite_rows}"
        )


if __name__ == "__main__":
    main()

