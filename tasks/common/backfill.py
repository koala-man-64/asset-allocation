from __future__ import annotations

from typing import Optional, Tuple

import os
import pandas as pd

from core import core as mdc


def _parse_bool(raw: Optional[str], *, default: bool) -> bool:
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return default


def get_latest_only_flag(domain: str, *, default: bool = True) -> bool:
    domain_key = f"SILVER_{domain.upper()}_LATEST_ONLY"
    domain_raw = os.environ.get(domain_key)
    if domain_raw is None:
        domain_raw = os.environ.get("SILVER_LATEST_ONLY")
    return _parse_bool(domain_raw, default=default)


def get_backfill_range() -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    # Backfill dates are intentionally non-configurable.
    # Callers should derive the minimal required window from existing data.
    return None, None


def filter_by_date(df: pd.DataFrame, date_col: str, start: Optional[pd.Timestamp], end: Optional[pd.Timestamp]) -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return df
    series = pd.to_datetime(df[date_col], errors="coerce")
    mask = series.notna()
    if start is not None:
        mask &= series >= start
    if end is not None:
        mask &= series <= end
    return df.loc[mask].copy()


def apply_backfill_start_cutoff(
    df: pd.DataFrame,
    *,
    date_col: str,
    backfill_start: Optional[pd.Timestamp],
    context: str,
) -> tuple[pd.DataFrame, int]:
    """
    Drop rows older than the provided start cutoff and return (filtered_df, dropped_count).
    """
    if backfill_start is None or df is None or df.empty or date_col not in df.columns:
        return df, 0

    before_count = int(len(df))
    filtered = filter_by_date(df, date_col, backfill_start, None)
    after_count = int(len(filtered))
    dropped = max(0, before_count - after_count)
    if dropped > 0:
        mdc.write_line(
            f"{context}: dropped {dropped} row(s) prior to start cutoff={backfill_start.date().isoformat()}."
        )
    return filtered, dropped
