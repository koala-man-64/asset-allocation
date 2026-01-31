from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Protocol, TypeVar, Union

from core import core as mdc


class _ByDateMain(Protocol):
    def __call__(self, argv: Optional[list[str]] = None) -> int: ...


PartnerReturn = TypeVar("PartnerReturn", bound=Union[None, int])


def _get_materialize_year_months(now: Optional[datetime] = None) -> list[str]:
    """
    Compute target year-month partitions for by-date materialization.

    Behavior mirrors the deployed by-date jobs:
      - If MATERIALIZE_YEAR_MONTH is set, use it (single partition).
      - Otherwise default to yesterday's year-month in UTC, expanded by
        MATERIALIZE_WINDOW_MONTHS (default 1).
    """

    override_raw = os.environ.get("MATERIALIZE_YEAR_MONTH")
    if override_raw:
        override = override_raw.strip()
        if override:
            return [override]

    window_raw = os.environ.get("MATERIALIZE_WINDOW_MONTHS")
    try:
        window = int(window_raw) if window_raw else 1
    except ValueError:
        window = 1

    if window <= 0:
        return []

    now_utc = now or datetime.now(timezone.utc)
    anchor = now_utc - timedelta(days=1)
    year = anchor.year
    month = anchor.month
    months: list[str] = []
    for _ in range(window):
        months.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month <= 0:
            month = 12
            year -= 1
    return months


def _get_by_date_run_at_utc_hour() -> Optional[int]:
    raw = os.environ.get("MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR")
    if raw is None:
        return None

    raw = raw.strip()
    if not raw:
        return None

    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(
            "Invalid MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR. Expected integer hour 0-23."
        ) from exc


def _should_run_by_date(run_at_utc_hour: Optional[int], now: Optional[datetime] = None) -> bool:
    if run_at_utc_hour is None:
        return True

    if run_at_utc_hour < 0:
        return False

    now_utc = now or datetime.now(timezone.utc)
    return now_utc.hour == run_at_utc_hour


def run_partner_then_by_date(
    *,
    job_name: str,
    partner_main: Callable[[], PartnerReturn],
    by_date_main: _ByDateMain,
    by_date_run_at_utc_hour: Optional[int] = None,
) -> int:
    """
    Run a partner job and then materialize its by-date table immediately after completion.

    - Holds a single distributed lock (`JobLock`) across both steps.
    - Defaults by-date year-month to yesterday's month in UTC (or uses MATERIALIZE_YEAR_MONTH).
    - Can gate by-date execution to a single UTC hour (env or argument) to enforce daily runs.
    """

    with mdc.JobLock(job_name):
        partner_rc = partner_main()
        if isinstance(partner_rc, int) and partner_rc != 0:
            return partner_rc

        run_hour = (
            by_date_run_at_utc_hour if by_date_run_at_utc_hour is not None else _get_by_date_run_at_utc_hour()
        )
        if not _should_run_by_date(run_hour):
            mdc.write_line(
                f"Skipping by-date materialization for job={job_name} "
                f"(MATERIALIZE_BY_DATE_RUN_AT_UTC_HOUR={run_hour})."
            )
            return 0

        year_months = _get_materialize_year_months()
        if not year_months:
            mdc.write_line(
                f"Skipping by-date materialization for job={job_name} "
                "(MATERIALIZE_WINDOW_MONTHS<=0)."
            )
            return 0

        for year_month in year_months:
            mdc.write_line(f"Running by-date materialization for job={job_name} year_month={year_month}...")
            rc = by_date_main(["--year-month", year_month])
            if isinstance(rc, int) and rc != 0:
                return rc
        return 0
