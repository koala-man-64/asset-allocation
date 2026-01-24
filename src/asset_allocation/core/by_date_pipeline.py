from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional, Protocol, TypeVar, Union

from asset_allocation.core import core as mdc


class _ByDateMain(Protocol):
    def __call__(self, argv: Optional[list[str]] = None) -> int: ...


PartnerReturn = TypeVar("PartnerReturn", bound=Union[None, int])


def _get_materialize_year_month(now: Optional[datetime] = None) -> str:
    """
    Compute the target year-month partition for by-date materialization.

    Behavior mirrors the deployed by-date jobs:
      - If MATERIALIZE_YEAR_MONTH is set, use it.
      - Otherwise default to yesterday's year-month in UTC.
    """

    override = os.environ.get("MATERIALIZE_YEAR_MONTH", "").strip()
    if override:
        return override

    now_utc = now or datetime.now(timezone.utc)
    return (now_utc - timedelta(days=1)).strftime("%Y-%m")


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

        year_month = _get_materialize_year_month()
        mdc.write_line(f"Running by-date materialization for job={job_name} year_month={year_month}...")
        return by_date_main(["--year-month", year_month])

