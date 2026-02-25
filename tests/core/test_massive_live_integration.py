from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from massive_provider import MassiveClient, MassiveConfig


def _live_enabled() -> bool:
    run_flag = str(os.getenv("RUN_LIVE_MASSIVE_TESTS", "")).strip().lower()
    api_key = str(os.getenv("MASSIVE_API_KEY", "")).strip()
    return run_flag in {"1", "true", "t", "yes", "y", "on"} and bool(api_key)


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.skipif(
    not _live_enabled(),
    reason="Set RUN_LIVE_MASSIVE_TESTS=1 and MASSIVE_API_KEY to run live Massive integration tests.",
)
def test_live_snapshot_and_daily_calls_succeed_without_mocking() -> None:
    cfg = MassiveConfig(
        api_key=str(os.getenv("MASSIVE_API_KEY", "")).strip(),
        base_url=str(os.getenv("MASSIVE_BASE_URL") or "https://api.massive.com"),
        timeout_seconds=float(os.getenv("MASSIVE_TIMEOUT_SECONDS") or 30.0),
        prefer_official_sdk=False,
    )
    requested = {"AAPL", "MSFT"}

    with MassiveClient(cfg) as client:
        snapshot = client.get_unified_snapshot(tickers=sorted(requested), asset_type="stocks")
        rows = snapshot.get("results") if isinstance(snapshot, dict) else None
        assert isinstance(rows, list) and rows

        returned = {
            str(row.get("ticker") or "").strip().upper()
            for row in rows
            if isinstance(row, dict) and str(row.get("ticker") or "").strip()
        }
        assert returned
        assert returned.issubset(requested)
        assert requested.intersection(returned)

        end_date = datetime.now(timezone.utc).date().isoformat()
        bars = client.list_ohlcv(
            ticker="AAPL",
            multiplier=1,
            timespan="day",
            from_="1970-01-01",
            to=end_date,
            adjusted=True,
            sort="asc",
            limit=10,
            pagination=False,
        )
        assert isinstance(bars, list)
        assert len(bars) > 0
