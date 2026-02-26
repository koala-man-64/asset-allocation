from __future__ import annotations

import pandas as pd

from tasks.common import backfill


def test_get_backfill_range_defaults_to_2016_floor(monkeypatch) -> None:
    monkeypatch.delenv("BACKFILL_START_DATE", raising=False)
    monkeypatch.delenv("BACKFILL_END_DATE", raising=False)
    monkeypatch.delenv("MARKET_BACKFILL_START_DATE", raising=False)
    monkeypatch.delenv("MARKET_BACKFILL_END_DATE", raising=False)

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_get_backfill_range_applies_explicit_start(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "2020-05-03")
    monkeypatch.delenv("BACKFILL_END_DATE", raising=False)

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2020-05-03")
    assert end is None


def test_get_backfill_range_clamps_start_before_floor(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "2010-01-01")
    monkeypatch.delenv("BACKFILL_END_DATE", raising=False)

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_get_backfill_range_uses_legacy_start_when_primary_missing(monkeypatch) -> None:
    monkeypatch.delenv("BACKFILL_START_DATE", raising=False)
    monkeypatch.setenv("MARKET_BACKFILL_START_DATE", "2019-04-01")
    monkeypatch.delenv("BACKFILL_END_DATE", raising=False)

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2019-04-01")
    assert end is None


def test_get_backfill_range_ignores_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "not-a-date")
    monkeypatch.setenv("BACKFILL_END_DATE", "also-not-a-date")

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2016-01-01")
    assert end is None


def test_get_backfill_range_drops_end_before_start(monkeypatch) -> None:
    monkeypatch.setenv("BACKFILL_START_DATE", "2022-01-01")
    monkeypatch.setenv("BACKFILL_END_DATE", "2021-12-31")

    start, end = backfill.get_backfill_range()

    assert start == pd.Timestamp("2022-01-01")
    assert end is None
