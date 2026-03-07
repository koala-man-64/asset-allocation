from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from tasks.common import postgres_gold_sync as sync


class _FakeCursor:
    def __init__(self, *, fetchall_rows=None, fail_on_execute: bool = False) -> None:
        self.fetchall_rows = list(fetchall_rows or [])
        self.fail_on_execute = fail_on_execute
        self.executed: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params=None) -> None:
        self.executed.append((sql, params))
        if self.fail_on_execute:
            raise RuntimeError("boom")

    def fetchall(self):
        return list(self.fetchall_rows)


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_load_domain_sync_state_returns_bucket_map(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = _FakeCursor(
        fetchall_rows=[
            ("a", 101.0, "success", 12, 2, "2026-03-07T01:02:03Z", None),
            ("b", 99.0, "failed", 0, 0, "2026-03-06T01:02:03Z", "bad"),
        ]
    )
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    state = sync.load_domain_sync_state("postgresql://test", domain="market")

    assert state["A"]["source_commit"] == 101.0
    assert state["A"]["status"] == "success"
    assert state["B"]["error"] == "bad"


def test_bucket_sync_is_current_requires_successful_matching_commit() -> None:
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 100.0, "status": "success"}},
            bucket="A",
            source_commit=100.0,
        )
        is True
    )
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 99.0, "status": "success"}},
            bucket="A",
            source_commit=100.0,
        )
        is False
    )
    assert (
        sync.bucket_sync_is_current(
            {"A": {"source_commit": 100.0, "status": "failed"}},
            bucket="A",
            source_commit=100.0,
        )
        is False
    )


def test_sync_gold_bucket_deletes_scope_symbols_copies_rows_and_updates_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = _FakeCursor()
    copied: dict[str, object] = {}

    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(cursor))

    def _fake_copy_rows(cur, *, table, columns, rows) -> None:
        copied["cursor"] = cur
        copied["table"] = table
        copied["columns"] = list(columns)
        copied["rows"] = list(rows)

    monkeypatch.setattr(sync, "copy_rows", _fake_copy_rows)

    result = sync.sync_gold_bucket(
        domain="market",
        bucket="a",
        frame=pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-02")],
                "symbol": ["aapl"],
                "close": [101.5],
                "range": [2.25],
                "volume": [1000],
            }
        ),
        scope_symbols=["MSFT"],
        source_commit=123.0,
        dsn="postgresql://test",
    )

    assert result.status == "ok"
    assert result.bucket == "A"
    assert result.row_count == 1
    assert result.symbol_count == 1
    assert result.scope_symbol_count == 2
    assert result.min_key == date(2026, 1, 2)
    assert copied["table"] == "gold.market_data"
    assert '"range"' in copied["columns"]
    assert copied["rows"][0][0] == date(2026, 1, 2)
    assert copied["rows"][0][1] == "AAPL"
    assert any("DELETE FROM gold.market_data" in sql for sql, _params in cursor.executed)
    assert any("INSERT INTO core.gold_sync_state" in sql for sql, _params in cursor.executed)


def test_sync_gold_bucket_records_failure_state(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}
    monkeypatch.setattr(sync, "connect", lambda _dsn: _FakeConnection(_FakeCursor(fail_on_execute=True)))
    monkeypatch.setattr(sync, "_record_failed_sync_state", lambda *args, **kwargs: recorded.update(kwargs))

    with pytest.raises(sync.PostgresError, match="Gold Postgres sync failed"):
        sync.sync_gold_bucket(
            domain="finance",
            bucket="A",
            frame=pd.DataFrame({"date": [pd.Timestamp("2026-01-02")], "symbol": ["AAPL"]}),
            scope_symbols=["AAPL"],
            source_commit=321.0,
            dsn="postgresql://test",
        )

    assert recorded["domain"] == "finance"
    assert recorded["bucket"] == "A"
    assert recorded["source_commit"] == 321.0
