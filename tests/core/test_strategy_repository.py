from __future__ import annotations

import json

from core.strategy_repository import StrategyRepository


class _FakeCursor:
    def __init__(self, *, fetchone_result=None, fetchall_result=None) -> None:
        self.fetchone_result = fetchone_result
        self.fetchall_result = fetchall_result or []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def cursor(self) -> _FakeCursor:
        return self._cursor


def test_get_strategy_config_reads_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_result=(
            {
                "universe": {
                    "source": "postgres_gold",
                    "root": {
                        "kind": "group",
                        "operator": "and",
                        "clauses": [
                            {
                                "kind": "condition",
                                "table": "market_data",
                                "column": "close",
                                "operator": "gt",
                                "value": 10,
                            }
                        ],
                    },
                }
            },
        )
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.get_strategy_config("momentum") == {
        "universe": {
            "source": "postgres_gold",
            "root": {
                "kind": "group",
                "operator": "and",
                "clauses": [
                    {
                        "kind": "condition",
                        "table": "market_data",
                        "column": "close",
                        "operator": "gt",
                        "value": 10,
                    }
                ],
            },
        }
    }
    sql, params = cursor.execute_calls[0]
    assert "FROM core.strategies" in sql
    assert params == ("momentum",)


def test_save_strategy_writes_to_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor()

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")
    repo.save_strategy(
        name="momentum",
        config={"rebalance": "monthly"},
        strategy_type="configured",
        description="Monthly momentum",
    )

    sql, params = cursor.execute_calls[0]
    assert "INSERT INTO core.strategies" in sql
    assert params == (
        "momentum",
        json.dumps({"rebalance": "monthly"}),
        "configured",
        "Monthly momentum",
    )


def test_get_strategy_reads_metadata_and_config(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchone_result=(
            "momentum",
            "configured",
            "Monthly momentum",
            "2026-03-07T00:00:00Z",
            {"rebalance": "monthly"},
        )
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.get_strategy("momentum") == {
        "name": "momentum",
        "type": "configured",
        "description": "Monthly momentum",
        "updated_at": "2026-03-07T00:00:00Z",
        "config": {"rebalance": "monthly"},
    }
    sql, params = cursor.execute_calls[0]
    assert "SELECT name, type, description, updated_at, config" in sql
    assert params == ("momentum",)


def test_list_strategies_reads_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(
        fetchall_result=[
            ("momentum", "configured", "Monthly momentum", "2026-03-07T00:00:00Z"),
        ]
    )

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.list_strategies() == [
        {
            "name": "momentum",
            "type": "configured",
            "description": "Monthly momentum",
            "updated_at": "2026-03-07T00:00:00Z",
        }
    ]
    sql, params = cursor.execute_calls[0]
    assert "FROM core.strategies" in sql
    assert params is None


def test_delete_strategy_deletes_from_core_schema(monkeypatch) -> None:
    cursor = _FakeCursor(fetchone_result=("momentum",))

    monkeypatch.setattr(
        "core.strategy_repository.connect",
        lambda _dsn: _FakeConnection(cursor),
    )

    repo = StrategyRepository("postgresql://user:pass@localhost/db")

    assert repo.delete_strategy("momentum") is True
    sql, params = cursor.execute_calls[0]
    assert "DELETE FROM core.strategies" in sql
    assert "RETURNING name" in sql
    assert params == ("momentum",)
