from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from core import delta_core
from monitoring.domain_metadata import collect_delta_table_metadata
from deltalake.exceptions import TableNotFoundError


def test_collect_delta_table_metadata_reports_rows_and_date_range(tmp_path) -> None:
    # Use the test storage redirection fixture (see tests/conftest.py) which patches delta URIs to local paths.
    container = "test-container"
    table_path = "market-data/AAPL"

    df = pd.DataFrame(
        {
            "symbol": ["A", "B", "A", "C"],
            "date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"]),
            "value": [1, 2, 3, 4],
        }
    )

    delta_core.store_delta(df, container=container, path=table_path, mode="overwrite", merge_schema=True)

    meta = collect_delta_table_metadata(container, table_path)
    assert meta["totalRows"] == 4
    assert meta["fileCount"] >= 1
    assert meta["totalBytes"] > 0
    assert meta["deltaVersion"] >= 0

    date_range = meta["dateRange"]
    assert date_range is not None
    assert date_range["source"] == "stats"
    assert date_range["column"] in {"date", "Date"}

    min_dt = datetime.fromisoformat(date_range["min"]).astimezone(timezone.utc)
    max_dt = datetime.fromisoformat(date_range["max"]).astimezone(timezone.utc)
    assert min_dt.date().isoformat() == "2024-01-01"
    assert max_dt.date().isoformat() == "2024-01-04"


def test_collect_delta_table_metadata_parses_string_date_stats(monkeypatch) -> None:
    class _FakeStructArray:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_pylist(self) -> list[dict[str, object]]:
            return self._rows

    class _FakeActions:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_struct_array(self) -> _FakeStructArray:
            return _FakeStructArray(self._rows)

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 7

        def get_add_actions(self, *args, **kwargs) -> _FakeActions:
            return _FakeActions(
                [
                    {
                        "num_records": 2,
                        "size_bytes": 120,
                        "min.date": "2024-01-01",
                        "max.date": "2024-01-03",
                    },
                    {
                        "num_records": 3,
                        "size_bytes": 220,
                        "min.date": "2024-01-04",
                        "max.date": "2024-01-10",
                    },
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "stats"
    assert meta["dateRange"]["column"] == "date"
    assert meta["totalRows"] == 5
    assert meta["fileCount"] == 2
    assert meta["totalBytes"] == 340
    assert meta["deltaVersion"] == 7

    min_dt = datetime.fromisoformat(meta["dateRange"]["min"]).astimezone(timezone.utc)
    max_dt = datetime.fromisoformat(meta["dateRange"]["max"]).astimezone(timezone.utc)
    assert min_dt.date().isoformat() == "2024-01-01"
    assert max_dt.date().isoformat() == "2024-01-10"


def _fake_add_action_factory() -> type:
    """Build a fake add-actions object for collect_delta_table_metadata tests."""
    class _FakeStructArray:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_pylist(self) -> list[dict[str, object]]:
            return self._rows

    class _FakeActions:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def to_struct_array(self) -> _FakeStructArray:
            return _FakeStructArray(self._rows)

    return _FakeActions


def test_collect_delta_table_metadata_uses_partition_date_when_available(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 3

        def get_add_actions(self, *args, **kwargs):
            rows = [
                {
                    "num_records": 1,
                    "size_bytes": 100,
                    "partition.Date": "2024-01-03T00:00:00",
                    "path": "part-1",
                },
                {
                    "num_records": 2,
                    "size_bytes": 120,
                    "partition.date": "2024-01-01T00:00:00",
                    "path": "part-2",
                },
            ]
            return fake_actions(rows)

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-01-03"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-01-03"


def test_collect_delta_table_metadata_prefers_partition_over_stats(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 9

        def get_add_actions(self, flatten: bool = False, *args, **kwargs):
            if flatten is False:
                return fake_actions(
                    [
                        {
                            "num_records": 1,
                            "size_bytes": 120,
                            "partition": {"Date": "2024-01-05"},
                            "path": "part-3",
                            "min": {"date": "2024-02-01"},
                            "max": {"date": "2024-02-10"},
                        }
                    ]
                )

            return fake_actions(
                [
                    {
                        "num_records": 1,
                        "size_bytes": 120,
                        "partition.Date": "2024-01-01",
                        "min.date": "2024-02-01",
                        "max.date": "2024-02-10",
                    }
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-01-01"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-01-01"


def test_collect_delta_table_metadata_uses_partition_values(monkeypatch) -> None:
    fake_actions = _fake_add_action_factory()

    class _FakeDeltaTable:
        def __init__(self, *args, **kwargs) -> None:
            return None

        def version(self) -> int:
            return 11

        def get_add_actions(self, *args, **kwargs):
            return fake_actions(
                [
                        {
                            "num_records": 2,
                            "size_bytes": 160,
                            "partition_values": {"date": "2024-05-01"},
                        },
                        {
                            "num_records": 1,
                            "size_bytes": 80,
                            "partition_values": {"Date": "2024-05-03"},
                    },
                ]
            )

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _FakeDeltaTable)
    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["source"] == "partition"
    assert meta["dateRange"]["column"] == "Date"
    assert datetime.fromisoformat(meta["dateRange"]["min"]).date().isoformat() == "2024-05-03"
    assert datetime.fromisoformat(meta["dateRange"]["max"]).date().isoformat() == "2024-05-03"


def test_collect_delta_table_metadata_handles_no_files_in_log_segment(monkeypatch) -> None:
    def _raise(*_args, **_kwargs) -> None:
        raise TableNotFoundError("Generic delta kernel error: No files in log segment")

    monkeypatch.setattr("monitoring.domain_metadata.DeltaTable", _raise)

    warnings: list[str] = []
    meta = collect_delta_table_metadata("test-container", "market-data/AAPL", warnings=warnings)

    assert meta["deltaVersion"] is None
    assert meta["fileCount"] == 0
    assert meta["totalBytes"] == 0
    assert meta["totalRows"] == 0
    assert meta["dateRange"] is None
    assert warnings == [
        "Delta table not readable at market-data/AAPL; no commit files found in _delta_log yet."
    ]

