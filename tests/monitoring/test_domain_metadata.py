from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from core import delta_core
from monitoring.domain_metadata import collect_delta_table_metadata


def test_collect_delta_table_metadata_reports_rows_and_date_range(tmp_path) -> None:
    # Use the test storage redirection fixture (see tests/conftest.py) which patches delta URIs to local paths.
    container = "test-container"
    table_path = "market-data-by-date"

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
    meta = collect_delta_table_metadata("test-container", "market-data-by-date", warnings=warnings)

    assert warnings == []
    assert meta["dateRange"] is not None
    assert meta["dateRange"]["column"] == "date"
    assert meta["totalRows"] == 5
    assert meta["fileCount"] == 2
    assert meta["totalBytes"] == 340
    assert meta["deltaVersion"] == 7

    min_dt = datetime.fromisoformat(meta["dateRange"]["min"]).astimezone(timezone.utc)
    max_dt = datetime.fromisoformat(meta["dateRange"]["max"]).astimezone(timezone.utc)
    assert min_dt.date().isoformat() == "2024-01-01"
    assert max_dt.date().isoformat() == "2024-01-10"

