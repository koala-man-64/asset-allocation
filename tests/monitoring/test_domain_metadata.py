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

