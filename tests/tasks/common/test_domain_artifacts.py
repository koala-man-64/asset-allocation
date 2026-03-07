from __future__ import annotations

import pandas as pd

from tasks.common import domain_artifacts


def test_summarize_frame_tracks_finance_subdomains() -> None:
    df = pd.DataFrame(
        {
            "symbol": ["AAPL", "AAPL", "MSFT"],
            "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
            "report_type": ["balance_sheet", "overview", "income_statement"],
            "value": [1, 2, 3],
        }
    )

    summary = domain_artifacts.summarize_frame(df, domain="finance", date_column="date")

    assert summary["symbolCount"] == 2
    assert summary["columnCount"] == 4
    assert summary["dateRange"]["source"] == "artifact"
    assert summary["subdomains"]["balance_sheet"]["symbolCount"] == 1
    assert summary["subdomains"]["valuation"]["symbolCount"] == 1
    assert summary["subdomains"]["income_statement"]["symbolCount"] == 1


def test_write_domain_artifact_aggregates_bucket_sidecars(monkeypatch) -> None:
    storage: dict[str, dict] = {}
    common_storage: dict[str, dict] = {}
    fake_client = object()

    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "silver-container")
    monkeypatch.setattr(domain_artifacts.mdc, "get_storage_client", lambda _container: fake_client)
    monkeypatch.setattr(
        domain_artifacts.mdc,
        "save_json_content",
        lambda data, file_path, client=None: storage.__setitem__(str(file_path), dict(data)),
    )
    monkeypatch.setattr(
        domain_artifacts.mdc,
        "get_json_content",
        lambda file_path, client=None: storage.get(str(file_path)),
    )
    monkeypatch.setattr(
        domain_artifacts.mdc,
        "save_common_json_content",
        lambda data, file_path: common_storage.__setitem__(str(file_path), dict(data)),
    )
    monkeypatch.setattr(
        domain_artifacts.mdc,
        "get_common_json_content",
        lambda file_path: common_storage.get(str(file_path)),
    )

    domain_artifacts.write_bucket_artifact(
        layer="silver",
        domain="market",
        bucket="A",
        df=pd.DataFrame(
            {
                "symbol": ["AAPL", "AAPL"],
                "date": ["2026-01-01", "2026-01-02"],
                "close": [1.0, 2.0],
            }
        ),
        date_column="date",
    )
    domain_artifacts.write_bucket_artifact(
        layer="silver",
        domain="market",
        bucket="M",
        df=pd.DataFrame(
            {
                "symbol": ["MSFT"],
                "date": ["2026-01-03"],
                "close": [3.0],
            }
        ),
        date_column="date",
    )

    payload = domain_artifacts.write_domain_artifact(
        layer="silver",
        domain="market",
        date_column="date",
        symbol_count_override=2,
        symbol_index_path="system/silver-index/market/latest.parquet",
    )

    assert payload is not None
    assert payload["symbolCount"] == 2
    assert payload["columnCount"] == 3
    assert payload["dateRange"]["source"] == "artifact"
    assert payload["dateRange"]["min"].startswith("2026-01-01")
    assert payload["dateRange"]["max"].startswith("2026-01-03")
    assert payload["artifactPath"] == "market-data/_metadata/domain.json"
    assert "metadata/domain-metadata.json" in common_storage
    assert "metadata/ui-cache/domain-metadata-snapshot.json" in common_storage

    snapshot_doc = common_storage["metadata/domain-metadata.json"]
    snapshot_entry = snapshot_doc["entries"]["silver/market"]["metadata"]
    assert snapshot_entry["symbolCount"] == 2
    assert snapshot_entry["metadataSource"] == "artifact"

    ui_snapshot_doc = common_storage["metadata/ui-cache/domain-metadata-snapshot.json"]
    assert ui_snapshot_doc["entries"]["silver/market"]["cachedAt"]
