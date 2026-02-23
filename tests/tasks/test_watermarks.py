from datetime import datetime, timezone

from tasks.common import watermarks


def test_signature_matches_prefers_etag_when_present():
    prior = {"etag": "etag-1", "last_modified": "2026-01-01T00:00:00+00:00"}
    current = {"etag": "etag-2", "last_modified": "2026-01-01T00:00:00+00:00"}

    assert watermarks.signature_matches(prior, current) is False


def test_should_process_blob_since_last_success_requires_change_for_known_blob():
    blob = {
        "name": "market-data/AAPL.csv",
        "etag": "etag-1",
        "last_modified": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    }
    prior = {
        "etag": "etag-1",
        "last_modified": "2026-01-01T00:00:00+00:00",
    }
    checkpoint = datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)

    should_process = watermarks.should_process_blob_since_last_success(
        blob,
        prior_signature=prior,
        last_success_at=checkpoint,
    )
    assert should_process is False


def test_should_process_blob_since_last_success_processes_new_or_changed_blob():
    unchanged_blob = {
        "name": "market-data/AAPL.csv",
        "etag": "etag-1",
        "last_modified": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    }
    changed_blob = {
        "name": "market-data/AAPL.csv",
        "etag": "etag-2",
        "last_modified": datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc),
    }
    prior = {
        "etag": "etag-1",
        "last_modified": "2026-01-01T00:00:00+00:00",
    }
    checkpoint = datetime(2026, 1, 2, 0, 0, tzinfo=timezone.utc)

    assert (
        watermarks.should_process_blob_since_last_success(
            unchanged_blob,
            prior_signature=None,
            last_success_at=checkpoint,
        )
        is True
    )
    assert (
        watermarks.should_process_blob_since_last_success(
            changed_blob,
            prior_signature=prior,
            last_success_at=checkpoint,
        )
        is True
    )
    assert (
        watermarks.should_process_blob_since_last_success(
            unchanged_blob,
            prior_signature=prior,
            last_success_at=checkpoint,
            force_reprocess=True,
        )
        is True
    )


def test_load_and_save_last_success(monkeypatch):
    saved = {}

    monkeypatch.setattr(watermarks, "_is_enabled", lambda: True)
    monkeypatch.setattr(
        watermarks.mdc,
        "get_common_json_content",
        lambda _path: {"last_success": "2026-01-31T00:00:00+00:00"},
    )
    monkeypatch.setattr(
        watermarks.mdc,
        "save_common_json_content",
        lambda payload, path: saved.update({"payload": payload, "path": path}),
    )

    loaded = watermarks.load_last_success("silver_market_data")
    assert loaded == datetime(2026, 1, 31, 0, 0, tzinfo=timezone.utc)

    watermarks.save_last_success("silver_market_data")
    assert saved["path"].endswith("system/watermarks/runs/silver_market_data.json")
    assert "last_success" in saved["payload"]
