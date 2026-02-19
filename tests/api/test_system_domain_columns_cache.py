from __future__ import annotations

import time

import pytest

from api import data_service
from api.endpoints import system


def test_read_cached_domain_columns_returns_empty_when_missing(monkeypatch) -> None:
    monkeypatch.setattr(system, "_domain_columns_cache_path", lambda: "metadata/domain-columns.json")
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: {"version": 1, "entries": {}})

    columns, updated_at, found = system._read_cached_domain_columns("silver", "market")

    assert columns == []
    assert updated_at is None
    assert found is False


def test_write_cached_domain_columns_persists_common_cache(monkeypatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(system, "_domain_columns_cache_path", lambda: "metadata/domain-columns.json")
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: {"version": 1, "entries": {}})

    def _save(payload, file_path):
        captured["payload"] = payload
        captured["file_path"] = file_path

    monkeypatch.setattr(system.mdc, "save_common_json_content", _save)

    columns, updated_at = system._write_cached_domain_columns(
        layer="silver",
        domain="market",
        columns=["Close", "Volume", "Close"],
    )

    assert columns == ["Close", "Volume"]
    assert isinstance(updated_at, str)
    assert captured["file_path"] == "metadata/domain-columns.json"

    payload = captured["payload"]
    assert isinstance(payload, dict)
    entries = payload.get("entries")
    assert isinstance(entries, dict)
    entry = entries.get("silver/market")
    assert isinstance(entry, dict)
    assert entry.get("columns") == ["Close", "Volume"]
    assert entry.get("updatedAt") == updated_at


def test_retrieve_domain_columns_uses_first_non_empty_row(monkeypatch) -> None:
    monkeypatch.setattr(
        data_service.DataService,
        "get_data",
        lambda layer, domain, ticker=None, limit=None: [{}, {"Close": 0.5, "Volume": 10}],
    )

    columns = system._retrieve_domain_columns("silver", "market", 500)

    assert columns == ["Close", "Volume"]


def test_retrieve_domain_columns_prefers_schema_first(monkeypatch) -> None:
    monkeypatch.setattr(
        system,
        "_retrieve_domain_columns_from_schema",
        lambda layer, domain: ["Close", "Volume", "Symbol"],
    )

    def _unexpected_get_data(*args, **kwargs):
        raise AssertionError("sampled DataService.get_data should not be called when schema columns are available")

    monkeypatch.setattr(data_service.DataService, "get_data", _unexpected_get_data)

    columns = system._retrieve_domain_columns("silver", "market", 500)
    assert columns == ["Close", "Volume", "Symbol"]


def test_run_with_timeout_raises_timeout_error() -> None:
    with pytest.raises(TimeoutError):
        system._run_with_timeout(
            lambda: time.sleep(0.05),
            timeout_seconds=0.01,
            timeout_message="timed out",
        )
