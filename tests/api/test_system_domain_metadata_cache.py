from __future__ import annotations

import pytest

from api.endpoints import system
from api.service.app import create_app
from tests.api._client import get_test_client


def _metadata_payload(*, layer: str, domain: str) -> dict[str, object]:
    return {
        "layer": layer,
        "domain": domain,
        "container": f"{layer}-container",
        "type": "blob",
        "computedAt": "2026-02-20T00:00:00+00:00",
        "symbolCount": 101,
        "warnings": [],
    }


def test_write_and_read_cached_domain_metadata_snapshot_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setattr(system, "_domain_metadata_cache_path", lambda: "metadata/domain-metadata.json")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-20T12:34:56+00:00")
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: {"version": 1, "entries": {}})

    def _save(payload: dict[str, object], file_path: str) -> None:
        captured["payload"] = payload
        captured["file_path"] = file_path

    monkeypatch.setattr(system.mdc, "save_common_json_content", _save)

    cached_at = system._write_cached_domain_metadata_snapshot(
        "silver",
        "market",
        _metadata_payload(layer="silver", domain="market"),
    )

    assert cached_at == "2026-02-20T12:34:56+00:00"
    assert captured["file_path"] == "metadata/domain-metadata.json"

    persisted = captured["payload"]
    assert isinstance(persisted, dict)
    entries = persisted.get("entries")
    assert isinstance(entries, dict)
    entry = entries.get("silver/market")
    assert isinstance(entry, dict)
    assert entry.get("cachedAt") == cached_at

    history = entry.get("history")
    assert isinstance(history, list)
    assert history[-1].get("symbolCount") == 101

    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: persisted)
    cached_payload = system._read_cached_domain_metadata_snapshot("silver", "market")

    assert isinstance(cached_payload, dict)
    assert cached_payload["layer"] == "silver"
    assert cached_payload["domain"] == "market"
    assert cached_payload["cachedAt"] == cached_at
    assert cached_payload["cacheSource"] == "snapshot"
    assert cached_payload["symbolCount"] == 101


@pytest.mark.asyncio
async def test_domain_metadata_cache_only_returns_cached_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(
        system,
        "_read_cached_domain_metadata_snapshot",
        lambda layer, domain: {
            **_metadata_payload(layer=layer, domain=domain),
            "cachedAt": "2026-02-20T12:00:00+00:00",
            "cacheSource": "snapshot",
        },
    )
    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("collect_domain_metadata should not run")),
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=bronze&domain=market&cacheOnly=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "bronze"
    assert payload["domain"] == "market"
    assert payload["cacheSource"] == "snapshot"
    assert payload["cachedAt"] == "2026-02-20T12:00:00+00:00"
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot"


@pytest.mark.asyncio
async def test_domain_metadata_refresh_writes_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: _metadata_payload(layer=str(kwargs.get("layer")), domain=str(kwargs.get("domain"))),
    )

    def _write(layer: str, domain: str, metadata: dict[str, object]) -> str:
        captured["layer"] = layer
        captured["domain"] = domain
        captured["metadata"] = dict(metadata)
        return "2026-02-20T13:00:00+00:00"

    monkeypatch.setattr(system, "_write_cached_domain_metadata_snapshot", _write)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=gold&domain=finance&refresh=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "gold"
    assert payload["domain"] == "finance"
    assert payload["cacheSource"] == "live-refresh"
    assert payload["cachedAt"] == "2026-02-20T13:00:00+00:00"

    assert captured["layer"] == "gold"
    assert captured["domain"] == "finance"
    assert isinstance(captured["metadata"], dict)
    assert captured["metadata"].get("cacheSource") == "live-refresh"


@pytest.mark.asyncio
async def test_domain_metadata_cache_only_miss_returns_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(system, "_read_cached_domain_metadata_snapshot", lambda layer, domain: None)
    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("collect_domain_metadata should not run")),
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata?layer=bronze&domain=market&cacheOnly=true")

    assert response.status_code == 200
    payload = response.json()
    assert payload["layer"] == "bronze"
    assert payload["domain"] == "market"
    assert payload["cacheSource"] == "snapshot"
    assert payload["symbolCount"] is None
    assert payload["warnings"]
    assert "No cached domain metadata snapshot found" in payload["warnings"][0]
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot-miss"
    assert response.headers.get("X-Domain-Metadata-Cache-Miss") == "1"
