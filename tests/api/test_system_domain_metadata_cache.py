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


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_returns_filtered_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {
            "version": 1,
            "updatedAt": "2026-02-20T12:00:00+00:00",
            "entries": {
                "bronze/market": {
                    "layer": "bronze",
                    "domain": "market",
                    "cachedAt": "2026-02-20T11:59:00+00:00",
                    "metadata": _metadata_payload(layer="bronze", domain="market"),
                },
                "silver/finance": {
                    "layer": "silver",
                    "domain": "finance",
                    "cachedAt": "2026-02-20T11:58:00+00:00",
                    "metadata": _metadata_payload(layer="silver", domain="finance"),
                },
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/system/domain-metadata/snapshot?layers=bronze&domains=market,finance&cacheOnly=true"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["version"] == 1
    assert payload["updatedAt"] == "2026-02-20T12:00:00+00:00"
    assert sorted(payload["entries"].keys()) == ["bronze/market"]
    entry = payload["entries"]["bronze/market"]
    assert entry["layer"] == "bronze"
    assert entry["domain"] == "market"
    assert entry["cacheSource"] == "snapshot"
    assert response.headers.get("X-Domain-Metadata-Source") == "snapshot-batch"
    assert response.headers.get("X-Domain-Metadata-Entry-Count") == "1"


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_can_warm_fill_missing_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {"version": 1, "updatedAt": None, "entries": {}},
    )

    monkeypatch.setattr(
        system,
        "collect_domain_metadata",
        lambda **kwargs: _metadata_payload(layer=str(kwargs.get("layer")), domain=str(kwargs.get("domain"))),
    )

    writes: list[tuple[str, str]] = []

    def _write(layer: str, domain: str, metadata: dict[str, object]) -> str:
        writes.append((layer, domain))
        return "2026-02-20T13:00:00+00:00"

    monkeypatch.setattr(system, "_write_cached_domain_metadata_snapshot", _write)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get(
            "/api/system/domain-metadata/snapshot?layers=bronze&domains=market&cacheOnly=false"
        )

    assert response.status_code == 200
    payload = response.json()
    assert writes == [("bronze", "market")]
    assert sorted(payload["entries"].keys()) == ["bronze/market"]
    entry = payload["entries"]["bronze/market"]
    assert entry["layer"] == "bronze"
    assert entry["domain"] == "market"
    assert entry["cacheSource"] == "snapshot"
    assert entry["cachedAt"] == "2026-02-20T13:00:00+00:00"


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_rejects_invalid_layer_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot?layers=invalid-layer")

    assert response.status_code == 400
    assert "layers contains unsupported value" in response.json().get("detail", "")


@pytest.mark.asyncio
async def test_domain_metadata_snapshot_returns_304_when_etag_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(
        system,
        "_load_domain_metadata_document",
        lambda force_refresh=False: {
            "version": 1,
            "updatedAt": "2026-02-20T12:00:00+00:00",
            "entries": {
                "bronze/market": {
                    "layer": "bronze",
                    "domain": "market",
                    "cachedAt": "2026-02-20T11:59:00+00:00",
                    "metadata": _metadata_payload(layer="bronze", domain="market"),
                }
            },
        },
    )

    app = create_app()
    async with get_test_client(app) as client:
        first = await client.get("/api/system/domain-metadata/snapshot?layers=bronze&domains=market")
        etag = first.headers.get("ETag")
        assert etag
        second = await client.get(
            "/api/system/domain-metadata/snapshot?layers=bronze&domains=market",
            headers={"If-None-Match": etag},
        )

    assert first.status_code == 200
    assert second.status_code == 304
    assert second.text == ""


@pytest.mark.asyncio
async def test_persisted_ui_domain_metadata_cache_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    captured: dict[str, object] = {}

    monkeypatch.setattr(system, "_domain_metadata_ui_cache_path", lambda: "metadata/ui-cache/domain.json")
    monkeypatch.setattr(system, "_utc_timestamp", lambda: "2026-02-20T14:00:00+00:00")
    monkeypatch.setattr(
        system.mdc,
        "save_common_json_content",
        lambda data, path: captured.update({"payload": data, "path": path}),
    )
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: captured.get("payload"))

    app = create_app()
    async with get_test_client(app) as client:
        write_response = await client.put(
            "/api/system/domain-metadata/snapshot/cache",
            json={
                "version": 1,
                "updatedAt": None,
                "entries": {
                    "bronze/market": {
                        **_metadata_payload(layer="bronze", domain="market"),
                        "cacheSource": "snapshot",
                    }
                },
                "warnings": [],
            },
        )
        read_response = await client.get("/api/system/domain-metadata/snapshot/cache")

    assert write_response.status_code == 200
    assert captured["path"] == "metadata/ui-cache/domain.json"
    written = write_response.json()
    assert written["updatedAt"] == "2026-02-20T14:00:00+00:00"
    assert read_response.status_code == 200
    assert sorted(read_response.json()["entries"].keys()) == ["bronze/market"]
    assert read_response.headers.get("X-Domain-Metadata-UI-Cache") == "hit"


@pytest.mark.asyncio
async def test_persisted_ui_domain_metadata_cache_miss_returns_empty_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_AUTH_MODE", "none")
    monkeypatch.setattr(system.mdc, "get_common_json_content", lambda path: None)

    app = create_app()
    async with get_test_client(app) as client:
        response = await client.get("/api/system/domain-metadata/snapshot/cache")

    assert response.status_code == 200
    payload = response.json()
    assert payload["entries"] == {}
    assert response.headers.get("X-Domain-Metadata-UI-Cache") == "miss"
