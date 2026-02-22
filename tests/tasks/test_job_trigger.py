from __future__ import annotations

import pytest

from tasks.common import job_trigger


def test_ensure_api_awake_skips_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("JOB_STARTUP_API_WAKE_ENABLED", "false")

    def _unexpected_probe(**_kwargs):
        raise AssertionError("health probe should not run when startup wake is disabled")

    monkeypatch.setattr(job_trigger, "_probe_health", _unexpected_probe)

    job_trigger.ensure_api_awake_from_env(required=True)


def test_ensure_api_awake_raises_when_required_and_base_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ASSET_ALLOCATION_API_BASE_URL", raising=False)
    monkeypatch.delenv("ASSET_ALLOCATION_API_URL", raising=False)

    with pytest.raises(RuntimeError, match="ASSET_ALLOCATION_API_BASE_URL"):
        job_trigger.ensure_api_awake_from_env(required=True)


def test_ensure_api_awake_starts_container_app_and_recovers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ASSET_ALLOCATION_API_BASE_URL", "http://asset-allocation-api")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID", "sub")
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_RESOURCE_GROUP", "rg")
    monkeypatch.setenv("JOB_STARTUP_API_WAKE_ENABLED", "true")

    probes = iter(
        [
            (False, "status=503"),
            (True, "status=200"),
        ]
    )

    monkeypatch.setattr(
        job_trigger,
        "_probe_health",
        lambda **_kwargs: next(probes),
    )
    monkeypatch.setattr(job_trigger.time, "sleep", lambda _seconds: None)

    start_calls: list[tuple[str, bool]] = []

    def _fake_start(*, app_name: str, cfg: job_trigger.ArmConfig, required: bool = True) -> bool:
        assert cfg.subscription_id == "sub"
        assert cfg.resource_group == "rg"
        start_calls.append((app_name, required))
        return True

    monkeypatch.setattr(job_trigger, "_start_container_app", _fake_start)

    job_trigger.ensure_api_awake_from_env(required=True)

    assert start_calls == [("asset-allocation-api", True)]


def test_resolve_startup_container_apps_matches_allowlist_from_domain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("JOB_STARTUP_API_CONTAINER_APPS", raising=False)
    monkeypatch.delenv("API_CONTAINER_APP_NAME", raising=False)
    monkeypatch.setenv("SYSTEM_HEALTH_ARM_CONTAINERAPPS", "asset-allocation-api,asset-allocation-ui")

    resolved = job_trigger._resolve_startup_container_apps("https://asset-allocation-api.internal.azurecontainerapps.io")
    assert resolved == ["asset-allocation-api"]
