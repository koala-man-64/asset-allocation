from __future__ import annotations

from datetime import datetime, timedelta, timezone

from monitoring import system_health
from monitoring.azure_blob_store import LastModifiedProbeResult


class _DummyStore:
    def __init__(self, *, marker_last_modified: datetime | None = None, marker_error: Exception | None = None):
        self._marker_last_modified = marker_last_modified
        self._marker_error = marker_error

    def get_blob_last_modified(self, *, container: str, blob_name: str) -> datetime | None:
        del container
        del blob_name
        if self._marker_error is not None:
            raise self._marker_error
        return self._marker_last_modified


def _marker_cfg(
    *,
    enabled: bool = True,
    fallback_to_legacy: bool = True,
    dual_read: bool = False,
) -> system_health.MarkerProbeConfig:
    return system_health.MarkerProbeConfig(
        enabled=enabled,
        container="common",
        prefix="system/health_markers",
        fallback_to_legacy=fallback_to_legacy,
        dual_read=dual_read,
        dual_read_tolerance_seconds=21600,
    )


def test_compute_layer_status_boundary_conditions() -> None:
    now = datetime(2026, 2, 16, 12, 0, tzinfo=timezone.utc)
    at_boundary = now - timedelta(seconds=60)

    assert (
        system_health._compute_layer_status(
            now,
            at_boundary,
            max_age_seconds=60,
            had_error=False,
        )
        == "healthy"
    )
    assert (
        system_health._compute_layer_status(
            now,
            at_boundary,
            max_age_seconds=59,
            had_error=False,
        )
        == "stale"
    )
    assert (
        system_health._compute_layer_status(
            now,
            None,
            max_age_seconds=60,
            had_error=False,
        )
        == "stale"
    )


def test_resolve_freshness_policy_uses_domain_override() -> None:
    policy = system_health._resolve_freshness_policy(
        layer_name="Silver",
        domain_name="market",
        default_max_age_seconds=129600,
        overrides={"silver.market": {"maxAgeSeconds": 43200}},
    )
    assert policy.max_age_seconds == 43200
    assert policy.source == "override:silver.market"


def test_resolve_freshness_policy_falls_back_to_default() -> None:
    policy = system_health._resolve_freshness_policy(
        layer_name="Gold",
        domain_name="earnings",
        default_max_age_seconds=129600,
        overrides={},
    )
    assert policy.max_age_seconds == 129600
    assert policy.source == "default"


def test_marker_probe_uses_marker_without_legacy_when_dual_read_disabled() -> None:
    marker_time = datetime(2026, 2, 16, 10, 0, tzinfo=timezone.utc)
    store = _DummyStore(marker_last_modified=marker_time)
    calls = {"legacy": 0}

    def legacy_probe() -> LastModifiedProbeResult:
        calls["legacy"] += 1
        return LastModifiedProbeResult(state="ok", last_modified=marker_time - timedelta(hours=1))

    resolved = system_health._resolve_last_updated_with_marker_fallback(
        layer_name="Silver",
        domain_name="market",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True, fallback_to_legacy=True, dual_read=False),
        legacy_source="legacy-prefix",
        legacy_probe_fn=legacy_probe,
    )

    assert resolved.status == "ok"
    assert resolved.source == "marker"
    assert resolved.last_updated == marker_time
    assert calls["legacy"] == 0


def test_marker_missing_falls_back_to_legacy_probe() -> None:
    legacy_time = datetime(2026, 2, 16, 9, 0, tzinfo=timezone.utc)
    store = _DummyStore(marker_last_modified=None)

    resolved = system_health._resolve_last_updated_with_marker_fallback(
        layer_name="Silver",
        domain_name="finance",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True, fallback_to_legacy=True, dual_read=False),
        legacy_source="legacy-prefix",
        legacy_probe_fn=lambda: LastModifiedProbeResult(state="ok", last_modified=legacy_time),
    )

    assert resolved.status == "ok"
    assert resolved.source == "legacy-prefix"
    assert resolved.last_updated == legacy_time


def test_probe_error_is_error_when_marker_fallback_disabled() -> None:
    store = _DummyStore(marker_error=RuntimeError("403 Forbidden"))
    calls = {"legacy": 0}

    def legacy_probe() -> LastModifiedProbeResult:
        calls["legacy"] += 1
        return LastModifiedProbeResult(state="ok", last_modified=None)

    resolved = system_health._resolve_last_updated_with_marker_fallback(
        layer_name="Bronze",
        domain_name="earnings",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=True, fallback_to_legacy=False, dual_read=False),
        legacy_source="legacy-prefix",
        legacy_probe_fn=legacy_probe,
    )

    assert resolved.status == "error"
    assert "403 Forbidden" in str(resolved.error)
    assert calls["legacy"] == 0


def test_legacy_probe_error_surfaces_as_error_when_marker_disabled() -> None:
    store = _DummyStore(marker_last_modified=None)

    resolved = system_health._resolve_last_updated_with_marker_fallback(
        layer_name="Bronze",
        domain_name="price-target",
        store=store,  # type: ignore[arg-type]
        marker_cfg=_marker_cfg(enabled=False, fallback_to_legacy=True, dual_read=False),
        legacy_source="legacy-prefix",
        legacy_probe_fn=lambda: LastModifiedProbeResult(state="error", error="AuthenticationFailed"),
    )

    assert resolved.status == "error"
    assert "AuthenticationFailed" in str(resolved.error)


def test_resolve_domain_schedule_uses_manual_trigger_metadata() -> None:
    cron, frequency = system_health._resolve_domain_schedule(
        job_name="silver-market-job",
        default_cron="30 14-23 * * *",
        job_schedule_metadata={
            "silver-market-job": system_health.JobScheduleMetadata(
                trigger_type="manual",
                cron_expression="",
            )
        },
    )
    assert cron == ""
    assert frequency == "Manual trigger"
