# Implementation Report

## 1. Execution Summary
- Implemented Phase 3B runtime telemetry signals for Azure resources using:
  - Azure Monitor **Metrics** (ARM `Microsoft.Insights/metrics`)
  - Azure **Log Analytics** (KQL aggregates only; no raw logs returned)
- Integrated these signals into the existing `/system/health` aggregation so `resources[].status/details` can degrade based on metrics/log thresholds (while preserving secure defaults and TTL caching).
- Added hermetic tests for metrics parsing, Log Analytics parsing, and end-to-end aggregation with fakes (no Azure calls).

**Out of scope**
- Persisted alert acknowledgement state and ack/unack APIs (explicitly deprioritized).
- Deep log retrieval or returning raw log lines to the UI.

## 2. Architectural Alignment Matrix
- **Requirement:** “Phase 3B: add Azure Monitor metrics + Log Analytics runtime signals.”
  - **Implementation:** `asset_allocation/monitoring/monitor_metrics.py`, `asset_allocation/monitoring/log_analytics.py`, `asset_allocation/monitoring/system_health.py`
  - **Status:** Complete
  - **Notes:** Signals are opt-in and bounded by timeouts; failures degrade gracefully.

- **Requirement:** “Secure defaults; do not leak sensitive payloads.”
  - **Implementation:** Log Analytics returns only scalar aggregates; no raw logs returned; Azure resource IDs remain gated behind `SYSTEM_HEALTH_VERBOSE_IDS` and service auth.
  - **Status:** Complete

- **Requirement:** “UI compatibility.”
  - **Implementation:** No breaking contract changes; signals are returned as an optional `resources[].signals` list and/or summarized into `resources[].details`.
  - **Status:** Complete

## 3. Change Set
**Added**
- `asset_allocation/monitoring/monitor_metrics.py`
- `asset_allocation/monitoring/log_analytics.py`
- `tests/monitoring/test_monitor_metrics.py`
- `tests/monitoring/test_log_analytics.py`
- `tests/monitoring/test_phase3b_signals.py`
- `docs/signoffs/audit_snapshot_phase3b_monitor_signals.json`
- `docs/signoffs/implementation_report_phase3b_monitor_signals.md`

**Modified**
- `asset_allocation/monitoring/system_health.py`
- `asset_allocation/monitoring/control_plane.py` (resource payload can now include `signals`)
- `asset_allocation/ui2.0/src/types/strategy.ts`
- `.env.template`

**Key Interfaces**
- API: `GET /system/health`
  - `resources[].signals?: { name, value, unit, timestamp, status, source }[]`
- Env vars:
  - Metrics: `SYSTEM_HEALTH_MONITOR_METRICS_*`
  - Logs: `SYSTEM_HEALTH_LOG_ANALYTICS_*`

## 4. Code Implementation
### Metrics probe (ARM)
```python
# asset_allocation/monitoring/monitor_metrics.py
url = f"https://management.azure.com{resource_id}/providers/microsoft.insights/metrics"
payload = arm.get_json(url, params={"metricnames": "...", "timespan": "...", "aggregation": "Average", ...})
```

### Log Analytics probe (aggregate-only)
```python
# asset_allocation/monitoring/log_analytics.py
POST https://api.loganalytics.io/v1/workspaces/{workspaceId}/query  { "query": "<KQL>", "timespan": "<start/end>" }
```

## 5. Observability & Operational Readiness
- All telemetry probes are opt-in via env flags and run within the existing `/system/health` TTL caching model.
- Failure modes:
  - If Azure APIs fail/timeout, signals become `unknown` and do not crash `/system/health`; resource degradation only occurs when signals are available and exceed thresholds.
- Recommended follow-up: add probe latency/error metrics and a global per-refresh time budget when the monitored surface area grows.

## 6. Cloud-Native Configuration (If applicable)
- Required permissions (typical):
  - Metrics: Reader on the resource group (to read metrics endpoint)
  - Log Analytics: Log Analytics Reader on the workspace
- Configuration is entirely via env vars (see `.env.template`).

## 7. Verification Steps
- Full suite: `python3 -m pytest -q`
- Monitoring focus: `python3 -m pytest -q tests/monitoring/test_monitor_metrics.py tests/monitoring/test_log_analytics.py tests/monitoring/test_phase3b_signals.py`

## 8. Risks & Follow-ups
- Metric names are resource/service dependent; configuration must specify metric names and thresholds appropriate to your environment.
- Log Analytics KQL is powerful; keep queries allowlisted and aggregate-only to avoid accidental data exposure.

## 9. Evidence & Telemetry
- `python3 -m pytest -q` → **132 passed, 3 warnings**
- `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out docs/signoffs/audit_snapshot_phase3b_monitor_signals.json` → wrote snapshot

