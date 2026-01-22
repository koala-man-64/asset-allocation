# Implementation Report

## 1. Execution Summary
- Produced a Phase 3 implementation plan to add **runtime Azure health signals** (Resource Health + Azure Monitor metrics/logs) and **durable alert acknowledgement** to the existing `/system/health` pipeline.
- No Phase 3 production code was implemented in this work item; this is a delivery-ready plan intended for parallel execution.

**Out of scope**
- Full incident management workflows (paging/notifications).
- Deep log retrieval per execution and full trace correlation (beyond Phase 3 signal summaries).

## 2. Architectural Alignment Matrix
- **Requirement:** “Report Azure runtime health to the UI.”
  - **Implementation (planned):** `asset_allocation/monitoring/resource_health.py`, `asset_allocation/monitoring/monitor_metrics.py`, `asset_allocation/monitoring/log_analytics.py`
  - **Status:** Planned
  - **Notes:** Use Managed Identity; bounded timeouts; cached results.

- **Requirement:** “Operators can acknowledge alerts.”
  - **Implementation (planned):** `asset_allocation/monitoring/alert_store.py` + new API endpoints in `asset_allocation/backtest/service/app.py`
  - **Status:** Planned
  - **Notes:** Stable `alert.id`; shared persistence (avoid per-replica state).

- **Requirement:** “Maintain secure defaults.”
  - **Implementation (planned):** keep Azure IDs redacted unless explicit opt-in behind auth; avoid logging sensitive payloads.
  - **Status:** Planned

## 3. Change Set
**Added (planned)**
- `asset_allocation/monitoring/resource_health.py` (AvailabilityStatuses probe)
- `asset_allocation/monitoring/monitor_metrics.py` (Azure Monitor metrics probe)
- `asset_allocation/monitoring/log_analytics.py` (Log Analytics query probe)
- `asset_allocation/monitoring/alert_store.py` (ack persistence interface + implementations)
- `tests/monitoring/test_phase3_runtime_signals.py`
- `tests/monitoring/test_alert_ack_store.py`

**Modified (planned)**
- `asset_allocation/monitoring/system_health.py` (merge runtime signals + stable alert IDs)
- `asset_allocation/backtest/service/app.py` (ack endpoints + auth gate)
- `asset_allocation/ui2.0/src/types/strategy.ts` (add `alert.id`; optional `resources[].signals`)
- `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx` (ack/unack UX; signal display)
- `.env.template` (Phase 3 env vars)

**Key Interfaces (planned)**
- `GET /system/health` (extends existing payload)
  - `alerts[].id: string` (stable)
  - `resources[].signals?: { name, value, unit, timestamp, status }[]`
- `POST /system/alerts/{id}/ack`
- `POST /system/alerts/{id}/unack`

## 4. Code Implementation
No code changes included in this work item (plan-only).

## 5. Observability & Operational Readiness
- Add metrics/logging around:
  - cache refresh duration + errors
  - per-probe latency + failures (ARM / ResourceHealth / Metrics / Logs)
- Add “kill switches” (env flags) to disable Monitor/Logs probes independently.

## 6. Cloud-Native Configuration (If applicable)
- Required Phase 3 configuration (planned):
  - Log Analytics workspace ID(s) and a small set of vetted KQL queries (no secrets).
  - Per-probe timeouts and enablement flags.
  - Managed Identity RBAC: Reader on RG + permissions for Log Analytics query (workspace-level).

## 7. Verification Steps
- Local: `python3 -m pytest -q`
- Focused: `python3 -m pytest -q tests/monitoring/`
- UI: rely on existing CI job that runs `pnpm exec vitest run` + `pnpm build` for ui2.0.

## 8. Risks & Follow-ups
- Multi-replica alert ack consistency requires shared storage; do not use local sqlite unless single-replica is guaranteed.
- Monitor/Log Analytics APIs can throttle; require strict time budgets and bounded retries.

## 9. Evidence & Telemetry
- Plan created based on current Phase 1/2 monitoring integration points and existing CI workflows.

