### 1. Executive Summary
- Phase 3 should extend the current `/system/health` design from “data freshness + ARM provisioning state” into “runtime health + operator workflow” by adding Azure Monitor signals and durable alert acknowledgement.
- The existing monitoring layer (`asset_allocation/monitoring/*`) is an appropriate boundary; Phase 3 should continue the additive, env-driven approach with strict timeouts, caching, and secure defaults.
- The biggest architectural risks for Phase 3 are (1) probe latency/throttling impacting request paths, and (2) multi-replica consistency for alert acknowledgement.
- Near-term priorities: introduce probe telemetry (latency/error counters), add stable alert IDs + persisted ack state, and add runtime signals (Resource Health + Monitor metrics/logs) behind feature flags.

### 2. System Map (High-Level)
- **API surface**
  - `GET /system/health` in `asset_allocation/backtest/service/app.py` (TTL-cached, auth-gated for sensitive fields).
- **Monitoring layer**
  - Aggregator: `asset_allocation/monitoring/system_health.py`
  - Control-plane probes: `asset_allocation/monitoring/control_plane.py` + `asset_allocation/monitoring/arm_client.py`
  - Data-plane probes: `asset_allocation/monitoring/azure_blob_store.py`
- **UI**
  - Types: `asset_allocation/ui2.0/src/types/strategy.ts`
  - View: `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx`

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- **[Alert acknowledgement requires durable identity + storage]**
  - **Evidence:** alerts are computed per-snapshot and currently lack stable IDs/persistence (`asset_allocation/monitoring/system_health.py`).
  - **Why it matters:** acknowledgement cannot be reliable across refreshes/restarts; operators can’t suppress known issues; alert history becomes noisy.
  - **Recommendation:** introduce deterministic `alert.id` + ack persistence (shared store) + auth-required ack endpoints.
  - **Acceptance Criteria:** ack state persists across restarts and across replicas; ack endpoints require auth; stable ID is deterministic across snapshots.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

#### 3.2 Major
- **[Control-plane “Succeeded” does not imply runtime health]**
  - **Evidence:** Phase 2 uses provisioning state + executions list; no runtime error-rate/latency signals.
  - **Why it matters:** services can be “Succeeded” while failing requests or crash-looping; operators get false positives.
  - **Recommendation:** add Azure Resource Health + Azure Monitor (metrics + logs) probes and incorporate into `resources[].status` derivation.
  - **Acceptance Criteria:** a resource can degrade/critical due to runtime signals even when provisioning is Succeeded.
  - **Owner Suggestion:** Architecture Review Agent + Delivery Engineer Agent

- **[Request-path probe coupling can amplify latency/throttling]**
  - **Evidence:** probes run on cache refresh on-demand (`asset_allocation/backtest/service/app.py`).
  - **Why it matters:** slow/throttled Azure APIs can increase `/system/health` latency and cause cascading failures.
  - **Recommendation:** add background refresh loop (optional) and bounded concurrency; implement retry/backoff for 429/5xx with strict overall time budget.
  - **Acceptance Criteria:** `/system/health` P95 remains bounded with external dependency degradation; Azure API calls remain within rate limits.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

#### 3.3 Minor
- **[Phase 3 will increase env-var surface area]**
  - **Evidence:** Phase 2 already adds multiple env vars; Phase 3 will add Monitor/Log Analytics settings.
  - **Why it matters:** misconfiguration can silently reduce coverage.
  - **Recommendation:** group config under clear prefixes; validate required combinations; document a minimal “enabled” checklist.
  - **Acceptance Criteria:** invalid configs produce a clear warning alert; docs include required keys and examples.
  - **Owner Suggestion:** Project Workflow Auditor Agent

### 4. Architectural Recommendations
- Keep `SystemHealth` as the stable contract; extend additively with optional fields:
  - `resources[].signals?: ResourceSignal[]` (metrics/log-derived signals)
  - `alerts[].id` (stable) and `alerts[].acknowledged` backed by persistence
- Prefer Managed Identity everywhere (no secrets); minimize new SDK dependencies unless they materially reduce complexity.
- Introduce a small “probe registry” pattern to prevent `system_health.py` from growing into a monolith as more Azure services are added.

### 5. Operational Readiness & Observability
- Add probe telemetry:
  - cache hit ratio, refresh duration, refresh failure counters
  - ARM/Monitor/Log Analytics request latency + error counters (by probe type)
- Add runbook notes: “disable Monitor probes” via env flags; alert on sustained probe failure vs real resource degradation.

### 6. Refactoring Examples (Targeted)
- Stable alert IDs (example approach): hash normalized alert fields (component + title + severity + stable discriminator).

### 7. Evidence & Telemetry
- Files reviewed for Phase 3 planning: monitoring layer + API endpoint + UI status page + CI workflow for UI build/test.

