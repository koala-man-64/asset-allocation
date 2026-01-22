### 1. Executive Summary
- Phase 3A extends the monitoring layer from ARM provisioning state to include **runtime availability** via Azure Resource Health, improving correctness for “Succeeded but unhealthy” cases.
- The integration is additive and safe-by-default: it is opt-in via env flag and does not change public API shape (only enriches `resources[].details`/`status`).
- Primary risks are external API latency/throttling and ambiguity when Resource Health returns `Unknown`; both are mitigated by best-effort behavior and caching already present at the endpoint.
- Near-term priority: add lightweight probe telemetry (latency/errors) before adding Azure Monitor metrics/logs.

### 2. System Map (High-Level)
- `asset_allocation/monitoring/resource_health.py`: calls `Microsoft.ResourceHealth/availabilityStatuses/current` for a resource ID and maps availability to `healthy|warning|error|unknown`.
- `asset_allocation/monitoring/control_plane.py`: merges provisioning-state status with availability-derived status using a worst-of mapping.
- `asset_allocation/monitoring/system_health.py`: env-driven enablement and aggregation; escalates `overall` based on warning/error resources.
- `asset_allocation/backtest/service/app.py`: serves `/system/health` with TTL cache and redaction gate for verbose IDs.

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None identified in Phase 3A scope (opt-in, best-effort, additive).

#### 3.2 Major
- **[Probe latency and throttling could impact refresh cycles]**
  - **Evidence:** Resource Health adds one extra Azure call per monitored resource when enabled.
  - **Why it matters:** Could increase refresh time and Azure API pressure as the monitored resource list grows.
  - **Recommendation:** Keep strict timeouts (already present) and consider bounded concurrency/retries if adding more probe types in Phase 3B.
  - **Acceptance Criteria:** `/system/health` remains bounded under degraded Azure API conditions; probe failures surface as alerts rather than timeouts.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

#### 3.3 Minor
- **[Unknown availability state does not currently degrade overall]**
  - **Evidence:** `unknown` signals do not produce an alert or `overall` degradation unless mapped to warning/error.
  - **Recommendation:** Evaluate whether repeated unknowns should produce a warning alert (to highlight monitoring blind spots) in Phase 3B.
  - **Acceptance Criteria:** Operators can distinguish “resource unhealthy” vs “monitoring incomplete.”
  - **Owner Suggestion:** Architecture Review Agent

### 4. Architectural Recommendations
- Keep Resource Health opt-in and best-effort; do not fail `/system/health` due to probe errors.
- Continue to treat Azure IDs as sensitive-ish metadata: use internally, redact externally unless explicitly enabled behind auth.
- If Phase 3 expands signals, adopt a small probe registry pattern to keep `system_health.py` from growing monolithic.

### 5. Operational Readiness & Observability
- Add probe telemetry (Phase 3B): latency histogram + failure counters per probe type, and cache hit ratio.

### 6. Refactoring Examples (Targeted)
- N/A (Phase 3A is small and already modular).

### 7. Evidence & Telemetry
- Files reviewed: `asset_allocation/monitoring/resource_health.py`, `asset_allocation/monitoring/control_plane.py`, `asset_allocation/monitoring/system_health.py`.
- Tests run: `python3 -m pytest -q` → **125 passed, 3 warnings**.

