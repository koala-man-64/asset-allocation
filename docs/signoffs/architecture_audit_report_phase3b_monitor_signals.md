### 1. Executive Summary
- Phase 3B successfully adds runtime telemetry health signals (Azure Monitor Metrics + Log Analytics aggregates) on top of Phase 2/3A control-plane + Resource Health checks.
- The architecture remains cloud-native and low-coupling: signals are **opt-in**, bounded by explicit timeouts, and integrated into the existing TTL-cached `/system/health` response.
- Secure defaults are preserved: no raw logs are returned, and Azure resource IDs remain redacted unless explicitly enabled behind auth.
- Key risks to manage next are probe fan-out/latency as the monitored set grows and safe governance of KQL query templates.

### 2. System Map (High-Level)
- **Aggregation + API**
  - `asset_allocation/monitoring/system_health.py`: merges data-layer freshness + ARM control-plane + Resource Health + telemetry signals.
  - `asset_allocation/backtest/service/app.py`: serves `GET /system/health` with TTL cache and auth gating.
- **Telemetry probes**
  - `asset_allocation/monitoring/monitor_metrics.py`: ARM metrics (`Microsoft.Insights/metrics`) per resource.
  - `asset_allocation/monitoring/log_analytics.py`: Log Analytics query API; returns scalar aggregates only.
- **Resource representation**
  - `asset_allocation/monitoring/control_plane.py`: `ResourceHealthItem` supports optional `signals` payload.

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None identified in Phase 3B scope (signals are opt-in and aggregate-only).

#### 3.2 Major
- **[Probe fan-out/latency scaling risk]**
  - **Evidence:** metrics/log queries can add one or more calls per resource per refresh.
  - **Why it matters:** can increase `/system/health` refresh latency and risk Azure throttling as monitored resources/queries increase.
  - **Recommendation:** introduce a per-refresh time budget, bounded concurrency, and retries with jitter for 429/5xx (capped).
  - **Acceptance Criteria:** `/system/health` P95 bounded under degraded Azure APIs; partial signals return with warning alerts rather than timeout.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

- **[KQL governance / safety]**
  - **Evidence:** queries are configured via JSON env and substituted with resource names.
  - **Why it matters:** poorly-scoped queries can be expensive or risk exposing sensitive information if returned (even though this implementation returns scalar aggregates only).
  - **Recommendation:** keep queries allowlisted and aggregate-only; validate that queries return a single numeric scalar; document safe templates.
  - **Acceptance Criteria:** queries validated at startup/first run; failures yield warning alerts without returning payload rows.
  - **Owner Suggestion:** Architecture Review Agent + Project Workflow Auditor Agent

#### 3.3 Minor
- **[Metric name portability]**
  - **Evidence:** metric names vary by Azure resource/service.
  - **Recommendation:** document your chosen metric set per resource type and keep thresholds configurable.
  - **Acceptance Criteria:** environment config includes metric names and thresholds that map to meaningful degradation signals.

### 4. Architectural Recommendations
- Continue using additive optional fields (`resources[].signals`) to avoid breaking the UI contract.
- Keep secure defaults: never return raw logs; keep Azure IDs gated behind auth and explicit opt-in.
- If Phase 3 expands to many resource types, adopt a small probe registry pattern to keep aggregation maintainable.

### 5. Operational Readiness & Observability
- Add lightweight telemetry for probe duration + errors per probe type (metrics vs logs) and cache hit ratio to support tuning timeouts/TTL.
- Add “kill switches” (already present via env flags) to disable metrics/log probes without code changes.

### 6. Refactoring Examples (Targeted)
- N/A (Phase 3B is already modular with dedicated probe modules).

### 7. Evidence & Telemetry
- Files reviewed: `asset_allocation/monitoring/system_health.py`, `asset_allocation/monitoring/monitor_metrics.py`, `asset_allocation/monitoring/log_analytics.py`, `asset_allocation/monitoring/control_plane.py`.
- Tests run: `python3 -m pytest -q` → **132 passed, 3 warnings**.

