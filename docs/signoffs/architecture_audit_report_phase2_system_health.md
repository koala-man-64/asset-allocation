### 1. Executive Summary
- Phase 2 introduces a clean, additive “monitoring” layer that expands `/system/health` from data-plane freshness (ADLS) to Azure **control-plane** status (Container Apps + Jobs + Executions).
- The design is cloud-native and low-coupling: ARM calls use Managed Identity, are bounded by explicit timeouts, and are wrapped by a TTL cache to protect both Azure rate limits and API latency.
- Security posture is strong by default: Azure resource IDs are not returned unless service auth is enabled and `SYSTEM_HEALTH_VERBOSE_IDS=true`.
- Near-term priorities: add lightweight probe telemetry (latency/failure counters), and consider bounded retries/backoff for ARM transient failures.

### 2. System Map (High-Level)
- **Backtest API**
  - `asset_allocation/backtest/service/app.py`: exposes `GET /system/health` and caches responses via `TtlCache`.
- **Monitoring Layer**
  - `asset_allocation/monitoring/system_health.py`: aggregates freshness probes + optional ARM probes into a UI-friendly payload (`overall`, `dataLayers`, `recentJobs`, `alerts`, optional `resources`).
  - `asset_allocation/monitoring/arm_client.py`: minimal ARM REST client using `DefaultAzureCredential` + `httpx`.
  - `asset_allocation/monitoring/control_plane.py`: probe helpers for Container Apps and Jobs; maps ARM properties to `healthy|warning|error|unknown` and job runs to `success|failed|running|pending`.
- **UI**
  - `asset_allocation/ui2.0/src/types/strategy.ts`: extends `SystemHealth` with `resources?: ResourceHealth[]`.
  - `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx`: renders Azure resource health table when `resources` is present.

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None identified in the scoped Phase 2 changes.

#### 3.2 Major
- **[Limited observability of probe performance]**
  - **Evidence:** No explicit metrics emitted for ARM probe latency/error rates; failures are surfaced only via `alerts[]`.
  - **Why it matters:** Without latency/failure counters, it’s hard to distinguish “Azure throttling/transient failures” vs “real resource degradation” and to tune TTL/timeout settings.
  - **Recommendation:** Add minimal metrics (probe duration histogram, success/failure counters, cache hit ratio) and structured logs with probe targets (names only; IDs redacted by default).
  - **Acceptance Criteria:** Metrics/structured logs exist for ARM + ADLS probes and can be used to alert on sustained failures or latency spikes.
  - **Owner Suggestion:** Delivery Engineer Agent / QA Release Gate Agent

#### 3.3 Minor
- **[ARM transient errors are best-effort only]**
  - **Evidence:** ARM calls are attempted once per refresh; exceptions generate a warning alert (“Azure monitoring disabled”).
  - **Why it matters:** Single-attempt probes may flap during brief Azure control-plane hiccups.
  - **Recommendation:** Add bounded retries with jitter for retryable classes (429/5xx/timeouts).
  - **Acceptance Criteria:** Retries are capped (e.g., 2 attempts) and do not materially increase tail latency.
  - **Owner Suggestion:** Delivery Engineer Agent

### 4. Architectural Recommendations
- Keep the additive probe model: data-plane + control-plane signals merged into one UI contract (`SystemHealth`) with optional `resources`.
- Maintain “secure defaults”: continue to redact Azure IDs unless explicitly enabled behind auth.
- Consider evolving probes into a plugin registry (Phase 3) so additional Azure resources (SQL, Storage, Key Vault) can be added without expanding a single module.

### 5. Operational Readiness & Observability
- Existing safeguards:
  - TTL caching for `/system/health` responses (reduces polling impact).
  - ARM request timeout control (`SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS`).
  - Alerts emitted for warning/error resources and failed executions.
- Recommended signals (Phase 3):
  - `system_health_cache_hit_total`, `system_health_refresh_error_total`
  - `arm_probe_duration_seconds` (by resource type), `arm_probe_errors_total`

### 6. Refactoring Examples (Targeted)
- N/A (Phase 2 changes are already modular and low-coupling).

### 7. Evidence & Telemetry
- Files reviewed: `asset_allocation/monitoring/system_health.py`, `asset_allocation/monitoring/arm_client.py`, `asset_allocation/monitoring/control_plane.py`, `asset_allocation/backtest/service/app.py`, `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx`.
- Tests run: `python3 -m pytest -q` → **124 passed, 3 warnings**.

