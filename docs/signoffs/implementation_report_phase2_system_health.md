# Implementation Report

## 1. Execution Summary
- Implemented Phase 2 Azure **control-plane** health probes (Container Apps + Container Apps Jobs + recent Job Executions) and surfaced the results via `GET /system/health`.
- Extended the UI System Status page to render Azure resource health when the backend returns `resources`.
- Added configuration knobs (env vars) and deterministic unit tests (no real Azure calls).

**Out of scope**
- Azure Monitor / Application Insights metrics, logs, or traces ingestion (Phase 3+).
- Persisted alert acknowledgement state (Phase 3+).
- Deep job-log retrieval (diagnostics beyond ARM status + executions list).

## 2. Architectural Alignment Matrix
- **Requirement:** “Monitor health of various Azure resources so the UI can display them.”
  - **Implementation:** `asset_allocation/monitoring/arm_client.py`, `asset_allocation/monitoring/control_plane.py`, `asset_allocation/monitoring/system_health.py`, `asset_allocation/backtest/service/app.py`, `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx`
  - **Status:** Complete
  - **Notes:** Uses ARM REST (Managed Identity) + TTL caching to avoid chatty polling.

- **Requirement:** “Secure by default; don’t leak resource IDs to unauthenticated callers.”
  - **Implementation:** `asset_allocation/backtest/service/app.py` gates `include_resource_ids` on `BACKTEST_AUTH_MODE != none` and `SYSTEM_HEALTH_VERBOSE_IDS=true`.
  - **Status:** Complete
  - **Notes:** `resources[].azureId` is omitted unless explicitly enabled.

- **Requirement:** “Fast + resilient health collection.”
  - **Implementation:** `asset_allocation/monitoring/system_health.py` uses best-effort ARM probes with per-request timeout; `asset_allocation/monitoring/ttl_cache.py` caches `/system/health` responses.
  - **Status:** Complete
  - **Notes:** Probe failures degrade via alerts instead of failing the endpoint when cached data exists.

## 3. Change Set
**Added**
- `asset_allocation/monitoring/arm_client.py`
- `asset_allocation/monitoring/control_plane.py`
- `tests/monitoring/test_control_plane.py`
- `docs/signoffs/audit_snapshot_phase2_system_health.json`
- `docs/signoffs/implementation_report_phase2_system_health.md`

**Modified**
- `asset_allocation/monitoring/system_health.py`
- `asset_allocation/backtest/service/app.py`
- `.env.template`
- `asset_allocation/ui2.0/src/types/strategy.ts`
- `asset_allocation/ui2.0/src/app/components/pages/SystemStatusPage.tsx`
- `tests/monitoring/test_system_health.py`

**Key Interfaces**
- API: `GET /system/health` returns `SystemHealth` with optional `resources?: ResourceHealth[]`
- Env vars:
  - `SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID`, `SYSTEM_HEALTH_ARM_RESOURCE_GROUP`
  - `SYSTEM_HEALTH_ARM_CONTAINERAPPS` (CSV), `SYSTEM_HEALTH_ARM_JOBS` (CSV)
  - `SYSTEM_HEALTH_ARM_API_VERSION`, `SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS`
  - `SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB`
  - `SYSTEM_HEALTH_VERBOSE_IDS` (only honored when auth enabled)

## 4. Code Implementation
### ARM client (REST + Managed Identity)
```python
# asset_allocation/monitoring/arm_client.py
token = self._credential.get_token("https://management.azure.com/.default")
resp = self._http.get(url, headers={"Authorization": f"Bearer {token.token}"}, params={"api-version": api_version})
```

### System health aggregation includes optional control-plane probes
```python
# asset_allocation/monitoring/system_health.py
arm_enabled = bool(subscription_id and resource_group and (app_names or job_names))
if arm_enabled:
    with AzureArmClient(arm_cfg) as arm:
        ...
```

### `/system/health` redacts Azure IDs by default
```python
# asset_allocation/backtest/service/app.py
include_ids = False
if settings.auth_mode != "none":
    raw = os.environ.get("SYSTEM_HEALTH_VERBOSE_IDS", "").strip().lower()
    include_ids = raw in {"1", "true", "t", "yes", "y", "on"}
return collect_system_health_snapshot(include_resource_ids=include_ids)
```

## 5. Observability & Operational Readiness
- Existing `/system/health` response includes:
  - `alerts[]` for stale/error data layers, warning/error Azure resources, and failed job executions.
  - Response headers: `X-System-Health-Cache: hit|miss`, `X-System-Health-Stale: 1` when serving stale cached data.
- Telemetry follow-up (recommended): add counters/timers for probe latency and failure rates (ARM + ADLS) and expose via app metrics.

## 6. Cloud-Native Configuration (If applicable)
- Configure the ARM probes via env vars (see `.env.template`).
- Ensure the runtime identity has `Microsoft.App/*/read` on the target resource group (Reader role is typically sufficient for GET/list).

## 7. Verification Steps
- Unit/integration tests: `python3 -m pytest -q`
- Focused monitoring tests: `python3 -m pytest -q tests/monitoring/test_system_health.py tests/monitoring/test_control_plane.py`
- Manual smoke:
  - `curl -s http://<backtest-api-host>/system/health | jq '.resources'`

## 8. Risks & Follow-ups
- ARM throttling / transient failures: consider adding bounded retries with jitter (HTTP 429/5xx) and per-probe latency metrics.
- API version drift: `SYSTEM_HEALTH_ARM_API_VERSION` is configurable; consider pinning per-resource type if Azure introduces breaking changes.
- UI automated coverage: CI runs `pnpm exec vitest run` + `pnpm build`, but no UI assertions were added for the new table.

## 9. Evidence & Telemetry
- `python3 -m pytest -q` → **124 passed, 3 warnings**
- `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out docs/signoffs/audit_snapshot_phase2_system_health.json` → wrote snapshot
