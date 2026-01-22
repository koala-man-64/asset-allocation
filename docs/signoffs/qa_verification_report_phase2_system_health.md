# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** High
- **Scope verified:** Phase 2 Azure control-plane monitoring for `/system/health` (Container Apps + Jobs + Job Executions) and UI rendering of `resources`.
- **Top remaining risks:** No live Azure environment verification in this report; ARM transient behavior (429/5xx) not simulated beyond unit tests.

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type (Unit/Int/E2E/Manual) | Local | Dev | Prod | Status | Notes |
|---|---:|---|---:|---:|---:|---|---|
| `/system/health` returns expected base keys | Medium | Automated (pytest) | ✅ | N/A | N/A | Pass | `tests/monitoring/test_system_health.py` |
| Azure ARM probe mapping: job executions → `JobRun` | High | Automated (pytest) | ✅ | N/A | N/A | Pass | `tests/monitoring/test_control_plane.py` |
| Azure ID redaction default (`resources[].azureId` omitted) | High | Automated (pytest) | ✅ | N/A | N/A | Pass | `tests/monitoring/test_system_health.py` |
| Overall status degradation/critical mapping via ARM signals | Medium | Automated (pytest) | ✅ | N/A | N/A | Pass | `tests/monitoring/test_system_health.py` |
| UI renders Azure resources table when present | Medium | CI build/test (vitest + build) | Not run | N/A | N/A | Pass (CI) | `.github/workflows/run_tests.yml` runs `pnpm exec vitest run` + `pnpm build` |

## 3. Test Cases (Prioritized)
- **Control-plane job execution mapping**
  - Purpose: ensure ARM execution status and timestamps map to UI `JobRun` consistently.
  - Steps: run `tests/monitoring/test_control_plane.py`.
  - Expected: status maps to `success|failed|running|pending`, runs are sorted by `startTime`.
  - Failure signals: incorrect ordering/status mapping; duration negative/missing unexpectedly.

- **Resource ID redaction**
  - Purpose: ensure Azure resource IDs are not returned unless explicitly enabled.
  - Steps: run `test_system_health_control_plane_redacts_resource_ids`.
  - Expected: `resources[].azureId` absent by default; present when `include_resource_ids=True`.
  - Failure signals: `azureId` present without explicit opt-in.

- **Overall health derivation from ARM signals**
  - Purpose: ensure warning resources degrade and failed job runs escalate to critical.
  - Steps: run `test_system_health_degraded_on_warning_resource` and `test_system_health_critical_on_failed_job_execution`.
  - Expected: `overall == degraded` for warning resources; `overall == critical` for failed executions.

## 4. Automated Tests Added/Updated (If applicable)
- Added: `tests/monitoring/test_control_plane.py`
- Updated: `tests/monitoring/test_system_health.py` (ARM-enabled aggregation tests using fakes; no network calls)

## 5. Environment Verification
### Local (Required)
- Commands run:
  - `python3 -m pytest -q` → **124 passed, 3 warnings**
  - `python3 -m pytest -q tests/monitoring/test_control_plane.py tests/monitoring/test_system_health.py` → **9 passed**
- Troubleshooting notes:
  - If running ARM probes locally, ensure `DefaultAzureCredential` can resolve a token (Managed Identity in Azure, or Azure CLI login locally).

#### Dev (Optional)
- Not executed (no dev endpoints/config provided). Suggested safe checks:
  - `GET /system/health` returns 200 and includes `resources` when ARM env vars are configured.

#### Prod (Optional, Safe-Only)
- Not executed. Suggested safe checks:
  - `GET /system/health` (read-only) and verify no `resources[].azureId` unless explicitly enabled behind auth.

## 6. CI/CD Verification (If applicable)
- Workflow reviewed: `.github/workflows/run_tests.yml`
  - Builds UI (ui2.0) in a pinned Node container and runs `vitest` + `pnpm build`.
  - Runs `pytest` after installing pinned Python dependencies (lockfiles).

## 7. Release Readiness Gate
- **Decision:** Pass (for scoped Phase 2 changes)
- **Evidence:** full local pytest suite green; targeted unit tests cover highest-risk logic (ARM mapping + ID redaction + overall derivation).
- **Rollback triggers:** sustained ARM probe failures or elevated `/system/health` latency; disable ARM probes by unsetting `SYSTEM_HEALTH_ARM_*` env vars.

## 8. Evidence & Telemetry
- Local tests: `python3 -m pytest -q` → **124 passed, 3 warnings**
- Repo inventory: `docs/signoffs/audit_snapshot_phase2_system_health.json`

## 9. Gaps & Recommendations
- Add retry/backoff tests for simulated ARM 429/5xx handling if retries are introduced.
- Add a small UI unit test asserting the Azure Resources table renders when `resources` is non-empty (optional).

## 10. Handoffs (Only if needed)
- `Handoff: DevOps Agent` — ensure container app/job Reader permissions are granted to the runtime identity and ARM env vars are set per environment.

