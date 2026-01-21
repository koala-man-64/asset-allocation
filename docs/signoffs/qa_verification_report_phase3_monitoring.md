# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** Medium (plan-only; no Phase 3 implementation verified yet)
- **Scope:** Test strategy and release gates for Phase 3 runtime monitoring + alert acknowledgement.
- **Top risks:** Azure API throttling/latency; multi-replica ack consistency; accidental leakage of Azure IDs/log payloads.

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type (Unit/Int/E2E/Manual) | Local | Dev | Prod | Status | Notes |
|---|---:|---|---:|---:|---:|---|---|
| Resource Health probe → `resources[].status` | High | Unit | Planned | Planned | Safe-only | Planned | Fake client; cover transitions and unknown states |
| Azure Monitor metrics probe parsing | High | Unit | Planned | Planned | Safe-only | Planned | Validate time window, units, missing series behavior |
| Log Analytics query probe parsing | High | Unit | Planned | Planned | Safe-only | Planned | Validate query errors/timeouts and redaction |
| Alert stable ID determinism | High | Unit | Planned | N/A | N/A | Planned | Same alert fields → same ID across refresh |
| Alert ack persistence across restarts/replicas | High | Integration | Planned | Planned | Safe-only | Planned | Requires shared backing store in dev |
| Ack endpoints authZ/authN | High | Integration | Planned | Planned | Safe-only | Planned | Ensure unauthenticated calls are rejected |
| UI ack/unack workflow | Medium | E2E/UI | CI | Dev optional | N/A | Planned | Add a small component test; E2E optional |
| `/system/health` latency under probe failures | High | Manual/Perf sanity | Planned | Planned | Safe-only | Planned | Force timeouts; verify bounded response time and alerts |

## 3. Test Cases (Prioritized)
- **Alert ID stability**
  - Purpose: prevent duplicated alerts and ensure ack targets the right alert.
  - Expected: deterministic `alerts[].id` across snapshots when underlying condition unchanged.

- **Ack persistence + auth**
  - Purpose: verify ack survives refresh/restart; verify ack endpoints require auth.
  - Expected: ack flips `acknowledged=true` and remains so until unacked; unauthenticated calls rejected.

- **Probe failure modes**
  - Purpose: ensure Azure API timeouts/throttling degrade gracefully (alerts) and do not break `/system/health`.
  - Expected: endpoint responds with cached/stale data when available; warnings surfaced via alerts.

## 4. Automated Tests Added/Updated (If applicable)
- Planned additions:
  - `tests/monitoring/test_phase3_runtime_signals.py`
  - `tests/monitoring/test_alert_ack_store.py`

## 5. Environment Verification
### Local (Required)
- Run: `python3 -m pytest -q`
- Run: `python3 -m pytest -q tests/monitoring/`

#### Dev (Optional)
- Safe checks:
  - `GET /system/health` returns runtime signals when enabled
  - Ack/unack endpoints work with test identity; verify no `azureId` leakage unless explicitly enabled behind auth

#### Prod (Optional, Safe-Only)
- Safe checks:
  - `GET /system/health` only; verify latency and error rate; verify no sensitive fields

## 6. CI/CD Verification (If applicable)
- Ensure CI continues to:
  - run full `pytest`
  - run ui2.0 `vitest` + `pnpm build`
- If new deps are added (e.g., azure monitor libs), ensure lockfiles updated and `pip check` passes.

## 7. Release Readiness Gate
- **Decision:** Not Applicable (plan-only). Gate should be evaluated after Phase 3 implementation lands with test evidence.

## 8. Evidence & Telemetry
- No Phase 3 execution evidence (plan-only).

## 9. Gaps & Recommendations
- Add deterministic fakes for Monitor/Log Analytics to keep unit tests hermetic.
- Add one “probe timeout” test to enforce bounded latency behavior.

