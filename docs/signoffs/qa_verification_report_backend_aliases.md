# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** High
- **Scope:** Additive backend aliases for UI data retrieval (`/market`, `/finance`, `/strategies`) + unit tests validating routing/path mapping.
- **Top risks remaining:** Behavior drift between alias vs canonical endpoints; environment-specific Delta access failures (requires dev/staging smoke once deployed).

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type | Local | Dev | Prod | Status | Notes |
|---|---:|---|---|---|---|---|---|
| `GET /market/{layer}/{ticker}` routing + path mapping | Medium | Unit | ✅ | Recommended | Safe-only | Pass | Uses dependency mocking |
| `GET /finance/{layer}/{sub_domain}/{ticker}` routing + path mapping | Medium | Unit | ✅ | Recommended | Safe-only | Pass | Includes invalid subdomain case |
| `GET /strategies` alias | Medium | Unit | ✅ | Recommended | Safe-only | Pass | Uses dependency mocking |
| Alias parity with canonical endpoints | Medium | Manual | Planned | Recommended | Safe-only | Pending | Compare outputs for a known ticker |
| Delta connectivity + auth in target env | High | Manual smoke | Planned | Recommended | Safe-only | Pending | Requires deployed backend + ADLS access |

## 3. Test Cases (Prioritized)
- **TC1: Unit tests for alias endpoints**
  - Purpose: validate request routing and path mapping deterministically.
  - Steps: `python3 -m pytest -q tests/backend/test_alias_endpoints.py`
  - Expected: all tests pass.
  - Status: Pass

- **TC2: Local smoke (manual)**
  - Preconditions: backend API runnable locally with any required env vars for import.
  - Steps:
    1) `uvicorn backend.api.main:app --reload`
    2) `curl -sS http://localhost:8000/market/silver/AAPL | head`
    3) `curl -sS http://localhost:8000/finance/silver/balance_sheet/AAPL | head`
    4) `curl -sS http://localhost:8000/strategies | head`
  - Expected: non-5xx responses; if data missing, `404` is acceptable.
  - Status: Planned

- **TC3: Dev/staging parity check (manual)**
  - Purpose: ensure aliases and canonical endpoints return equivalent data.
  - Steps (example):
    - `curl -sS "$BASE_URL/market/silver/AAPL" | jq 'length'`
    - `curl -sS "$BASE_URL/data/silver/market?ticker=AAPL" | jq 'length'`
  - Expected: equivalent payload shapes and comparable row counts.
  - Status: Pending

## 4. Automated Tests Added/Updated (If applicable)
- Added `tests/backend/test_alias_endpoints.py` covering:
  - invalid layer/subdomain rejection
  - market/finance path mapping for silver + gold
  - strategies alias routing

## 5. Environment Verification
### Local (Required)
- `python3 -m pytest -q tests/backend/test_alias_endpoints.py` → **7 passed**

### Dev (Recommended)
- Run TC2 and TC3 once deployed with real Delta access.

### Prod (Optional, Safe-Only)
- Safe-only checks:
  - hit `/health` and a single alias endpoint for a known ticker
  - observe error rates and latency; rollback if sustained 5xx

## 6. CI/CD Verification (If applicable)
- No workflow changes required. CI already runs `python -m pytest` and will pick up the new backend tests.

## 7. Release Readiness Gate
- **Gate decision:** Pass with conditions
- **Conditions:**
  - Perform a dev/staging parity check for 1–2 representative tickers before relying on aliases in production UI.

## 8. Evidence & Telemetry
- `python3 -m pytest -q tests/backend/test_alias_endpoints.py` → **7 passed**

## 9. Gaps & Recommendations
- Add one lightweight contract test that asserts alias and canonical outputs match for a stubbed DeltaTable (equivalence test) to prevent drift.

