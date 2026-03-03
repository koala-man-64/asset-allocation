# Drift Report

## Summary
- Mode: `audit`
- Generated at: `2026-03-03T17:28:32.596218+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **120.0** (threshold fail: `35.0`)
- Result: **FAIL**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `api/endpoints/system.py` | 2 | 60.75 |
| `(command-output)` | 1 | 15.0 |
| `tests/tasks/test_backfill.py` | 1 | 3.41 |
| `tests/earnings_data/test_silver_earnings_data.py` | 1 | 3.41 |
| `tests/tasks/test_market_reconciliation.py` | 1 | 3.41 |
| `tests/core/test_massive_gateway_client.py` | 1 | 3.41 |
| `tests/market_data/test_gold_market_data_reconciliation.py` | 1 | 3.41 |
| `tests/api/test_data_service_bronze_raw.py` | 1 | 3.41 |
| `tests/market_data/test_silver_market_data.py` | 1 | 3.41 |
| `tests/price_target_data/test_silver_price_target_data.py` | 1 | 3.41 |

## Category Findings
### Security Drift
- **[HIGH] Protected paths changed** (confidence 0.93)
  - Expected vs Observed: Protected areas require explicit opt-in review before modification. | Files matching protected globs were modified.
  - Files: `api/endpoints/system.py`
  - Evidence:
    - Protected files touched: api/endpoints/system.py
  - Attribution:
    - `api/endpoints/system.py`
      - b61bf5b|rdprokes|2026-02-26|purge parity
  - Recommendation: Require explicit approver sign-off and run focused regression/security tests.
  - Verification:
    - `Manual security review`
    - `Auth/infrastructure regression tests`

### Config/Infra Drift
- **[MEDIUM] Recent config churn detected** (confidence 0.6)
  - Expected vs Observed: Configuration should remain stable and coordinated across contributors. | 26 recent commits touched config/infra files in lookback window.
  - Evidence:
    - Lookback config-touching commits: 26
  - Recommendation: Consolidate config ownership, batch related changes, and document rationale in PRs.
  - Verification:
    - `Inspect recent config PRs`
    - `Audit gate consistency across workflows`

### Test Drift
- **[HIGH] Test cases removed** (confidence 0.8)
  - Expected vs Observed: Coverage should not regress for changed behavior. | Detected removed test definitions/assertions in diff.
  - Files: `tests/api/test_data_service_bronze_raw.py`, `tests/api/test_system_purge_parallelism.py`, `tests/core/test_alpha_vantage_gateway_client.py`, `tests/core/test_massive_gateway_client.py`, `tests/earnings_data/test_silver_earnings_data.py`, `tests/finance_data/test_silver_finance_data.py`, `tests/market_data/test_gold_market_data_reconciliation.py`, `tests/market_data/test_silver_market_data.py`, `tests/price_target_data/test_silver_price_target_data.py`, `tests/tasks/test_backfill.py`
  - Evidence:
    - tests/api/test_data_service_bronze_raw.py: -def test_bronze_market_reads_raw_csv(monkeypatch):
    - tests/api/test_data_service_bronze_raw.py: -def test_bronze_earnings_reads_raw_json(monkeypatch):
    - tests/api/test_system_purge_parallelism.py: -def test_resolve_purge_preview_load_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    - tests/api/test_system_purge_parallelism.py: -def test_resolve_purge_scope_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    - tests/api/test_system_purge_parallelism.py: -def test_resolve_purge_symbol_target_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    - tests/api/test_system_purge_parallelism.py: -def test_resolve_purge_symbol_layer_workers(monkeypatch: pytest.MonkeyPatch) -> None:
  - Attribution:
    - `tests/api/test_data_service_bronze_raw.py`
      - 7072dbe|rdprokes|2026-02-08|cleaned
    - `tests/api/test_system_purge_parallelism.py`
      - 98b6921|rdprokes|2026-02-25|fixed purging
    - `tests/core/test_alpha_vantage_gateway_client.py`
      - 5787e89|rdprokes|2026-02-21|gogo
    - `tests/core/test_massive_gateway_client.py`
      - 72a40e1|rdprokes|2026-02-28|fixed
  - Recommendation: Restore removed tests or add equivalent coverage for modified behavior.
  - Verification:
    - `Run fast and full tests`
    - `Check coverage trend`

### Performance Drift
- **[LOW] Potential performance drift signals detected** (confidence 0.62)
  - Expected vs Observed: Performance-sensitive code paths should avoid repeated query/network calls in loops and benchmark regressions. | Detected suspicious loop + I/O patterns and/or benchmark command failures.
  - Files: `api/data_service.py`, `api/endpoints/data.py`, `api/endpoints/system.py`, `core/config.py`, `core/core.py`, `core/pipeline.py`, `docs/data_lineage.md`, `monitoring/domain_metadata.py`, `tasks/common/market_reconciliation.py`, `tasks/earnings_data/bronze_earnings_data.py`
  - Evidence:
    -             for ticker in sorted(symbol_candidates):
  - Attribution:
    - `api/data_service.py`
      - c30c75c|rdprokes|2026-02-18|removed market by date
    - `api/endpoints/data.py`
      - c30c75c|rdprokes|2026-02-18|removed market by date
    - `api/endpoints/system.py`
      - b61bf5b|rdprokes|2026-02-26|purge parity
    - `core/config.py`
      - 53a0df9|rdprokes|2026-02-09|fixed massive
  - Recommendation: Inspect hotspots for N+1 patterns, cache repeated calls, and add focused micro-benchmarks.
  - Verification:
    - `Run benchmark command(s)`
    - `Profile impacted endpoints/functions`

## Suggested Remediation Plan
1. **[HIGH] Protected paths changed** (Security Drift)
   - What to change: Require explicit approver sign-off and run focused regression/security tests.
   - Why: Expected: Protected areas require explicit opt-in review before modification. Observed: Files matching protected globs were modified.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: high
   - Verification:
     - `Manual security review`
     - `Auth/infrastructure regression tests`
2. **[MEDIUM] Recent config churn detected** (Config/Infra Drift)
   - What to change: Consolidate config ownership, batch related changes, and document rationale in PRs.
   - Why: Expected: Configuration should remain stable and coordinated across contributors. Observed: 26 recent commits touched config/infra files in lookback window.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Inspect recent config PRs`
     - `Audit gate consistency across workflows`
3. **[HIGH] Test cases removed** (Test Drift)
   - What to change: Restore removed tests or add equivalent coverage for modified behavior.
   - Why: Expected: Coverage should not regress for changed behavior. Observed: Detected removed test definitions/assertions in diff.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run fast and full tests`
     - `Check coverage trend`
4. **[LOW] Potential performance drift signals detected** (Performance Drift)
   - What to change: Inspect hotspots for N+1 patterns, cache repeated calls, and add focused micro-benchmarks.
   - Why: Expected: Performance-sensitive code paths should avoid repeated query/network calls in loops and benchmark regressions. Observed: Detected suspicious loop + I/O patterns and/or benchmark command failures.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run benchmark command(s)`
     - `Profile impacted endpoints/functions`

## Appendix
### Tool Run Status
- `quality-gates` `<skipped>` -> **skipped**
```text
Skipped by --skip-quality-gates
```
