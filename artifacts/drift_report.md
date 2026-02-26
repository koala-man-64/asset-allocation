# Drift Report

## Summary
- Mode: `audit`
- Generated at: `2026-02-26T19:00:02.206300+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **107.5** (threshold fail: `35.0`)
- Result: **FAIL**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `(command-output)` | 3 | 60.0 |
| `deploy/job_silver_finance_data.yaml` | 1 | 7.5 |
| `deploy/job_bronze_finance_data.yaml` | 1 | 7.5 |
| `deploy/app_api.yaml` | 1 | 7.5 |
| `monitoring/domain_metadata.py` | 1 | 1.47 |
| `tests/monitoring/test_domain_metadata.py` | 1 | 1.47 |
| `tasks/finance_data/silver_finance_data.py` | 1 | 1.47 |
| `tests/test_runtime_config.py` | 1 | 1.47 |
| `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` | 1 | 1.47 |
| `api/endpoints/system.py` | 1 | 1.47 |

## Category Findings
### Architecture Drift
- **[MEDIUM] Inconsistent micro-architecture patterns** (confidence 0.7)
  - Expected vs Observed: Use a consistent error-handling pattern. Baseline prevalence favors `exceptions`. | Competing patterns detected in changed files: exceptions, sentinel_returns.
  - Files: `api/endpoints/system.py`, `core/runtime_config.py`, `monitoring/domain_metadata.py`, `tasks/finance_data/bronze_finance_data.py`, `tasks/finance_data/silver_finance_data.py`, `tasks/market_data/gold_market_data.py`, `tasks/market_data/materialize_gold_market_by_date.py`, `tasks/market_data/silver_market_data.py`, `tests/finance_data/test_silver_finance_data.py`, `tests/market_data/test_materialize_gold_market_by_date.py`
  - Evidence:
    - Baseline strategy counts: {'exceptions': 11, 'sentinel_returns': 8}
    - Observed strategy counts: {'exceptions': 12, 'sentinel_returns': 8}
  - Attribution:
    - `api/endpoints/system.py`
      - 1691099|rdprokes|2026-02-26|gold purge
    - `core/runtime_config.py`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `monitoring/domain_metadata.py`
      - 8ce79b6|rdprokes|2026-02-26|done
    - `tasks/finance_data/bronze_finance_data.py`
      - 98b6921|rdprokes|2026-02-25|fixed purging
  - Recommendation: Standardize error handling around `exceptions` and migrate divergent paths incrementally.
  - Verification:
    - `Run targeted tests for standardized paths`
    - `Re-run drift audit`

### Config/Infra Drift
- **[HIGH] Configuration/infra files changed** (confidence 0.8)
  - Expected vs Observed: Pipeline and infra changes should preserve or strengthen quality/safety gates. | CI/deploy/configuration files were modified.
  - Files: `deploy/app_api.yaml`, `deploy/job_bronze_finance_data.yaml`, `deploy/job_silver_finance_data.yaml`
  - Evidence:
    - --- a/ui/src/app/__tests__/GoldMaterializationPage.test.tsx
    - -  const isYearMonthValid = !yearMonthTrimmed || /^\d{4}-\d{2}$/.test(yearMonthTrimmed);
    - -  const isMaxTablesValid = !maxTablesTrimmed || /^[1-9]\d*$/.test(maxTablesTrimmed);
  - Attribution:
    - `deploy/app_api.yaml`
      - 7faca87|rdprokes|2026-02-25|updated replica count
    - `deploy/job_bronze_finance_data.yaml`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `deploy/job_silver_finance_data.yaml`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
  - Recommendation: Review config deltas with release/security owners and validate gates remain enforced.
  - Verification:
    - `Run CI pipeline in branch`
    - `Validate deploy plans and policy checks`
- **[MEDIUM] Recent config churn detected** (confidence 0.6)
  - Expected vs Observed: Configuration should remain stable and coordinated across contributors. | 32 recent commits touched config/infra files in lookback window.
  - Evidence:
    - Lookback config-touching commits: 32
  - Recommendation: Consolidate config ownership, batch related changes, and document rationale in PRs.
  - Verification:
    - `Inspect recent config PRs`
    - `Audit gate consistency across workflows`
- **[LOW] Config churn trend** (confidence 0.7)
  - Expected vs Observed: CI/lint/config should evolve through coordinated, low-churn changes. | 32 recent commits modified config-related files.
  - Evidence:
    - Commits touching config in lookback: 32
  - Recommendation: Batch configuration changes and codify ownership/approval expectations.
  - Verification:
    - `Inspect config commit history`

### Test Drift
- **[HIGH] Test cases removed** (confidence 0.8)
  - Expected vs Observed: Coverage should not regress for changed behavior. | Detected removed test definitions/assertions in diff.
  - Evidence:
    - -def test_normalize_env_override_gold_market_by_date_max_tables_int():
    - -  const isYearMonthValid = !yearMonthTrimmed || /^\d{4}-\d{2}$/.test(yearMonthTrimmed);
    - -  const isMaxTablesValid = !maxTablesTrimmed || /^[1-9]\d*$/.test(maxTablesTrimmed);
  - Recommendation: Restore removed tests or add equivalent coverage for modified behavior.
  - Verification:
    - `Run fast and full tests`
    - `Check coverage trend`

## Suggested Remediation Plan
1. **[MEDIUM] Inconsistent micro-architecture patterns** (Architecture Drift)
   - What to change: Standardize error handling around `exceptions` and migrate divergent paths incrementally.
   - Why: Expected: Use a consistent error-handling pattern. Baseline prevalence favors `exceptions`. Observed: Competing patterns detected in changed files: exceptions, sentinel_returns.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run targeted tests for standardized paths`
     - `Re-run drift audit`
2. **[HIGH] Configuration/infra files changed** (Config/Infra Drift)
   - What to change: Review config deltas with release/security owners and validate gates remain enforced.
   - Why: Expected: Pipeline and infra changes should preserve or strengthen quality/safety gates. Observed: CI/deploy/configuration files were modified.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run CI pipeline in branch`
     - `Validate deploy plans and policy checks`
3. **[MEDIUM] Recent config churn detected** (Config/Infra Drift)
   - What to change: Consolidate config ownership, batch related changes, and document rationale in PRs.
   - Why: Expected: Configuration should remain stable and coordinated across contributors. Observed: 32 recent commits touched config/infra files in lookback window.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Inspect recent config PRs`
     - `Audit gate consistency across workflows`
4. **[LOW] Config churn trend** (Config/Infra Drift)
   - What to change: Batch configuration changes and codify ownership/approval expectations.
   - Why: Expected: CI/lint/config should evolve through coordinated, low-churn changes. Observed: 32 recent commits modified config-related files.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: low
   - Verification:
     - `Inspect config commit history`
5. **[HIGH] Test cases removed** (Test Drift)
   - What to change: Restore removed tests or add equivalent coverage for modified behavior.
   - Why: Expected: Coverage should not regress for changed behavior. Observed: Detected removed test definitions/assertions in diff.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run fast and full tests`
     - `Check coverage trend`

## Appendix
### Tool Run Status
- `quality-gates` `<skipped>` -> **skipped**
```text
Skipped by --skip-quality-gates
```
