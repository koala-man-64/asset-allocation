# Drift Report

## Summary
- Mode: `audit`
- Generated at: `2026-02-28T14:15:15.167642+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **41.5** (threshold fail: `35.0`)
- Result: **FAIL**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `(command-output)` | 1 | 15.0 |
| `tests/market_data/test_bronze_market_data.py` | 2 | 6.4 |
| `tests/price_target_data/test_bronze_price_target_data.py` | 2 | 6.4 |
| `tests/earnings_data/test_bronze_earnings_data.py` | 2 | 6.4 |
| `tests/finance_data/test_bronze_finance_data.py` | 2 | 6.4 |
| `tasks/market_data/bronze_market_data.py` | 1 | 0.15 |
| `ui/src/app/components/pages/system-status/JobKillSwitchPanel.tsx` | 1 | 0.15 |
| `tasks/price_target_data/bronze_price_target_data.py` | 1 | 0.15 |
| `tasks/finance_data/bronze_finance_data.py` | 1 | 0.15 |
| `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` | 1 | 0.15 |

## Category Findings
### Config/Infra Drift
- **[MEDIUM] Recent config churn detected** (confidence 0.6)
  - Expected vs Observed: Configuration should remain stable and coordinated across contributors. | 36 recent commits touched config/infra files in lookback window.
  - Evidence:
    - Lookback config-touching commits: 36
  - Recommendation: Consolidate config ownership, batch related changes, and document rationale in PRs.
  - Verification:
    - `Inspect recent config PRs`
    - `Audit gate consistency across workflows`

### Test Drift
- **[MEDIUM] Test philosophy drift** (confidence 0.63)
  - Expected vs Observed: A single dominant testing style should be applied within a module area. | Competing test styles detected: {'snapshot': 1, 'integration-heavy': 1}.
  - Files: `tests/earnings_data/test_bronze_earnings_data.py`, `tests/finance_data/test_bronze_finance_data.py`, `tests/market_data/test_bronze_market_data.py`, `tests/price_target_data/test_bronze_price_target_data.py`
  - Evidence:
    - Test style counts: {'snapshot': 1, 'integration-heavy': 1}
  - Attribution:
    - `tests/earnings_data/test_bronze_earnings_data.py`
      - 496c82f|rdprokes|2026-02-22|updated jobs for incremental refresh
    - `tests/finance_data/test_bronze_finance_data.py`
      - 98b6921|rdprokes|2026-02-25|fixed purging
    - `tests/market_data/test_bronze_market_data.py`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `tests/price_target_data/test_bronze_price_target_data.py`
      - 496c82f|rdprokes|2026-02-22|updated jobs for incremental refresh
  - Recommendation: Define module-level test style guidance (snapshot vs mocks vs integration) and align suites.
  - Verification:
    - `Run full test suite`
    - `Review flaky test rates`

### Docs Drift
- **[LOW] Code changed without documentation updates** (confidence 0.68)
  - Expected vs Observed: Docs/examples/changelog should stay aligned with behavior and API changes. | No documentation files changed alongside code updates.
  - Files: `tasks/earnings_data/bronze_earnings_data.py`, `tasks/finance_data/bronze_finance_data.py`, `tasks/market_data/bronze_market_data.py`, `tasks/price_target_data/bronze_price_target_data.py`, `tests/earnings_data/test_bronze_earnings_data.py`, `tests/finance_data/test_bronze_finance_data.py`, `tests/market_data/test_bronze_market_data.py`, `tests/price_target_data/test_bronze_price_target_data.py`, `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`, `ui/src/app/components/pages/system-status/JobKillSwitchPanel.tsx`
  - Evidence:
    - No files matched docs patterns in the change set.
  - Attribution:
    - `tasks/earnings_data/bronze_earnings_data.py`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `tasks/finance_data/bronze_finance_data.py`
      - b61bf5b|rdprokes|2026-02-26|purge parity
    - `tasks/market_data/bronze_market_data.py`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `tasks/price_target_data/bronze_price_target_data.py`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
  - Recommendation: Update README/docs/changelog to reflect behavior, API, and configuration changes.
  - Verification:
    - `Review docs for changed modules`
    - `Run docs lint/checks if available`

## Suggested Remediation Plan
1. **[MEDIUM] Recent config churn detected** (Config/Infra Drift)
   - What to change: Consolidate config ownership, batch related changes, and document rationale in PRs.
   - Why: Expected: Configuration should remain stable and coordinated across contributors. Observed: 36 recent commits touched config/infra files in lookback window.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Inspect recent config PRs`
     - `Audit gate consistency across workflows`
2. **[MEDIUM] Test philosophy drift** (Test Drift)
   - What to change: Define module-level test style guidance (snapshot vs mocks vs integration) and align suites.
   - Why: Expected: A single dominant testing style should be applied within a module area. Observed: Competing test styles detected: {'snapshot': 1, 'integration-heavy': 1}.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: low
   - Verification:
     - `Run full test suite`
     - `Review flaky test rates`
3. **[LOW] Code changed without documentation updates** (Docs Drift)
   - What to change: Update README/docs/changelog to reflect behavior, API, and configuration changes.
   - Why: Expected: Docs/examples/changelog should stay aligned with behavior and API changes. Observed: No documentation files changed alongside code updates.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: low
   - Verification:
     - `Review docs for changed modules`
     - `Run docs lint/checks if available`

## Appendix
### Tool Run Status
- `lint` `python3 -m ruff check .` -> **passed** (exit 0)
```text
All checks passed!
```
- `test_fast` `python3 -m pytest -q tests/tasks tests/market_data tests/finance_data tests/earnings_data tests/price_target_data` -> **passed** (exit 0)
```text
...............................................................................................................................................
=============================== warnings summary ===============================
tests/earnings_data/test_feature_generator.py::test_compute_features_adds_expected_columns
tests/earnings_data/test_feature_generator.py::test_compute_features_rolls_over_quarters
tests/earnings_data/test_feature_generator.py::test_compute_features_handles_divide_by_zero
tests/earnings_data/test_gold_earnings_data.py::test_compute_features
  /mnt/c/Users/rdpro/Projects/AssetAllocation/tasks/earnings_data/gold_earnings_data.py:142: DeprecationWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
    out = out.groupby("symbol", sort=False, group_keys=False).apply(

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
143 passed, 4 warnings in 9.18s
```
