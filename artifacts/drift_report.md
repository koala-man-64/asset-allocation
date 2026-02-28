# Drift Report

## Summary
- Mode: `audit`
- Generated at: `2026-02-28T15:02:51.689697+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **16.5** (threshold fail: `35.0`)
- Result: **PASS**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `(command-output)` | 1 | 15.0 |
| `tests/monitoring/test_control_plane.py` | 1 | 0.17 |
| `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` | 1 | 0.17 |
| `monitoring/system_health.py` | 1 | 0.17 |
| `ui/src/types/strategy.ts` | 1 | 0.17 |
| `monitoring/control_plane.py` | 1 | 0.17 |
| `ui/src/app/components/pages/system-status/JobKillSwitchPanel.tsx` | 1 | 0.17 |
| `ui/src/app/components/pages/SystemStatusPage.tsx` | 1 | 0.17 |
| `tests/monitoring/test_system_health.py` | 1 | 0.17 |
| `ui/src/app/__tests__/SystemStatusPage.test.tsx` | 1 | 0.17 |

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

### Docs Drift
- **[LOW] Code changed without documentation updates** (confidence 0.68)
  - Expected vs Observed: Docs/examples/changelog should stay aligned with behavior and API changes. | No documentation files changed alongside code updates.
  - Files: `monitoring/control_plane.py`, `monitoring/system_health.py`, `tests/monitoring/test_control_plane.py`, `tests/monitoring/test_system_health.py`, `ui/src/app/__tests__/SystemStatusPage.test.tsx`, `ui/src/app/components/pages/SystemStatusPage.tsx`, `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`, `ui/src/app/components/pages/system-status/JobKillSwitchPanel.tsx`, `ui/src/types/strategy.ts`
  - Evidence:
    - No files matched docs patterns in the change set.
  - Attribution:
    - `monitoring/control_plane.py`
      - 6e868ba|rdprokes|2026-02-14|fuxed test
    - `monitoring/system_health.py`
      - fb8bba3|rdprokes|2026-02-23|dont even know
    - `tests/monitoring/test_control_plane.py`
      - 7786ef9|rdprokes|2026-01-31|g
    - `tests/monitoring/test_system_health.py`
      - 42d2729|rdprokes|2026-02-19|hjgf
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
2. **[LOW] Code changed without documentation updates** (Docs Drift)
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
143 passed, 4 warnings in 8.16s
```
