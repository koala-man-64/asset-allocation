# Drift Report

## Summary
- Mode: `audit`
- Generated at: `2026-03-03T19:48:53.172443+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **26.5** (threshold fail: `35.0`)
- Result: **PASS**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `ui/src/app/components/layout/LeftNavigation.tsx` | 2 | 8.83 |
| `ui/src/app/App.tsx` | 2 | 8.83 |
| `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx` | 2 | 8.83 |
| `(command-output)` | 1 | 0.0 |

## Category Findings
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
- **[MEDIUM] Code changed without nearby test updates** (confidence 0.72)
  - Expected vs Observed: Behavioral code changes should include corresponding test updates where relevant. | Detected source changes without matching test file changes.
  - Files: `ui/src/app/App.tsx`, `ui/src/app/components/layout/LeftNavigation.tsx`, `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`
  - Evidence:
    - Changed non-test files: ui/src/app/App.tsx, ui/src/app/components/layout/LeftNavigation.tsx, ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx
  - Attribution:
    - `ui/src/app/App.tsx`
      - 424b7f6|rdprokes|2026-02-25|fixed massive
    - `ui/src/app/components/layout/LeftNavigation.tsx`
      - 98b6921|rdprokes|2026-02-25|fixed purging
    - `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`
      - 72a40e1|rdprokes|2026-02-28|fixed
  - Recommendation: Add targeted unit tests for modified modules and critical edge cases.
  - Verification:
    - `Run fast tests`
    - `Review changed modules for edge cases`

### Docs Drift
- **[LOW] Code changed without documentation updates** (confidence 0.68)
  - Expected vs Observed: Docs/examples/changelog should stay aligned with behavior and API changes. | No documentation files changed alongside code updates.
  - Files: `ui/src/app/App.tsx`, `ui/src/app/components/layout/LeftNavigation.tsx`, `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`
  - Evidence:
    - No files matched docs patterns in the change set.
  - Attribution:
    - `ui/src/app/App.tsx`
      - 424b7f6|rdprokes|2026-02-25|fixed massive
    - `ui/src/app/components/layout/LeftNavigation.tsx`
      - 98b6921|rdprokes|2026-02-25|fixed purging
    - `ui/src/app/components/pages/system-status/DomainLayerComparisonPanel.tsx`
      - 72a40e1|rdprokes|2026-02-28|fixed
  - Recommendation: Update README/docs/changelog to reflect behavior, API, and configuration changes.
  - Verification:
    - `Review docs for changed modules`
    - `Run docs lint/checks if available`

## Suggested Remediation Plan
1. **[MEDIUM] Recent config churn detected** (Config/Infra Drift)
   - What to change: Consolidate config ownership, batch related changes, and document rationale in PRs.
   - Why: Expected: Configuration should remain stable and coordinated across contributors. Observed: 26 recent commits touched config/infra files in lookback window.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Inspect recent config PRs`
     - `Audit gate consistency across workflows`
2. **[MEDIUM] Code changed without nearby test updates** (Test Drift)
   - What to change: Add targeted unit tests for modified modules and critical edge cases.
   - Why: Expected: Behavioral code changes should include corresponding test updates where relevant. Observed: Detected source changes without matching test file changes.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run fast tests`
     - `Review changed modules for edge cases`
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
- `quality-gates` `<skipped>` -> **skipped**
```text
Skipped by --skip-quality-gates
```
