# Drift Report

## Summary
- Mode: `recommend`
- Generated at: `2026-02-26T23:40:10.663870+00:00`
- Baseline: `main` (configured baseline.branch)
- Compare: `main` -> `HEAD`
- Drift score: **30.0** (threshold fail: `35.0`)
- Result: **PASS**

## Top Drift Hotspots
| File/Module | Findings | Score |
|---|---:|---:|
| `(command-output)` | 1 | 15.0 |
| `deploy/job_silver_market_data.yaml` | 1 | 1.07 |
| `.github/workflows/supply_chain_security.yml` | 1 | 1.07 |
| `deploy/job_bronze_price_target_data.yaml` | 1 | 1.07 |
| `deploy/job_gold_market_data.yaml` | 1 | 1.07 |
| `deploy/job_bronze_market_data.yaml` | 1 | 1.07 |
| `deploy/job_silver_price_target_data.yaml` | 1 | 1.07 |
| `deploy/job_gold_earnings_data.yaml` | 1 | 1.07 |
| `deploy/job_silver_finance_data.yaml` | 1 | 1.07 |
| `deploy/job_bronze_finance_data.yaml` | 1 | 1.07 |

## Category Findings
### Config/Infra Drift
- **[MEDIUM] Configuration/infra files changed** (confidence 0.8)
  - Expected vs Observed: Pipeline and infra changes should preserve or strengthen quality/safety gates. | CI/deploy/configuration files were modified.
  - Files: `.github/dependabot.yml`, `.github/workflows/supply_chain_security.yml`, `deploy/job_bronze_earnings_data.yaml`, `deploy/job_bronze_finance_data.yaml`, `deploy/job_bronze_market_data.yaml`, `deploy/job_bronze_price_target_data.yaml`, `deploy/job_gold_earnings_data.yaml`, `deploy/job_gold_finance_data.yaml`, `deploy/job_gold_market_data.yaml`, `deploy/job_gold_price_target_data.yaml`
  - Evidence:
    - Changed config files: .github/dependabot.yml, .github/workflows/supply_chain_security.yml, deploy/job_bronze_earnings_data.yaml, deploy/job_bronze_finance_data.yaml, deploy/job_bronze_market_data.yaml, deploy/job_bronze_price_target_data.yaml, deploy/job_gold_earnings_data.yaml, deploy/job_gold_finance_data.yaml, deploy/job_gold_market_data.yaml, deploy/job_gold_price_target_data.yaml, deploy/job_silver_earnings_data.yaml, deploy/job_silver_finance_data.yaml
  - Attribution:
    - `.github/dependabot.yml`
      - 8ce79b6|rdprokes|2026-02-26|done
    - `.github/workflows/supply_chain_security.yml`
      - 8ce79b6|rdprokes|2026-02-26|done
    - `deploy/job_bronze_earnings_data.yaml`
      - 4b8c4fd|rdprokes|2026-02-25|removed backfill
    - `deploy/job_bronze_finance_data.yaml`
      - b61bf5b|rdprokes|2026-02-26|purge parity
  - Recommendation: Review config deltas with release/security owners and validate gates remain enforced.
  - Verification:
    - `Run CI pipeline in branch`
    - `Validate deploy plans and policy checks`
- **[MEDIUM] Recent config churn detected** (confidence 0.6)
  - Expected vs Observed: Configuration should remain stable and coordinated across contributors. | 17 recent commits touched config/infra files in lookback window.
  - Evidence:
    - Lookback config-touching commits: 17
  - Recommendation: Consolidate config ownership, batch related changes, and document rationale in PRs.
  - Verification:
    - `Inspect recent config PRs`
    - `Audit gate consistency across workflows`

## Suggested Remediation Plan
1. **[MEDIUM] Configuration/infra files changed** (Config/Infra Drift)
   - What to change: Review config deltas with release/security owners and validate gates remain enforced.
   - Why: Expected: Pipeline and infra changes should preserve or strengthen quality/safety gates. Observed: CI/deploy/configuration files were modified.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Run CI pipeline in branch`
     - `Validate deploy plans and policy checks`
2. **[MEDIUM] Recent config churn detected** (Config/Infra Drift)
   - What to change: Consolidate config ownership, batch related changes, and document rationale in PRs.
   - Why: Expected: Configuration should remain stable and coordinated across contributors. Observed: 17 recent commits touched config/infra files in lookback window.
   - Patch approach: Apply deterministic fixes first (formatter/lint/rule-based edits), then targeted manual refactors.
   - Risk: medium
   - Verification:
     - `Inspect recent config PRs`
     - `Audit gate consistency across workflows`

## Patch Preview (Recommend Mode)
### `deploy/job_silver_market_data.yaml`
```diff
@@ -40,6 +40,10 @@ properties:
         value: INFO
       - name: DISABLE_DOTENV
         value: "true"
+      - name: BACKFILL_START_DATE
+        value: ${BACKFILL_START_DATE}
+      - name: BACKFILL_END_DATE
+        value: ${BACKFILL_END_DATE}
       - name: SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID
         value: ${AZURE_SUBSCRIPTION_ID}
       - name: SYSTEM_HEALTH_ARM_RESOURCE_GROUP
```
### `.github/workflows/supply_chain_security.yml`
```diff
@@ -5,6 +5,8 @@ on:
   push:
     branches:
       - main
+  schedule:
+    - cron: "17 9 * * 1-5"
   workflow_dispatch:
 
 permissions:
```
```diff
@@ -35,6 +37,20 @@ jobs:
           python -m pip install --upgrade pip
           python -m pip install pip-audit
 
+      - name: Prepare audit artifacts
+        run: |
+          mkdir -p artifacts
+
+      - name: Collect runtime audit report (JSON)
+        run: |
+          set -euo pipefail
+          pip-audit -r requirements.lock.txt --format json -o artifacts/pip-audit-runtime.json || true
+
+      - name: Collect development audit report (JSON)
+        run: |
+          set -euo pipefail
+          pip-audit -r requirements-dev.lock.txt --format json -o artifacts/pip-audit-dev.json || true
... (hunk trimmed)
```
### `deploy/job_bronze_price_target_data.yaml`
```diff
@@ -43,6 +43,10 @@ properties:
         value: INFO
       - name: DISABLE_DOTENV
         value: "true"
+      - name: BACKFILL_START_DATE
+        value: ${BACKFILL_START_DATE}
+      - name: BACKFILL_END_DATE
+        value: ${BACKFILL_END_DATE}
       - name: DEBUG_SYMBOLS
         value: "${DEBUG_SYMBOLS}"
       - name: SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID
```
### `deploy/job_gold_market_data.yaml`
```diff
@@ -44,6 +44,10 @@ properties:
         value: INFO
       - name: DISABLE_DOTENV
         value: "true"
+      - name: BACKFILL_START_DATE
+        value: ${BACKFILL_START_DATE}
+      - name: BACKFILL_END_DATE
+        value: ${BACKFILL_END_DATE}
       - name: SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID
         value: ${AZURE_SUBSCRIPTION_ID}
       - name: SYSTEM_HEALTH_ARM_RESOURCE_GROUP
```

## Appendix
### Tool Run Status
- `lint` `python3 -m ruff check .` -> **passed** (exit 0)
```text
All checks passed!
```
- `test_fast` `python3 -m pytest -q tests/tasks tests/market_data tests/finance_data tests/earnings_data tests/price_target_data` -> **passed** (exit 0)
```text
..................................................................................................................................
=============================== warnings summary ===============================
tests/earnings_data/test_feature_generator.py::test_compute_features_adds_expected_columns
tests/earnings_data/test_feature_generator.py::test_compute_features_rolls_over_quarters
tests/earnings_data/test_feature_generator.py::test_compute_features_handles_divide_by_zero
tests/earnings_data/test_gold_earnings_data.py::test_compute_features
  /mnt/c/Users/rdpro/Projects/AssetAllocation/tasks/earnings_data/gold_earnings_data.py:142: DeprecationWarning: DataFrameGroupBy.apply operated on the grouping columns. This behavior is deprecated, and in a future version of pandas the grouping columns will be excluded from the operation. Either pass `include_groups=False` to exclude the groupings or explicitly select the grouping columns after groupby to silence this warning.
    out = out.groupby("symbol", sort=False, group_keys=False).apply(

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
130 passed, 4 warnings in 7.29s
```
