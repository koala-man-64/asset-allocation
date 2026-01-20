# QA Verification Report

## 1. Executive Summary
- **Overall confidence level:** High
- **Scope verified:** Phase 3 wrapper migrations/parity for `ConfiguredStrategy`, docs/examples, and a correctness fix for executing “liquidate all” targets.
- **Top remaining risks:** No CI evidence attached in this report (local-only); optional dependency drift (`verify_imports.py` vs installed deps).

## 2. Test Matrix (Functionality Coverage)
| Feature/Flow | Risk | Test Type | Local | Dev | Prod | Status | Notes |
|---|---|---|---|---|---|---|---|
| Legacy → configured wrapper: `TopNSignalStrategy` first-pick parity | Low | Automated (pytest) | ✅ | N/A | N/A | Pass | `tests/backtest/test_configured_strategy_legacy_parity.py` |
| Legacy → configured wrapper: `LongShortTopNStrategy` stop-loss exit on non-rebalance day | High | Automated (pytest) | ✅ | N/A | N/A | Pass | Validates daily exit evaluation + liquidation execution |
| Legacy → configured wrapper: `LongShortTopNStrategy` partial-exit scale emission on non-rebalance day | Medium | Automated (pytest) | ✅ | N/A | N/A | Pass | Strategy-level contract parity (`decision.scales`) |
| Engine executes explicit empty targets (`{}`) | High | Automated (pytest) | ✅ | N/A | N/A | Pass | Covered by stop-loss parity test (single-symbol liquidation) |
| ConfiguredStrategy debug artifacts documentation + examples | Low | Manual review | ✅ | N/A | N/A | Pass | `docs/backtesting_guide.md`, `backtests/example_configured_*.yaml` |

## 3. Test Cases (Prioritized)
- **Configured wrapper parity: Top-N selection**
  - Purpose: configured pipeline can replicate legacy “top-N by column” behavior.
  - Preconditions/data: 2 symbols, deterministic signals ranking, rebalance every N days.
  - Steps: run legacy config and configured config; inspect `trades.csv`.
  - Expected results: both choose the same symbol on the first trade.
  - Failure signals: mismatched first trade symbol; missing trades.

- **Configured wrapper parity: stop-loss exit on non-rebalance day**
  - Purpose: ensure exits execute daily even when rebalance gate is closed, including full liquidation.
  - Preconditions/data: 1 symbol, deterministic stop-loss trigger via intraday low, long-only sizing.
  - Steps: run legacy and configured backtests; compare normalized trades.
  - Expected results: matching buy + sell trades; sell occurs after stop-loss day close.
  - Failure signals: missing sell trade; position never exits.

- **Configured wrapper parity: partial-exit emits scales on non-rebalance day**
  - Purpose: ensure scale changes are emitted (and not suppressed by rebalance gating).
  - Preconditions/data: 1 symbol, partial-exit rule after N trading days, stable prices.
  - Steps: call `on_bar` for legacy and configured strategies across dates with a held position snapshot.
  - Expected results: both return `StrategyDecision` with `scales["AAA"] ~= 0.5` on trigger day.
  - Failure signals: decision is `None` or scale missing/incorrect.

## 4. Automated Tests Added/Updated (If applicable)
- Added: `tests/backtest/test_configured_strategy_legacy_parity.py`
  - Asserts parity between legacy strategies and equivalent configured strategy configs.
  - Includes an engine-run regression for full liquidation execution.

## 5. Environment Verification
### Local (Required)
- Commands run:
  - `python3 -m pytest -q`
  - `python3 -m pytest -q tests/backtest/test_configured_strategy_legacy_parity.py`
- Expected outputs:
  - `pytest` exits 0; all tests pass.
- Troubleshooting notes:
  - `python3 verify_imports.py` currently fails due to missing `bs4` (BeautifulSoup); this is outside pytest coverage and should be aligned with dependency declarations if it’s intended to be a required check.

### Dev (Optional)
- Not executed (no dev endpoints or environment details provided).

### Prod (Optional, Safe-Only)
- Not executed (no production verification requested or environment details provided).

## 6. CI/CD Verification (If applicable)
- Workflows reviewed:
  - `.github/workflows/run_tests.yml` (tests + UI build)
  - `.github/workflows/lint_workflows.yml` (actionlint)
  - `.github/workflows/deploy.yml`, `.github/workflows/trigger_all_jobs.yml` (Azure deploy/ops)
- Notes:
  - Workflows declare `permissions:` and use pinned action SHAs for key `uses:` steps.
  - `run_tests.yml` runs `pytest` and builds the UI using a pinned `node:20-bookworm-slim` image digest.

## 7. Release Readiness Gate
- **Decision:** Pass (for the scoped Phase 3 changes)
- **Evidence:** local test suite green; added targeted parity/regression tests covering the highest-risk behavior (non-rebalance exits and full liquidation execution).
- **Rollback triggers:** unexpected behavior in strategies that liquidate all positions (now executed instead of silently skipped); monitor trade counts and end-of-run exposure for strategies with tight risk exits.

## 8. Evidence & Telemetry
- `python3 -m pytest -q` → **78 passed** (local)
- `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out audit_snapshot.json` → updated inventory snapshot

## 9. Gaps & Recommendations
- Add a dedicated engine test that asserts explicit empty targets (`{}`) liquidate all holdings at the next open (beyond the parity coverage).
- Decide whether `verify_imports.py` is a required gate; if so, align optional dependencies (e.g., `bs4`) and document required extras.
- Consider enabling Dependabot (`.github/dependabot.yml`) for Python/Node dependency update automation.

## 10. Handoffs (Only if needed)
- `Handoff: Project Workflow Auditor Agent` -- ensure dependency governance and optional-deps policy are explicit (e.g., `verify_imports.py` vs lockfiles).
