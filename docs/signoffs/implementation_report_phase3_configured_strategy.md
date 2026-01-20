# Implementation Report

## 1. Execution Summary
- Implemented Phase 3 “wrapper migrations” for the new `ConfiguredStrategy` pipeline by adding legacy-to-configured config generators and parity/regression tests (exits + scales on non-rebalance days).
- Fixed a backtest execution gap where an explicit “liquidate all” target (`{}`) would not execute at the next open due to falsy `pending_targets` handling.
- Added documentation and example YAMLs for `strategy.type: configured`, including how to enable and interpret per-run debug artifacts.

**Out of scope**
- Building a full CLI “convert legacy YAML → configured YAML” tool.
- Large-scale refactors or re-architecture of the backtest engine beyond the liquidation execution fix.

## 2. Architectural Alignment Matrix
- **Requirement:** “Deliver ConfiguredStrategy pipeline end-to-end (engine unchanged).”
  - **Implementation:** `asset_allocation/backtest/configured_strategy/*` (already present) + Phase 3 migrations/tests/docs in this change set.
  - **Status:** Complete (engine has one targeted correctness fix; no interface changes).
  - **Notes:** Engine change is a correctness fix for full-liquidation targets; required to make “exit last holding” work.

- **Requirement:** “Exits/scales evaluate daily regardless of rebalance schedule.”
  - **Implementation:** Parity tests cover stop-loss exit and partial-exit scale emission on non-rebalance days.
  - **Status:** Complete.
  - **Notes:** Stop-loss parity validated via engine-run test; partial-exit parity validated at strategy contract level (decision emission + scales).

- **Requirement:** “Implement 1–2 legacy-strategy wrapper migrations to prove parity.”
  - **Implementation:** `asset_allocation/backtest/configured_strategy/legacy_migrations.py` provides wrappers for:
    - `TopNSignalStrategy`
    - `LongShortTopNStrategy`
  - **Status:** Complete.

- **Requirement:** “Documentation polish: example YAMLs + debug artifacts.”
  - **Implementation:** `docs/backtesting_guide.md`, `backtests/example_configured_topn.yaml`, `backtests/example_configured_vcp_breakout_long.yaml`.
  - **Status:** Complete.

## 3. Change Set
**Added**
- `asset_allocation/backtest/configured_strategy/legacy_migrations.py`
- `tests/backtest/test_configured_strategy_legacy_parity.py`
- `backtests/example_configured_topn.yaml`
- `backtests/example_configured_vcp_breakout_long.yaml`
- `docs/signoffs/implementation_report_phase3_configured_strategy.md`

**Modified**
- `asset_allocation/backtest/configured_strategy/holding.py` (optional held-score refresh)
- `asset_allocation/backtest/configured_strategy/selection.py` (allow `min_score: null` for Top-N selection)
- `asset_allocation/backtest/engine.py` (execute explicit empty targets by distinguishing “no trade scheduled” vs “empty target”)
- `docs/backtesting_guide.md`
- `audit_snapshot.json` (regenerated inventory)

**Key Interfaces**
- `holding_policy.replace_all.refresh_held_scores: bool` (default `false`)
- `holding_policy.replace_all.refresh_mode: "raw" | "abs_signed"` (default `"abs_signed"`)
- `selection.topn.min_score: null` now means “no threshold filter”

## 4. Code Implementation
### Key Fix: execute “liquidate all” targets
```python
# asset_allocation/backtest/engine.py
# None => no trade scheduled; dict (possibly empty) => explicit target weights
pending_targets: Optional[Dict[str, float]] = None

if i > 0 and pending_targets is not None:
    execution = broker.execute_target_weights(market, target_weights=pending_targets)
```

### Wrapper helpers (legacy → configured)
```python
# asset_allocation/backtest/configured_strategy/legacy_migrations.py
def configured_config_for_topn_signal_strategy(...)-> Dict[str, Any]: ...
def configured_config_for_long_short_topn_strategy(...)-> Dict[str, Any]: ...
```

### Configured holding policy option for legacy parity
```python
# asset_allocation/backtest/configured_strategy/holding.py
ReplaceAllPolicy(..., refresh_held_scores: bool = False, refresh_mode: Literal["raw","abs_signed"] = "abs_signed")
```

## 5. Observability & Operational Readiness
- `ConfiguredStrategy` can write per-decision-date debug artifacts under `backtest_results/<RUN_ID>/strategy_debug/` when enabled via `strategy.debug`.
- Existing `Reporter` artifacts (trades, daily metrics, parquet outputs) remain unchanged and serve as the primary run-level telemetry.

## 6. Cloud-Native Configuration (If applicable)
N/A for this change set (no new services or deployment manifests introduced).

## 7. Verification Steps
- Run full test suite: `python3 -m pytest -q`
- Run only parity tests: `python3 -m pytest -q tests/backtest/test_configured_strategy_legacy_parity.py`
- Regenerate repo/workflow snapshot: `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out audit_snapshot.json`

## 8. Risks & Follow-ups
- Engine behavior change: empty target dicts are now treated as actionable (liquidation) when produced by a strategy decision.
  - Follow-up: add a dedicated engine unit test for full-liquidation execution (empty targets) to prevent regressions.
- Optional dependencies: `verify_imports.py` fails locally due to missing `bs4` (BeautifulSoup) even though the pytest suite passes.
  - Follow-up: confirm whether `bs4` should be in `requirements-opt.txt` / documented as an optional extra.

## 9. Evidence & Telemetry
- `python3 -m pytest -q` → **78 passed** (local)
- `python3 .codex/skills/project-workflow-auditor-agent/scripts/audit_snapshot.py --repo . --out audit_snapshot.json` → wrote updated snapshot
