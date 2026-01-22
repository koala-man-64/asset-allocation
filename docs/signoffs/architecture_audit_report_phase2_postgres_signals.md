### 1. Executive Summary
- Phase 2 enables **dual-write of derived ranking signals** into Postgres while preserving the architectural constraint that **ADLS/Delta remains canonical** and Postgres remains a rebuildable serving layer.
- The integration point is intentionally narrow: Postgres replication happens **after** successful Delta writes in `materialize_signals_for_year_month()`, so canonical data ownership and backfill semantics remain unchanged.
- The primary architectural risks are **failure semantics** (whether Postgres write failures should fail the ranking job) and **drift risk** if best-effort mode is enabled. Both are mitigated with explicit env flags and idempotent “delete month + insert month” semantics.
- Near-term priority: deploy Phase 2 with a staged canary (single `year_month`) and validate Postgres counts/state before enabling any Postgres readers (Phase 4).

### 2. System Map (High-Level)
- **Canonical pipeline (unchanged)**
  - Ranking runner computes rankings and materializes signals per month (`scripts/ranking/runner.py:465` → `scripts/ranking/signals.py:219`).
  - Signals and composite signals are written to Delta with `mode="overwrite"` scoped by `predicate=year_month` (`scripts/ranking/signals.py:253`).
- **New derived replication (Phase 2)**
  - Postgres schema for signals is introduced via migration `deploy/sql/postgres/migrations/0003_ranking_signals.sql:1`.
  - Optional Postgres write is executed after Delta writes succeed (`scripts/ranking/signals.py:219`) using `POSTGRES_DSN`.
  - Replication is transactional and idempotent per month: `DELETE ... WHERE year_month = ?` then COPY insert into:
    - `ranking.ranking_signal`
    - `ranking.composite_signal_daily`
    - state tracked via `ranking.signal_sync_state`
    (`scripts/ranking/postgres_signals.py:1`).

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None identified for Phase 2 scope.

#### 3.2 Major
- **[Failure semantics: Delta succeeds but Postgres fails]**
  - **Evidence:** Postgres replication occurs after Delta writes (`scripts/ranking/signals.py:219`).
  - **Why it matters:** If Postgres write fails, Delta remains correct but Postgres can lag or drift; if job fails, retries may re-run compute and reattempt Postgres.
  - **Recommendation:** Keep default “required” mode so Postgres write failure fails the job when `POSTGRES_DSN` is set, and allow explicit opt-out via `POSTGRES_SIGNALS_WRITE_REQUIRED=false`.
  - **Acceptance Criteria:** Documented flags (`docs/postgres_phase2.md:1`); job logs clearly show Postgres replication attempt and failure reason without printing secrets.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

- **[Drift risk when best-effort mode is used]**
  - **Evidence:** `POSTGRES_SIGNALS_WRITE_REQUIRED` can allow continuing on errors (`scripts/ranking/signals.py:219`).
  - **Why it matters:** Postgres can diverge from Delta if replication fails but the job still succeeds.
  - **Recommendation:** If best-effort is required operationally, add monitoring on `ranking.signal_sync_state` freshness and error rates before enabling any readers.
  - **Acceptance Criteria:** A defined alerting signal exists (future phase) or an operator runbook step validates `ranking.signal_sync_state` recency.
  - **Owner Suggestion:** DevOps Agent / QA Release Gate Agent

#### 3.3 Minor
- **[Postgres replication performance considerations]**
  - **Evidence:** Phase 2 uses monthly delete + COPY insert (`scripts/ranking/postgres_signals.py:1`).
  - **Why it matters:** Large months could increase DB write time; however this is bounded and uses COPY.
  - **Recommendation:** Keep indexes minimal (already) and retain monthly overwrite semantics to avoid upsert bloat.
  - **Acceptance Criteria:** End-to-end job runtime remains within the existing ACA job timeout; no sustained DB CPU saturation from the ranking job.
  - **Owner Suggestion:** Delivery Engineer Agent / DevOps Agent

### 4. Architectural Recommendations
- Keep Postgres as a **derived** layer only; do not introduce any workflow where Postgres becomes the only copy of a required dataset.
- Add Postgres readers only after replication is stable and observable (Phase 4).
- Consider tightening DB role privileges once table set stabilizes (grant only what the writer needs).

### 5. Operational Readiness & Observability
- Logging: writer logs month + row counts; does not log DSN values (`scripts/ranking/postgres_signals.py:1`).
- Optional verification: `POSTGRES_SIGNALS_VERIFY_COUNTS=true` enforces count parity after write (`scripts/ranking/postgres_signals.py:1`).
- Operator visibility: `ranking.signal_sync_state` records last successful sync per month (`deploy/sql/postgres/migrations/0003_ranking_signals.sql:1`).

### 6. Refactoring Examples (Targeted)
- Postgres write is isolated behind a single call site after Delta writes:
  - `scripts/ranking/signals.py:219` calls `write_signals_for_year_month(...)` when `POSTGRES_DSN` is set.

### 7. Evidence & Telemetry
- Files reviewed:
  - `deploy/sql/postgres/migrations/0003_ranking_signals.sql:1`
  - `scripts/ranking/signals.py:219`
  - `scripts/ranking/postgres_signals.py:1`
  - `scripts/common/postgres.py:1`
  - `docs/postgres_phase2.md:1`
- Commands run:
  - `PYTHONPATH=$PWD pytest -q tests/ranking` → **11 passed**

