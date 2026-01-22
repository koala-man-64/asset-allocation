# Implementation Report

## 1. Execution Summary
- Implemented Phase 2 dual-write for **derived ranking signals**: Delta remains canonical and the ranking job optionally replicates monthly signal partitions into Postgres after Delta writes succeed.
- Added repo-owned Postgres schema migration for signal tables and a lightweight Postgres writer using COPY for efficient ingestion.
- Introduced explicit operational flags controlling failure semantics and verification behavior.

**Out of scope**
- Any data backfill/migration of historical months.
- Postgres-backed reads in backtest loader (Phase 4).
- Backtest run-state move to Postgres (Phase 3).
- Private networking/VNet integration for Postgres.

## 2. Architectural Alignment Matrix
- **Requirement:** “ADLS/Delta remains canonical; Postgres is rebuildable/derived.”
  - **Implementation:** Postgres replication runs only after Delta writes succeed (`scripts/ranking/signals.py:219`).
  - **Status:** Complete.
  - **Notes:** Postgres is not used as a source in this phase; no bidirectional coupling.

- **Requirement:** “Signals partition overwrite semantics match Delta (year_month).”
  - **Implementation:** Transactional `DELETE ... WHERE year_month = ?` + COPY insert (`scripts/ranking/postgres_signals.py:1`).
  - **Status:** Complete.
  - **Notes:** Mirrors `store_delta(... mode="overwrite", predicate=year_month)` semantics.

- **Requirement:** “Operational controls for failure/verification.”
  - **Implementation:** `POSTGRES_SIGNALS_WRITE_REQUIRED` and `POSTGRES_SIGNALS_VERIFY_COUNTS` (`scripts/ranking/signals.py:219`, `scripts/ranking/postgres_signals.py:1`).
  - **Status:** Complete.

## 3. Change Set
**Added**
- `deploy/sql/postgres/migrations/0003_ranking_signals.sql`
- `scripts/common/postgres.py`
- `scripts/ranking/postgres_signals.py`
- `docs/postgres_phase2.md`
- `docs/signoffs/implementation_report_phase2_postgres_signals.md`

**Modified**
- `scripts/ranking/signals.py` (dual-write after Delta writes)
- `requirements.txt` (psycopg dependency)
- `requirements.lock.txt` (psycopg dependency)
- `requirements-dev.lock.txt` (psycopg dependency)

**Key Interfaces**
- **Env vars**
  - `POSTGRES_DSN` — enables Postgres signal replication when set.
  - `POSTGRES_SIGNALS_WRITE_REQUIRED` — default `true`; when `false` replication errors do not fail the job.
  - `POSTGRES_SIGNALS_VERIFY_COUNTS` — default `false`; when `true` verifies stored row counts per month.
- **Postgres tables**
  - `ranking.ranking_signal`
  - `ranking.composite_signal_daily`
  - `ranking.signal_sync_state`

## 4. Code Implementation
- Postgres replication entrypoint:
  - `scripts/ranking/signals.py:219` reads `POSTGRES_DSN` and calls `scripts/ranking/postgres_signals.py:1`.
- Writer implementation:
  - Uses `COPY ... FROM STDIN` (psycopg) for bulk insert and overwrites partitions via delete+copy.
  - Writes a sync record to `ranking.signal_sync_state`.
- Schema:
  - `deploy/sql/postgres/migrations/0003_ranking_signals.sql:1` creates the minimal tables + indexes + role grants (if `ranking_writer` exists).

## 5. Observability & Operational Readiness
- **Logs:** Month + row counts are logged; DSNs are never printed (`scripts/ranking/postgres_signals.py:1`).
- **Operator visibility:** `ranking.signal_sync_state` provides a lightweight “last write” ledger per month.
- **Rollback:** unset `POSTGRES_DSN` or set `POSTGRES_SIGNALS_WRITE_REQUIRED=false` (`docs/postgres_phase2.md:1`).

## 6. Cloud-Native Configuration (If applicable)
- Ranking job already has `POSTGRES_DSN` secretRef wiring in `deploy/job_platinum_ranking.yaml:61` (Phase 1).
- No manifest changes required for Phase 2 beyond setting `secrets.RANKING_POSTGRES_DSN` in the deploy workflow environment.

## 7. Verification Steps
- Unit tests (local):
  - `PYTHONPATH=$PWD pytest -q tests/ranking`
- Apply migrations (dev/staging):
  - `pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql`
- Smoke verification (dev/staging):
  - Run ranking job once and validate:
    - `SELECT * FROM ranking.signal_sync_state ORDER BY synced_at DESC LIMIT 25;`
    - Counts by `year_month` in both signal tables.

## 8. Risks & Follow-ups
- If `POSTGRES_SIGNALS_WRITE_REQUIRED=false`, Postgres can drift from Delta; add a freshness/alert signal before enabling readers (Phase 4).
- Consider tightening DB privileges after schema stabilizes (remove `CREATE` privilege on `ranking` schema for the writer role if not needed).

## 9. Evidence & Telemetry
- `PYTHONPATH=$PWD pytest -q tests/ranking` → **11 passed**
- Dependency availability check:
  - `python3 -m pip install --dry-run psycopg==3.2.3`
  - `python3 -m pip install --dry-run psycopg-binary==3.2.3`

