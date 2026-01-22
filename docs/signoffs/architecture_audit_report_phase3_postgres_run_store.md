### 1. Executive Summary
- Phase 3 moves **backtest run-state** into Postgres (`backtest.runs`) while preserving the architectural constraint that **artifacts remain in ADLS/Blob** and Postgres holds only transactional/queryable state.
- The integration is narrow and low-risk: a new `PostgresRunStore` implements the existing store interface and is selected via `BACKTEST_RUN_STORE_MODE=postgres`, with fast-fail validation when `BACKTEST_POSTGRES_DSN` is missing.
- The primary architectural risk remains the **single-replica requirement**: the service executes runs in-process, so scaling replicas can duplicate work and corrupt run state; this is mitigated by keeping `maxReplicas: 1` in Container Apps.
- Near-term priorities: ensure Postgres migrations are applied in the target DB, validate readiness behavior (`/readyz`), and run a dev smoke to confirm run listing/update semantics.

### 2. System Map (High-Level)
- **Backtest Service (FastAPI)**
  - Accepts run submissions (`POST /backtests`) and persists run metadata/state via a configurable store (`asset_allocation/backtest/service/app.py:105`).
  - Executes runs in-process via a threadpool (`asset_allocation/backtest/service/job_manager.py:1`), updating run state throughout execution.
- **Run Store Backends**
  - `sqlite`: local file (`asset_allocation/backtest/service/run_store.py:1`).
  - `adls`: JSON blobs per run (`asset_allocation/backtest/service/adls_run_store.py:1`).
  - `postgres` (Phase 3): transactional run-state in `backtest.runs` (`asset_allocation/backtest/service/postgres_run_store.py:34`, `deploy/sql/postgres/migrations/0004_backtest_runs.sql:3`).
- **Artifacts**
  - Written locally under `BACKTEST_OUTPUT_DIR` and optionally uploaded to ADLS via `BACKTEST_ADLS_RUNS_DIR` / `output.adls_dir` (unchanged behavior; run store stores only pointers).

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None identified within Phase 3 scope.

#### 3.2 Major
- **[Multi-replica execution hazard remains]**
  - **Evidence:** runs execute in-process (threadpool) and the deployment keeps `maxReplicas: 1` (`asset_allocation/backtest/service/job_manager.py:1`, `deploy/app_backtest_api.yaml:83`).
  - **Why it matters:** scaling the service beyond one replica can duplicate run execution and create inconsistent run-state updates.
  - **Recommendation:** keep `maxReplicas: 1` until a distributed lease/queue model is implemented; document this as a hard operational constraint.
  - **Acceptance Criteria:** Container Apps manifest continues to enforce single replica; any future change to allow >1 replica includes a distributed coordination mechanism and QA coverage.
  - **Owner Suggestion:** Architecture Review Agent + Delivery Engineer Agent + QA Release Gate Agent

- **[Migrations/privileges are a deployment gate]**
  - **Evidence:** `PostgresRunStore.init_db()` checks for `backtest.runs` and fails readiness if missing (`asset_allocation/backtest/service/postgres_run_store.py:44`).
  - **Why it matters:** deploying the service in Postgres mode without applying migrations (or without correct role grants) will fail startup/readiness.
  - **Recommendation:** treat `deploy/apply_postgres_migrations.ps1` as a required pre-deploy step; ensure `backtest_service` role has CRUD on `backtest.runs` (`deploy/sql/postgres/migrations/0004_backtest_runs.sql:26`).
  - **Acceptance Criteria:** migration 0004 applied in the target DB; `/readyz` returns 200 when `BACKTEST_RUN_STORE_MODE=postgres`; the service can create and list runs successfully.
  - **Owner Suggestion:** Delivery Engineer Agent + DevOps Agent + QA Release Gate Agent

#### 3.3 Minor
- **[Connection management may need pooling at higher scale]**
  - **Evidence:** `PostgresRunStore` opens a new connection per operation (`asset_allocation/backtest/service/postgres_run_store.py:41`).
  - **Why it matters:** at higher QPS or larger concurrency, frequent connect/disconnect can add latency and increase DB connection churn.
  - **Recommendation:** keep current approach for the initial rollout; add a pool only when needed, driven by observed connection counts/latency.
  - **Acceptance Criteria:** basic monitoring/spot checks show stable DB connections and acceptable latency under expected load.
  - **Owner Suggestion:** Delivery Engineer Agent / DevOps Agent

### 4. Architectural Recommendations
- Maintain the “serving/state split”: keep run-state queryable in Postgres and avoid duplicating large artifacts in Postgres.
- Preserve the explicit config gate (`BACKTEST_RUN_STORE_MODE`) and fail-fast semantics (`BACKTEST_POSTGRES_DSN` required) to avoid partial, silent misconfiguration.
- If multi-replica execution is desired, adopt a queue + worker model or add distributed leasing keyed by `run_id` before increasing replicas.

### 5. Operational Readiness & Observability
- **Readiness:** `/readyz` now checks connectivity for stores exposing `ping()` (`asset_allocation/backtest/service/app.py:207`).
- **Recovery:** queued/running reconciliation on startup is preserved for Postgres (`asset_allocation/backtest/service/postgres_run_store.py:66`).
- **Security:** DSN is treated as a secret and is not printed in logs; only environment variable presence is validated (`asset_allocation/backtest/service/settings.py:223`).

### 6. Refactoring Examples (Targeted)
- Run-store selection remains localized to the app lifespan:
  - `asset_allocation/backtest/service/app.py:105` selects `PostgresRunStore` based on `run_store_mode`.

### 7. Evidence & Telemetry
- Files reviewed:
  - `asset_allocation/backtest/service/app.py:105`
  - `asset_allocation/backtest/service/settings.py:107`
  - `asset_allocation/backtest/service/postgres_run_store.py:34`
  - `deploy/sql/postgres/migrations/0004_backtest_runs.sql:3`
  - `deploy/app_backtest_api.yaml:73`
- Commands run:
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_postgres_run_store_mode.py` → **2 passed**
  - `PYTHONPATH=$PWD pytest -q tests/backtest/test_phase3_service_api.py` → **8 passed**

