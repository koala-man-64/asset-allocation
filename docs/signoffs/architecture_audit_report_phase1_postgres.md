### 1. Executive Summary
- Phase 1 establishes Postgres foundations (provisioning, migrations, and deploy-time secret wiring) without changing canonical data ownership: ADLS/Delta remains the source of truth and Postgres remains unused by runtime code until later phases.
- The highest-risk architectural issue identified earlier—backtest service replica safety while executing runs in-process—has been addressed at the deployment layer by enforcing `maxReplicas: 1` for the backtest API.
- The chosen networking baseline is cost-minimizing (public endpoint + firewall rules + TLS-required DSNs). This is acceptable for Phase 1 if paired with least privilege and a follow-up backlog item to migrate to private networking.
- Near-term priorities: keep Postgres schema versioned in-repo (done), add least-privileged DB roles and secret rotation runbook (Phase 1 provisioning follow-up), and only enable Postgres readers after ingestion watermarking exists (Phase 2/4).

### 2. System Map (High-Level)
- **Provisioning**
  - `deploy/provision_azure_postgres.ps1`: creates Postgres Flexible Server + database + firewall rules (Azure CLI); can optionally apply migrations and create least-privileged app roles.
  - `deploy/apply_postgres_migrations.ps1`: applies versioned SQL migrations using `psql` (local or Dockerized).
- **Schema (repo-owned)**
  - `deploy/sql/postgres/migrations/0001_schema_migrations.sql`: creates `public.schema_migrations`.
  - `deploy/sql/postgres/migrations/0002_init_schemas.sql`: creates `core`, `ranking`, `backtest` schemas.
- **Deployment wiring**
  - Ranking job: `deploy/job_platinum_ranking.yaml` adds `POSTGRES_DSN` (secret ref, unused until Phase 2).
  - Backtest API: `deploy/app_backtest_api.yaml` adds `BACKTEST_POSTGRES_DSN` (secret ref, unused until Phase 3) and enforces `maxReplicas: 1`.
  - CI deploy: `.github/workflows/deploy.yml` passes Postgres DSN secrets to `envsubst` in deploy steps.

### 3. Findings (Triaged)
#### 3.1 Critical (Must Fix)
- None for Phase 1 change set.

#### 3.2 Major
- **[Public endpoint is acceptable only with explicit follow-up to private networking]**
  - **Evidence:** `docs/postgres_phase1.md` documents DEC-002 baseline (public+TLS).
  - **Why it matters:** Public endpoints increase exposure and require strict firewall/credential hygiene.
  - **Recommendation:** Track a backlog item to move to private endpoint + VNet integration for Container Apps.
  - **Acceptance Criteria:** Private networking plan documented with target date; Postgres firewall rules are least-privilege (no broad ranges beyond what’s required).
  - **Owner Suggestion:** DevOps Agent

- **[Deploy-time envsubst writes secrets into temporary YAML]**
  - **Evidence:** `.github/workflows/deploy.yml` uses `envsubst` to render `deploy/*.yaml` containing `secrets:` values.
  - **Why it matters:** Any accidental logging of rendered YAML can leak secrets.
  - **Recommendation:** Keep temp YAML deletion (already present), avoid printing rendered files, and keep DSNs confined to deploy workflow only.
  - **Acceptance Criteria:** No CI logs include DSN values; temp YAML is removed; no DSN secrets referenced in PR workflows.
  - **Owner Suggestion:** Project Workflow Auditor Agent / DevOps Agent

#### 3.3 Minor
- **[Migrations are minimal (schemas only)]**
  - **Evidence:** `deploy/sql/postgres/migrations/0002_init_schemas.sql`.
  - **Why it matters:** Future phases will need tables/indexes; this is acceptable for Phase 1.
  - **Recommendation:** Add tables/indexes in Phase 2/3 migrations only when writers/readers land.
  - **Acceptance Criteria:** Phase 2/3 add tables via new migration files; no manual schema drift.
  - **Owner Suggestion:** Delivery Engineer Agent

### 4. Architectural Recommendations
- Keep Postgres strictly “derived serving/state” and remain rebuildable from Delta; do not introduce bidirectional dependencies.
- Keep backtest API single-replica until you add run ownership/leases and/or decouple execution into worker jobs.
- Add ingestion watermarking before any Postgres-based signal reads (Phase 4).

### 5. Operational Readiness & Observability
- Phase 1 adds operational runbook material in `docs/postgres_phase1.md` (provisioning + migrations + secrets wiring).
- Future observability requirements (Phase 2+): ingestion lag metrics/watermarks, DB connectivity readiness checks, and connection budget caps.

### 6. Refactoring Examples (Targeted)
- Deployment safety guardrail:
  - `deploy/app_backtest_api.yaml` sets `scale.maxReplicas: 1` to prevent replica-induced run-state corruption while runs execute in-process.

### 7. Evidence & Telemetry
- Files reviewed: `deploy/provision_azure_postgres.ps1`, `deploy/apply_postgres_migrations.ps1`, `deploy/sql/postgres/migrations/*.sql`, `deploy/app_backtest_api.yaml`, `deploy/job_platinum_ranking.yaml`, `.github/workflows/deploy.yml`, `docs/postgres_phase1.md`.
- Commands run:
  - YAML parse: `python3 -c "import yaml; yaml.safe_load(open(...))"` → OK
  - PowerShell parse: `powershell.exe ... Parser::ParseFile(...)` → OK
