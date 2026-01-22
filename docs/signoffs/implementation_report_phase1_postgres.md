# Implementation Report

## 1. Execution Summary
- Implemented Phase 1 foundations for introducing Azure Postgres as a serving/state layer: provisioning automation, repo-owned schema scaffolding (migrations), and deploy-time secret wiring for Container Apps resources.
- Enforced a correctness guardrail for the backtest API by capping replicas to 1 while it executes runs in-process.
- Provisioning script can optionally apply migrations and create least-privileged application roles (so DSNs do not need to use the admin user).
- Provisioning script now retries server creation in fallback regions when a location is restricted, and emits computed DSNs directly (avoids reliance on `az ... --query ... -o tsv` output on Windows).
- Re-running with `-CreateAppUsers` sets the app role passwords to the provided/generated values (intentional password rotation for idempotent DSN output).
- Provisioning script now treats “not found” checks (`db show`, firewall rule `show`) as non-fatal under PowerShell and proceeds to create missing resources.

**Out of scope**
- Any application code that writes/reads Postgres (signals dual-write, Postgres run store, Postgres signal reads).
- Private endpoint/VNet integration for Postgres (documented as a follow-up).

## 2. Architectural Alignment Matrix
- **Requirement:** “ADLS/Delta remains canonical; Postgres is rebuildable.”
  - **Implementation:** `deploy/sql/postgres/migrations/*` + `deploy/apply_postgres_migrations.ps1`
  - **Status:** Complete (Phase 1 baseline).
  - **Notes:** Schema is minimal and versioned; no runtime dependency introduced yet.

- **Requirement:** “Postgres provisioning is automated and repeatable.”
  - **Implementation:** `deploy/provision_azure_postgres.ps1`
  - **Status:** Complete.
  - **Notes:** Uses Azure CLI; supports firewall rule configuration and redacted outputs by default.

- **Requirement:** “Deployment wiring supports secrets safely.”
  - **Implementation:** `.github/workflows/deploy.yml`, `deploy/job_platinum_ranking.yaml`, `deploy/app_backtest_api.yaml`
  - **Status:** Complete.
  - **Notes:** DSNs are injected only in deploy workflow (push to `main` / manual dispatch).

- **Requirement:** “Backtest run-state must not be corrupted by replicas.”
  - **Implementation:** `deploy/app_backtest_api.yaml` `scale.maxReplicas: 1`
  - **Status:** Complete (Option A).
  - **Notes:** True multi-instance execution requires leases/workerization (future phase).

## 3. Change Set
**Added**
- `deploy/provision_azure_postgres.ps1`
- `deploy/apply_postgres_migrations.ps1`
- `deploy/sql/postgres/migrations/0001_schema_migrations.sql`
- `deploy/sql/postgres/migrations/0002_init_schemas.sql`
- `docs/postgres_phase1.md`
- `docs/signoffs/implementation_report_phase1_postgres.md`

**Modified**
- `.github/workflows/deploy.yml` (inject DSN secrets for `envsubst`)
- `deploy/job_platinum_ranking.yaml` (add `POSTGRES_DSN` secret ref)
- `deploy/app_backtest_api.yaml` (add `BACKTEST_POSTGRES_DSN` secret ref; set `maxReplicas: 1`)

**Key Interfaces**
- GitHub secrets (deploy workflow only):
  - `RANKING_POSTGRES_DSN`
  - `BACKTEST_POSTGRES_DSN`
- Container Apps env vars (unused until later phases):
  - Ranking job: `POSTGRES_DSN`
  - Backtest API: `BACKTEST_POSTGRES_DSN`

## 4. Code Implementation
### Replica safety guardrail
```yaml
# deploy/app_backtest_api.yaml
scale:
  minReplicas: 0
  maxReplicas: 1
```

### Migration runner (Dockerized psql option)
```powershell
pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql
```

## 5. Observability & Operational Readiness
- Phase 1 adds `docs/postgres_phase1.md` documenting provisioning, migrations, and secret wiring.
- Recommended next signals (Phase 2+): ingestion watermark, Postgres connectivity readiness, and connection budget caps.

## 6. Cloud-Native Configuration (If applicable)
- Container Apps YAML manifests now accept Postgres DSN secrets via `secrets:` + `secretRef` wiring.
- Deploy workflow injects DSNs only for deploy steps (not PR workflows).

## 7. Verification Steps
- YAML parsing sanity:
  - `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/deploy.yml'))"`
  - `python3 -c "import yaml; yaml.safe_load(open('deploy/app_backtest_api.yaml'))"`
  - `python3 -c "import yaml; yaml.safe_load(open('deploy/job_platinum_ranking.yaml'))"`
- PowerShell syntax sanity (Windows PowerShell):
  - `powershell.exe -NoProfile -Command "[void][System.Management.Automation.Language.Parser]::ParseFile('deploy/provision_azure_postgres.ps1',[ref]$null,[ref]$null)"`

- Provision + migrate + create app users (staging):
  - `pwsh deploy/provision_azure_postgres.ps1 -SubscriptionId <id> -ServerName <name> -AllowAzureServices -ApplyMigrations -CreateAppUsers -UseDockerPsql -EmitSecrets`

## 8. Risks & Follow-ups
- If Postgres DSN secrets are not set, deploy-time `envsubst` will render empty values; ensure secrets exist before enabling these deploy paths.
- Public networking baseline (DEC-002) requires strict firewall rules and TLS-only DSNs; plan a migration to private endpoint/VNet integration.
- Backtest service remains single-replica until leases/workerization is implemented.

## 9. Evidence & Telemetry
- YAML parse: OK
- PowerShell parse: OK
