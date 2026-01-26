# Postgres Serving Split — Phase 1 (Foundations)

Phase 1 establishes the **foundations** for adding Azure Postgres as a *rebuildable serving/state layer* while keeping **ADLS/Delta canonical**.

This phase **does not** enable Postgres writers/readers in application code yet; it provisions infrastructure, versioned schema scaffolding, and safe secret wiring.

## Decisions (Phase 1 defaults)

- **DEC-001 Backtest scaling model:** *Single replica* while runs execute in-process.
  - Rationale: the backtest API currently executes jobs in-process; scaling replicas can corrupt run state and create duplicate execution.
  - Enforcement: `deploy/app_backtest_api.yaml` sets `maxReplicas: 1`.
- **DEC-002 Postgres networking model:** *Public endpoint + firewall rules + TLS-required DSNs* (cost-min baseline).
  - Rationale: fastest path to an operational DB; private endpoint/VNet integration can be added later.

## What Phase 1 adds

- **Provisioning scripts:**
  - Combined infra + Postgres: `deploy/provision_azure_all.ps1`
  - Postgres-only: `deploy/provision_azure_postgres.ps1`
- **Migrations baseline:**
  - `deploy/sql/postgres/migrations/0001_schema_migrations.sql`
  - `deploy/sql/postgres/migrations/0002_init_schemas.sql`
- **Migration runner:** `deploy/apply_postgres_migrations.ps1` (supports local `psql` or Dockerized `psql`)
- **Deploy wiring (secrets only; unused until later phases):**
  - Ranking job: `deploy/job_platinum_ranking.yaml` → `POSTGRES_DSN` secret ref
  - Backtest API: `deploy/app_backtest_api.yaml` → `POSTGRES_DSN` secret ref
  - GitHub deploy workflow: `.github/workflows/deploy.yml` passes the secrets to `envsubst`

## Required GitHub Secrets (for deploy workflow)

Add these secrets in your GitHub repo settings:

- `POSTGRES_DSN` — DSN for the ranking job (future Phase 2 writer).
- `POSTGRES_DSN` — DSN for the backtest API (future Phase 3 run store).

**Single Postgres server + database**

This repo assumes **one Postgres server** and **one database** (with multiple schemas like `ranking` and `backtest`).
`POSTGRES_DSN` and `POSTGRES_DSN` should therefore point to the **same host/port/database** and typically
only differ by **username/password** (separate least-privileged roles).

Recommended DSN format:

- `postgresql://<user>:<password>@<server>.postgres.database.azure.com:5432/<db>?sslmode=require`

## Provision Postgres (Azure)

Run (PowerShell):

```powershell
# Recommended: provision core infra + Postgres in one step
pwsh deploy/provision_azure_all.ps1 `
  -Location "eastus2" `
  -SkuName "standard_b1ms" `
  -Tier "Burstable" `
  -LocationFallback @("eastus2","centralus","westus2") `
  -ServerName "<server-name>" `
  -AllowAzureServices `
  -ApplyMigrations `
  -CreateAppUsers `
  -UseDockerPsql `
  -EmitSecrets
```

Or, Postgres-only:

```powershell
pwsh deploy/provision_azure_postgres.ps1 `
  -SubscriptionId "<SUBSCRIPTION_ID>" `
  -Location "eastus2" `
  -SkuName "standard_b1ms" `
  -Tier "Burstable" `
  -LocationFallback @("eastus2","centralus","westus2") `
  -ServerName "<server-name>" `
  -AllowAzureServices `
  -ApplyMigrations `
  -CreateAppUsers `
  -UseDockerPsql `
  -EmitSecrets
```

Notes:
- `deploy/provision_azure_all.ps1` and `deploy/provision_azure_postgres.ps1` use `SYSTEM_HEALTH_ARM_SUBSCRIPTION_ID` (or `AZURE_SUBSCRIPTION_ID` / `SUBSCRIPTION_ID`) from the repo root `.env` when `-SubscriptionId` is omitted.
- If you see: “The location is restricted for provisioning of flexible servers”, pick another region (often `eastus2`, `centralus`, `westus2`).
  - Quick check: `az postgres flexible-server list-skus -l <region> --query "[0].reason" -o tsv` (empty output means “allowed”).
- The provisioning script retries server creation in `-LocationFallback` regions if the first location is restricted.
- If you see a `ResourceNotFound` for the database during `db show`, the script will create the database and continue (expected for first-time setup when the server exists but the database does not).
- If you re-run with `-CreateAppUsers`, the script will set (rotate) the app role passwords to the `-RankingWriterPassword/-BacktestServicePassword` values (or newly generated values if omitted).
- `-AllowAzureServices` adds a firewall rule `0.0.0.0` (Azure-internal access). For tighter rules, use `-AllowIpRangeStart/-AllowIpRangeEnd`.
- Without `-EmitSecrets`, outputs redact the admin password and connection strings.

## Apply migrations (schema baseline)

You need a DSN with sufficient privileges (typically the admin DSN).

Using Dockerized `psql` (recommended if `psql` is not installed locally):

```powershell
pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql
```

Using local `psql`:

```powershell
pwsh deploy/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>"
```

## Deploy notes

- The deploy workflow renders Container Apps YAML via `envsubst` (temporary files are deleted).
- Do not add Postgres DSN secrets to PR workflows; keep them confined to `.github/workflows/deploy.yml` (push to `main` / manual dispatch).

## Next phases (high level)

- **Phase 2:** Dual-write signals into Postgres (Delta remains canonical), add ingestion watermark.
- **Phase 3:** Move backtest run-state to Postgres (plus correctness under the single-replica model, or add leases for multi-instance).
- **Phase 4:** Optional Postgres signal reads + hardening/observability.
