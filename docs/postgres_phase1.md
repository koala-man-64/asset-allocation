# Postgres Serving Split — Phase 1 (Foundations)

Phase 1 establishes the **foundations** for adding Azure Postgres as a *rebuildable serving/state layer* while keeping **ADLS/Delta canonical**.

This phase **does not** enable Postgres writers/readers in application code yet; it provisions infrastructure, versioned schema scaffolding, and safe secret wiring.

## Decisions (Phase 1 defaults)

- **DEC-001 Backtest scaling model (historical):** *Single replica* while runs execute in-process.
  - Rationale: when backtest execution lived in-process, scaling replicas could corrupt run state and create duplicate execution.
  - Current status: backtest in-process routing was removed; `deploy/app_api.yaml` now scales for ingestion reliability (`minReplicas: 1`, `maxReplicas: 3`).
- **DEC-002 Postgres networking model:** *Public endpoint + firewall rules + TLS-required DSNs* (cost-min baseline).
  - Rationale: fastest path to an operational DB; private endpoint/VNet integration can be added later.

## What Phase 1 adds

- **Provisioning script:** `scripts/provision_azure_postgres.ps1`
- **Migrations baseline:**
  - `deploy/sql/postgres/migrations/0001_schema_migrations.sql`
  - `deploy/sql/postgres/migrations/0002_init_schemas.sql`
- **Migration runner:** `scripts/apply_postgres_migrations.ps1` (supports local `psql` or Dockerized `psql`)
- **Deploy wiring (secrets only; unused until later phases):**
  - Backtest API: `deploy/app_api.yaml` → `POSTGRES_DSN` secret ref
  - GitHub deploy workflow: `.github/workflows/deploy.yml` passes the secrets to `envsubst`

## Required GitHub Secrets (for deploy workflow)

Add these secrets in your GitHub repo settings:

- `POSTGRES_DSN` — DSN for the API/backtest service role.

**Single Postgres server + database**

This repo assumes **one Postgres server** and **one database** (with multiple schemas like `core` and `backtest`).
Schemas are managed via repo migrations (e.g., `core`, `backtest`, `monitoring`, `gold`, `platinum`).

Recommended DSN format:

- `postgresql://<user>:<password>@<server>.postgres.database.azure.com:5432/<db>?sslmode=require`

## Provision Postgres (Azure)

Run (PowerShell):

```powershell
pwsh scripts/provision_azure_postgres.ps1 `
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
- If you see: “The location is restricted for provisioning of flexible servers”, pick another region (often `eastus2`, `centralus`, `westus2`).
  - Quick check: `az postgres flexible-server list-skus -l <region> --query "[0].reason" -o tsv` (empty output means “allowed”).
- The provisioning script retries server creation in `-LocationFallback` regions if the first location is restricted.
- If you see a `ResourceNotFound` for the database during `db show`, the script will create the database and continue (expected for first-time setup when the server exists but the database does not).
- If you re-run with `-CreateAppUsers`, the script will set (rotate) the app role password to the `-BacktestServicePassword` value (or a newly generated value if omitted).
- `-AllowAzureServices` adds a firewall rule `0.0.0.0` (Azure-internal access). For tighter rules, use `-AllowIpRangeStart/-AllowIpRangeEnd`.
- Without `-EmitSecrets`, outputs redact the admin password and connection strings.

## Apply migrations (schema baseline)

You need a DSN with sufficient privileges (typically the admin DSN).

Using Dockerized `psql` (recommended if `psql` is not installed locally):

```powershell
pwsh scripts/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>" -UseDockerPsql
```

Using local `psql`:

```powershell
pwsh scripts/apply_postgres_migrations.ps1 -Dsn "<ADMIN_DSN>"
```

## Deploy notes

- The deploy workflow renders Container Apps YAML via `envsubst` (temporary files are deleted).
- Do not add Postgres DSN secrets to PR workflows; keep them confined to `.github/workflows/deploy.yml` (push to `main` / manual dispatch).

## Next phases (high level)

- **Phase 2:** Harden the backtest run store and operational guardrails.
