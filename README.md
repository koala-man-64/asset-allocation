# AssetAllocation

AssetAllocation is an Azure-oriented market data and operations platform. The repo combines Python ETL jobs, a FastAPI control and data service, and a React/Vite UI. It ingests market, finance, earnings, and price-target data into bronze, silver, and gold Delta tables on Azure Data Lake Storage, exposes inspection and admin APIs, and stores runtime controls such as debug symbols, runtime config overrides, and strategy definitions in Postgres.

## What Runs Here

- Data pipelines in `tasks/` materialize Bronze, Silver, and Gold datasets for the market, finance, earnings, and price-target domains.
- The FastAPI app in `api/service/app.py` serves `/api/data`, `/api/system`, `/api/strategies`, provider gateway endpoints, Swagger/OpenAPI, `/config.js`, and the realtime websocket.
- The React UI in `ui/` is the operator control plane for system health, data exploration, data quality, runtime config, debug symbols, symbol purge, Postgres exploration, and strategy configuration.
- Azure deployment uses one Container App with API and UI sidecars plus scheduled Container App Jobs under `deploy/job_*.yaml`.

## Quickstart

### Prerequisites

- Python 3.14
- Node.js and `pnpm` (the UI workspace pins `pnpm@10.28.1`)
- Azure Storage credentials and provider API keys if you want to run ETL jobs against real services
- `POSTGRES_DSN` if you want runtime config, debug-symbol, Postgres explorer, or strategy features

### Backend Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
python3 -m pip install -r requirements-dev.txt
cp .env.template .env
```

The backend runtime, CI, and container images are standardized on Python 3.14.

### Run the API Locally

```bash
python3 scripts/run_api_dev.py
```

Then open:

- `http://127.0.0.1:9000/api/docs`
- `http://127.0.0.1:9000/api/openapi.json`
- `http://127.0.0.1:9000/config.js`

### Run the UI Locally

```bash
cd ui
pnpm install
pnpm dev
```

The UI proxies API calls to `VITE_API_PROXY_TARGET`, which defaults to `http://127.0.0.1:9000` in `.env.template`.

## Common Workflows

### Run Backend Tests

```bash
python3 -m pytest -q
```

### Run UI Checks

```bash
cd ui
pnpm lint
pnpm exec vitest run --coverage
pnpm build
```

### Refresh Runtime Dependency Manifests

```bash
python3 scripts/dependency_governance.py sync
python3 scripts/dependency_governance.py check --report artifacts/dependency_governance_report.json
```

## Runtime and Data Model

- `core.runtime_config` Postgres rows let operators change allowlisted runtime overrides, including debug-symbol filters, without rebuilding the containers.
- The API applies runtime config at startup; ETL jobs apply runtime config and debug symbols during job startup.
- System health surfaces live under `/api/system/health`, `/healthz`, `/readyz`, and `/api/ws/updates`.
- `/config.js` publishes concrete auth capabilities and API base URLs that the frontend reads at runtime.

## Current API Scope

The mounted FastAPI routers are `data`, `system`, `system/postgres`, `strategies`, `providers/alpha-vantage`, and `providers/massive`. Historical backtest naming still appears in compatibility surfaces such as `backtestApiBaseUrl` in `/config.js`, so treat `/api/docs` and `/api/openapi.json` as the authoritative route map.

## Deployment

- `scripts/provision_azure_interactive.ps1` is the recommended interactive entrypoint for Azure setup. It now walks preflight validation, shared resource provisioning, Entra OIDC application provisioning, optional Postgres provisioning, optional GitHub env sync, optional cost guardrails, and post-provision validation in one session.
- `.github/workflows/deploy.yml` builds and deploys the repo to Azure.
- `scripts/provision_azure.ps1` and `scripts/provision_azure_postgres.ps1` remain the underlying targeted provisioners used by the interactive wrapper.
- `scripts/provision_entra_oidc.ps1` is the focused Entra/Microsoft Graph provisioner that creates or reconciles the API and SPA app registrations, service principals, delegated permissions, and app-role assignments, then writes the resulting OIDC values back into `.env.web` or `.env`.
- `deploy/app_api_public.yaml` is the public-ingress unified API and UI Container App manifest used by the default deploy workflow.
- `deploy/app_api.yaml` is the internal-ingress variant for private-only deployments.
- Scheduled Azure Container App Jobs under `deploy/job_*.yaml` run Bronze, Silver, and Gold workloads for the supported data domains.

### Authentication

- Public browser access should use Microsoft Entra OIDC, not a shared browser API key.
- Production deploys are OIDC-only: configure `API_OIDC_*`, `UI_OIDC_*`, and `ASSET_ALLOCATION_API_SCOPE`.
- Keep `API_KEY` only as a local/private compatibility fallback for non-browser callers while any internal callers are still being migrated.
- UI-managed OIDC requires `UI_OIDC_AUTHORITY`, `UI_OIDC_CLIENT_ID`, and an absolute `UI_OIDC_REDIRECT_URI` alongside API OIDC.
- Production auth should prefer role-based enforcement via `API_OIDC_REQUIRED_ROLES`; leave `API_OIDC_REQUIRED_SCOPES` empty unless every caller is delegated-user only.
- Bronze jobs should authenticate to the API with the shared user-assigned managed identity via `ASSET_ALLOCATION_API_SCOPE`, not `X-API-Key`.

## Evidence

- `pyproject.toml`
- `.env.template`
- `api/service/app.py`
- `api/service/settings.py`
- `core/runtime_config.py`
- `core/debug_symbols.py`
- `core/strategy_repository.py`
- `tasks/market_data/gold_market_data.py`
- `ui/src/app/App.tsx`
- `.github/workflows/deploy.yml`
- `deploy/app_api.yaml`
- `deploy/app_api_public.yaml`
- `tests/api/test_swagger_docs.py`
- `tests/api/test_config_js_contract.py`
