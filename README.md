# AssetAllocation

Python project for market/finance data pipelines, system monitoring, and a backtest framework.

## Quickstart

### Prerequisites
- Python 3.10 (matches Docker/CI) and `pip`

### Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements-dev.txt
python3 -m pip install -e .
```

### Configure environment
- Copy `.env.template` to `.env` and fill required values, or run the interactive setup:
  - `pwsh scripts/setup-env.ps1`

### Run tests
```bash
python3 -m pytest -q
```



## UI (Vite) local dev

### Prerequisites
- Node.js + `pnpm` (see `ui/package.json` `packageManager`)

### Run UI only
```bash
cd ui
pnpm install
pnpm dev
```

### Run UI + API (VS Code)
- Run the task `dev: ui+api` (it runs `pnpm install` for the UI first).

### Troubleshooting
- If you see `'vite' is not recognized...`, run `pnpm install` in `ui/` (or delete `ui/node_modules` and reinstall).
- If UI loads but API calls do not respond, check for API port conflicts:
  - `python3 scripts/run_api_dev.py` (or use VS Code task `api: dev`).
  - If it reports `Port ... is already in use`, free that port or set matching values for `API_PORT` and `VITE_API_PROXY_TARGET` in `.env` / `.env.local`.

## Backend API (UI)

The UI calls the FastAPI service under `/api/*` (see `api/API_ENDPOINTS.md`). Common endpoints:

- `GET /api/data/{layer}/market?ticker={ticker}` (layer: `silver|gold`)
- `GET /api/data/{layer}/finance/{sub_domain}?ticker={ticker}` (layer: `silver|gold`)
- `GET /api/system/health`
- `WS /api/ws/updates`

Swagger/OpenAPI docs in browser:
- `GET /api/docs` (Swagger UI)
- `GET /api/openapi.json` (OpenAPI spec)
- `GET /docs` (redirects to the active API docs path)

## Runtime Config & Debug Symbols (DB-Backed)

This repo supports DB-backed runtime configuration so operational knobs can be changed without redeploying.

- **Debug symbols** live in Postgres table `core.debug_symbols` (migration `deploy/sql/postgres/migrations/0009_debug_symbols_config.sql`).
- **Runtime config overrides** live in Postgres table `core.runtime_config` (migration `deploy/sql/postgres/migrations/0010_runtime_config.sql`).

**ETL jobs** apply both at startup via `core/core.py` → `log_environment_diagnostics()`:
- `core/runtime_config.py` loads allowlisted overrides (scope precedence: `job:<CONTAINER_APP_JOB_NAME>` then `global`) and applies them to `os.environ`.
- `core/debug_symbols.py` refreshes debug symbols from Postgres and updates `core.config.DEBUG_SYMBOLS` for debug filtering.
 - `core/config.py` `reload_settings()` re-reads `AppSettings` after overrides so downstream code sees updated `core.config.*` values.

**API service** applies runtime config once on startup. Ongoing refreshes are manual-only.

**UI pages** (served by the UI app) for updates:
- `/debug-symbols` for the `core.debug_symbols` allowlist and enable/disable flag.
- `/runtime-config` for allowlisted runtime config keys (global scope today; job scopes supported in schema).

The runtime-config allowlist includes common pipeline knobs (backfills/materialization), system-health probes, and selected non-secret ingestion tunables (e.g., Alpha Vantage rate limits/timeouts).

Alpha Vantage Bronze jobs use per-job locks. Finance Bronze/Silver additionally support a shared cross-layer lock (`FINANCE_PIPELINE_SHARED_LOCK_NAME`) to prevent overlap during handoff windows.

## Finance Bronze -> Silver Handoff

Finance ingestion includes convergence and handoff controls to reduce Bronze/Silver symbol drift:

- `SILVER_FINANCE_CATCHUP_MAX_PASSES` controls bounded relist/catch-up passes per Silver run.
- `FINANCE_RUN_MANIFESTS_ENABLED=true` enables Bronze finance run manifests in `AZURE_CONTAINER_COMMON` under `system/run-manifests/`.
- `SILVER_FINANCE_USE_BRONZE_MANIFEST=true` allows Silver finance to prefer the latest unacknowledged Bronze manifest and write per-run acknowledgements.
- `BRONZE_FINANCE_SHARED_LOCK_WAIT_SECONDS` / `SILVER_FINANCE_SHARED_LOCK_WAIT_SECONDS` tune shared-lock wait behavior.

You can run an explicit reconciliation report to audit current Bronze->Silver finance coverage:

```bash
python3 -m tasks.finance_data.reconcile_finance_coverage
```

The report is written to `system/reconciliation/finance_coverage/latest.json` in the common container and includes:
- `totalBronzeOnlySymbolCount` / `bronzeOnlySymbolCount`: Bronze symbols missing in Silver (lag).
- `totalSilverOnlySymbolCount` / `silverOnlySymbolCount`: Silver symbols not present in Bronze (orphans).

## Gold Market By-Date View

You can materialize a single by-date Gold market table (`market_by_date`) from per-symbol Gold tables (`market/<symbol>`):

```bash
python3 -m tasks.market_data.materialize_gold_market_by_date
```

Column projection is configurable:
- `GOLD_BY_DATE_DOMAIN=market|finance|earnings|price-target`
- `GOLD_MARKET_BY_DATE_COLUMNS=close,volume,return_1d,vol_20d` (always includes `date` and `symbol`)
- `MATERIALIZE_YEAR_MONTH=YYYY-MM` for a single-month partial rebuild
- `MATERIALIZE_YEAR_MONTH=YYYY-MM..YYYY-MM` for a month-range partial rebuild

To run this as part of the regular Gold market job, set:
- `GOLD_MARKET_BY_DATE_ENABLED=true`

## Deployment

Azure deployment is driven by `.github/workflows/deploy.yml` and manifests under `deploy/`.

## Dependency lockfiles
- `requirements.lock.txt` is used by Docker builds for reproducible images.
- `requirements-dev.lock.txt` is used by CI for reproducible test installs.

## Docs
- `api/API_ENDPOINTS.md`
- `docs/alpha_vantage_fair_rate_limiting.md`
- `docs/api_background_workers_runbook.md`
- `docs/config_js_contract.md`
- `docs/strategy_pipeline_layer_domain_bindings.md`

