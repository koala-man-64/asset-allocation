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

**API service** applies runtime config on startup and periodically refreshes it when `RUNTIME_CONFIG_REFRESH_SECONDS` is set (default 60s).

**UI pages** (served by the UI app) for updates:
- `/debug-symbols` for the `core.debug_symbols` allowlist and enable/disable flag.
- `/runtime-config` for allowlisted runtime config keys (global scope today; job scopes supported in schema).

The runtime-config allowlist includes common pipeline knobs (backfills/materialization), system-health probes, and selected non-secret ingestion tunables (e.g., Alpha Vantage rate limits/timeouts).

Alpha Vantage Bronze jobs use per-job locks only; provider-level contention is handled by API-side fair-share rate limiting keyed by caller job headers.

## Deployment

Azure deployment is driven by `.github/workflows/deploy.yml` and manifests under `deploy/`.

## Dependency lockfiles
- `requirements.lock.txt` is used by Docker builds for reproducible images.
- `requirements-dev.lock.txt` is used by CI for reproducible test installs.

## Docs
- `api/API_ENDPOINTS.md`
- `docs/alpha_vantage_fair_rate_limiting.md`
- `docs/config_js_contract.md`
- `docs/strategy_pipeline_layer_domain_bindings.md`

