# Backtest Service (Phases 3–5)

FastAPI service for config-driven backtest execution with persistent run state and artifact access.

## UI Hosting (Phase 4, Option A)

When the service is built with `Dockerfile.backtest_api`, the React SPA is built from `ui/`
and served from the same FastAPI process:

- UI: `GET /`
- Static assets: `GET /assets/*`
- API: `GET /api/backtests/*`

You can override the UI dist directory with `BACKTEST_UI_DIST_DIR` (must contain `index.html` and `assets/`).

## Run Locally

```bash
export BACKTEST_OUTPUT_DIR=./backtest_results
export BACKTEST_DB_PATH=./backtest_results/runs.sqlite3
export BACKTEST_MAX_CONCURRENT=1

# Run store mode:
# - sqlite (default): persists run state to a local sqlite file
# - adls: persists run state to ADLS/Blob (cheap but slower for listing/querying)
# - postgres: persists run state to Postgres (recommended for cloud deployments)
export BACKTEST_RUN_STORE_MODE=sqlite

# When BACKTEST_RUN_STORE_MODE=postgres, this is required:
export BACKTEST_POSTGRES_DSN="postgresql://backtest_service:<password>@<server>.postgres.database.azure.com:5432/asset_allocation?sslmode=require"

# When BACKTEST_RUN_STORE_MODE=adls, this is required and is used as the run-store location.
# When BACKTEST_RUN_STORE_MODE=postgres, this is optional and is used as the default upload
# location when a submitted config does not set output.adls_dir.
# Format: <container>/<path-prefix> or abfss://<container>@<account>.dfs.core.windows.net/<path-prefix>
export BACKTEST_ADLS_RUNS_DIR="platinum/backtest-api-results"

# Local data reads are disabled by default. Enable + restrict to allowlisted dirs:
export BACKTEST_ALLOW_LOCAL_DATA=true
export BACKTEST_ALLOWED_DATA_DIRS="$(pwd)/data"

# Optional auth (recommended for any shared environment):
export BACKTEST_API_KEY="change-me"
# Optional: customize API key header name (default: X-API-Key)
export BACKTEST_API_KEY_HEADER="X-API-Key"

# Optional: browser-safe auth via OIDC/JWT
# If BACKTEST_OIDC_ISSUER + BACKTEST_OIDC_AUDIENCE are set, the service will accept bearer tokens.
# When both API key + OIDC are configured, the default mode becomes api_key_or_oidc.
export BACKTEST_AUTH_MODE="api_key_or_oidc"   # none|api_key|oidc|api_key_or_oidc
export BACKTEST_OIDC_ISSUER="https://login.microsoftonline.com/<tenant-id>/v2.0"
export BACKTEST_OIDC_AUDIENCE="api://<api-client-id>"
# Optional override; otherwise discovered via the issuer's OIDC discovery document:
export BACKTEST_OIDC_JWKS_URL="https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys"
# Optional authorization:
export BACKTEST_OIDC_REQUIRED_SCOPES="backtests.read,backtests.write"
export BACKTEST_OIDC_REQUIRED_ROLES=""

# Required: Content-Security-Policy header (override as needed)
export BACKTEST_CSP="default-src 'self'; base-uri 'none'; frame-ancestors 'none'"

# Required: system-health cache TTL (used by /api/system/* endpoints)
export SYSTEM_HEALTH_TTL_SECONDS=30

uvicorn api.service.app:app --reload --port 8000
```

## API

- `POST /api/backtests` - submit a run (YAML or JSON config)
- `GET /api/backtests` - list runs (filters: `status`, `q`, `limit`, `offset`)
- `GET /api/backtests/{run_id}/status`
- `GET /api/backtests/{run_id}/summary` (query: `source=auto|local|adls`)
- `GET /api/backtests/{run_id}/artifacts` (query: `remote=true`)
- `GET /api/backtests/{run_id}/artifacts/{name}` (query: `source=auto|local|adls`)
- `GET /api/backtests/{run_id}/metrics/timeseries` (query: `source=auto|local|adls`, `max_points=...`)
- `GET /api/backtests/{run_id}/metrics/rolling` (query: `window_days=...`, `source=auto|local|adls`, `max_points=...`)
- `GET /api/backtests/{run_id}/trades` (query: `source=auto|local|adls`, `limit=...`, `offset=...`)

### Example: Submit (JSON)

```bash
curl -X POST "http://localhost:8000/api/backtests" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d @backtest.json
```

### Example: Submit (YAML)

```bash
curl -X POST "http://localhost:8000/api/backtests" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d '{"config_yaml": "'"$(cat backtest.yaml | sed 's/"/\\"/g')"'"}'
```

## Artifact Uploads (ADLS/Blob)

Set `output.adls_dir` in the backtest config to enable upload after a run completes.

If `BACKTEST_ADLS_RUNS_DIR` is set, uploads will default to that
location even when `output.adls_dir` is not provided.

Supported formats:
- `container/path-prefix`
- `abfss://<container>@<account>.dfs.core.windows.net/<path-prefix>`

The service uploads to:
`<container>/<path-prefix>/<run_id>/<artifact files>`

## Security Notes

- Local filesystem reads via `data.price_source=local` are blocked unless explicitly enabled + allowlisted.
- Auth modes:
  - `BACKTEST_AUTH_MODE=api_key` requires `BACKTEST_API_KEY`.
  - `BACKTEST_AUTH_MODE=oidc` requires bearer tokens (`Authorization: Bearer ...`).
  - `BACKTEST_AUTH_MODE=api_key_or_oidc` allows either (recommended for “UI + automation”).
  - `BACKTEST_AUTH_MODE=none` disables auth (recommended only for internal/dev).

## UI Auth (Phase 5)

The UI supports OIDC in the browser (preferred) and **dev-only** API keys.

For Option A hosting, the FastAPI service serves a runtime config file at `GET /config.js`.
The SPA loads it from `ui/index.html` and reads values from `window.__BACKTEST_UI_CONFIG__`.

### Runtime UI config (served by API)

Backtest API env vars (recommended for Option A):
- `BACKTEST_UI_AUTH_MODE=oidc|api_key|none` (defaults to `oidc` when the API supports OIDC; otherwise `none`)
- `BACKTEST_UI_OIDC_CLIENT_ID=...` (SPA app registration client id)
- `BACKTEST_UI_OIDC_AUTHORITY=https://login.microsoftonline.com/<tenant-id>` (optional; defaults from `BACKTEST_OIDC_ISSUER`)
- `BACKTEST_UI_OIDC_SCOPES=api://<api-client-id>/backtests.read api://<api-client-id>/backtests.write`
- `BACKTEST_UI_API_BASE_URL=` (optional; default empty means “same origin”)

### UI env vars (Vite, local dev)

These are used when running the UI via Vite (`pnpm dev`) and as a fallback if `/config.js` is not present:
- `VITE_AUTH_MODE=oidc|api_key|none`
- `VITE_OIDC_CLIENT_ID=...`
- `VITE_OIDC_AUTHORITY=https://login.microsoftonline.com/<tenant-id>`
- `VITE_OIDC_SCOPES=api://<api-client-id>/backtests.read api://<api-client-id>/backtests.write`

Dev-only API key support:
- `VITE_BACKTEST_API_KEY=...` (only sent automatically in dev unless `VITE_ALLOW_BROWSER_API_KEY=true`)
