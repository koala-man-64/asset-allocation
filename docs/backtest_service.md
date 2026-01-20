# Backtest Service (Phase 3)

FastAPI service for config-driven backtest execution with persistent run state and artifact access.

## Run Locally

```bash
export BACKTEST_OUTPUT_DIR=./backtest_results
export BACKTEST_DB_PATH=./backtest_results/runs.sqlite3
export BACKTEST_MAX_CONCURRENT=1

# Run store mode:
# - sqlite (default): persists run state to a local sqlite file
# - adls: persists run state to ADLS/Blob (recommended for cloud deployments)
export BACKTEST_RUN_STORE_MODE=sqlite

# When BACKTEST_RUN_STORE_MODE=adls, this is required and is used as the default
# upload location when a submitted config does not set output.adls_dir.
# Format: <container>/<path-prefix> or abfss://<container>@<account>.dfs.core.windows.net/<path-prefix>
export BACKTEST_ADLS_RUNS_DIR="platinum/backtest-api-results"

# Local data reads are disabled by default. Enable + restrict to allowlisted dirs:
export BACKTEST_ALLOW_LOCAL_DATA=true
export BACKTEST_ALLOWED_DATA_DIRS="$(pwd)/data"

# Optional auth (recommended for any shared environment):
export BACKTEST_API_KEY="change-me"

uvicorn asset_allocation.backtest.service.app:app --reload --port 8000
```

## API

- `POST /backtests` - submit a run (YAML or JSON config)
- `GET /backtests` - list runs (filters: `status`, `q`, `limit`, `offset`)
- `GET /backtests/{run_id}/status`
- `GET /backtests/{run_id}/summary` (query: `source=auto|local|adls`)
- `GET /backtests/{run_id}/artifacts` (query: `remote=true`)
- `GET /backtests/{run_id}/artifacts/{name}` (query: `source=auto|local|adls`)
- `GET /backtests/{run_id}/metrics/timeseries` (query: `source=auto|local|adls`, `max_points=...`)
- `GET /backtests/{run_id}/metrics/rolling` (query: `window_days=...`, `source=auto|local|adls`, `max_points=...`)
- `GET /backtests/{run_id}/trades` (query: `source=auto|local|adls`, `limit=...`, `offset=...`)

### Example: Submit (JSON)

```bash
curl -X POST "http://localhost:8000/backtests" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d @backtest.json
```

### Example: Submit (YAML)

```bash
curl -X POST "http://localhost:8000/backtests" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d '{"config_yaml": "'"$(cat backtest.yaml | sed 's/"/\\"/g')"'"}'
```

## Artifact Uploads (ADLS/Blob)

Set `output.adls_dir` in the backtest config to enable upload after a run completes.

If `BACKTEST_RUN_STORE_MODE=adls` and `BACKTEST_ADLS_RUNS_DIR` is set, uploads will default to that
location even when `output.adls_dir` is not provided.

Supported formats:
- `container/path-prefix`
- `abfss://<container>@<account>.dfs.core.windows.net/<path-prefix>`

The service uploads to:
`<container>/<path-prefix>/<run_id>/<artifact files>`

## Security Notes

- Local filesystem reads via `data.price_source=local` are blocked unless explicitly enabled + allowlisted.
- If `BACKTEST_API_KEY` is set, all backtest endpoints require `X-API-Key`.
