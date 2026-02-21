# API Background Workers Runbook

## Scope

The API does not run periodic background refresh workers.

## Current Behavior

- Runtime config and debug symbols are applied once at API startup.
- Purge rules run only when explicitly triggered via API endpoints (manual operations).
- UI-driven data updates are manual refresh actions; no automatic polling is required.

## Operational Signals

- You should no longer see repeating lifecycle logs for:
  - `runtime_config_refresh`
  - `periodic_purge_rules`
- If Postgres is unavailable at startup, a one-time startup warning may still appear.

## Test Guidance

- Existing lifespan tests should focus on startup/shutdown stability and manual endpoints.
- Keep `POSTGRES_DSN` unset in tests unless the scenario explicitly requires DB integration.
