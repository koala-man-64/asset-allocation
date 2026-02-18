# API Background Workers Runbook

## Scope

This runbook covers API lifespan background workers started from `api/service/app.py`:

- `runtime_config_refresh`
- `periodic_purge_rules`

## Environment Contract

- `BACKGROUND_WORKERS_ENABLED`
  - Global gate for non-essential background workers.
  - Default behavior:
    - Test mode (`TEST_MODE=true` or pytest context): disabled
    - Non-test mode: enabled
  - Explicit values:
    - truthy (`1,true,yes,on`): enabled
    - falsey (`0,false,no,off`): disabled
- `PURGE_RULES_ENABLED`
  - Per-worker gate for `periodic_purge_rules`.
  - Default: enabled.
- `POSTGRES_DSN`
  - Must be set for purge/runtime config workers to have a data source.

## Lifecycle Behavior

Shutdown uses a cancellation-safe helper:

1. Set worker stop event.
2. Wait for graceful exit (bounded timeout).
3. Cancel task only if timeout is exceeded.
4. Suppress `asyncio.CancelledError` so app teardown does not fail.

## Operational Signals

The API emits worker lifecycle logs:

- `Background worker gate resolved: enabled=... test_env=...`
- `Background task started: ...`
- `Background task '...' stopped gracefully.`
- `Background task '...' cancelled after timeout.`

## Test Guidance

- Test harness defaults:
  - `BACKGROUND_WORKERS_ENABLED=false`
  - `PURGE_RULES_ENABLED=false`
  - inherited `POSTGRES_DSN` removed
- For explicit worker tests, override env vars inside the test via `monkeypatch`.

