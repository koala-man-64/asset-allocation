# Postgres Serving Split — Phase 4 (Optional Postgres Signal Reads)

Phase 4 adds **optional** support for reading **platinum composite signals** from **Postgres** during backtests to improve interactive latency, while keeping **ADLS/Delta canonical**.

## Scope (explicit)

- **In scope**
  - Read `ranking.composite_signal_daily` from Postgres for the configured date range + universe.
  - Keep existing ADLS/Delta signal reads as default and canonical.
  - Minimal config surface area for choosing the signal source.
- **Out of scope**
  - Backfill/migration of historical partitions into Postgres.
  - Drift/health monitoring for signal freshness (assumed handled elsewhere).
  - Any change to the ranking writer/dual-write flow (Phase 2 remains authoritative for writes).

## Preconditions

- Phase 2 has been deployed and is writing `ranking.composite_signal_daily` to Postgres.
- The backtest environment has a DSN available (suggested: reuse `BACKTEST_POSTGRES_DSN`).

## Design decisions (Phase 4)

- **DEC-401 Canonical truth remains Delta:** Postgres is a **serving cache** and is always optional.
- **DEC-402 Reader is opt-in:** Backtests default to current behavior unless explicitly configured.
- **DEC-403 Keep merge-conflict surface low:** Avoid changes to `services/backtest_api/app.py` for Phase 4.

## Implementation steps

### 1) Extend backtest config to support a signal source selector

Update `backtest/config.py`:

- Add `signal_source` to `DataConfig`, e.g.:
  - `signal_source: Literal["auto", "local", "ADLS", "postgres"] = "auto"`
  - `auto` keeps current behavior (signals follow `price_source`).
- Update `validate_config_dict_strict()` allowlist to include `signal_source` under `data`.

Acceptance criteria:

- Existing configs continue to parse and run without changes.
- Strict validation accepts the new field and rejects unknown values.

### 2) Implement a Postgres signals loader (composite daily)

Update `backtest/data_access/loader.py`:

- Add `_load_signals_postgres(config: BacktestConfig, data: DataConfig) -> Optional[pd.DataFrame]`
  - DSN source: `BACKTEST_POSTGRES_DSN` (default) with an optional override env var if needed later.
  - Query `ranking.composite_signal_daily` filtered by:
    - `date BETWEEN :start AND :end`
    - `symbol = ANY(:symbols)` (or `IN (...)` batched if needed)
  - Return a DataFrame including at least:
    - `date`, `symbol`, `composite_percentile`
    - plus the remaining columns as available (safe to keep full row).
- Update `load_backtest_inputs()` to choose the signals loader based on:
  - `signal_source` when set
  - otherwise `price_source` (current behavior)

Acceptance criteria:

- When configured for Postgres, signals load succeeds without Delta access.
- When Postgres returns no rows, behavior is deterministic (either `None` or explicit error—choose and document).

### 3) Add tests (selection + basic shape)

Add unit coverage around the **routing logic** (not full Postgres integration):

- Ensure the loader chooses:
  - local signals when `signal_source=local`
  - delta signals when `signal_source=ADLS`
  - postgres signals when `signal_source=postgres`
  - “follow price_source” when `signal_source=auto` / unset
- For Postgres path, mock the query layer and validate DataFrame columns include `date` and `symbol`.

Acceptance criteria:

- Tests validate routing decisions and prevent regressions when later merging with UI/backtest API changes.

### 4) Documentation + examples

- Update `docs/backtest_service.md` to document:
  - `data.signal_source`
  - required DSN env vars
  - canonical source statement (Delta remains truth)

## Merge-prep notes for `ag-ui-wiring`

The local `ag-ui-wiring` copy introduces API and UI changes that are likely to conflict if Phase 4 touches shared files. To minimize future merge conflicts:

- Keep Phase 4 changes isolated to:
  - `backtest/config.py`
  - `backtest/data_access/loader.py`
- Account for UI contract drift:
  - `ag-ui-wiring` UI calls `/api/system/health` via `ui/src/services/backtestApi.ts` (when `BACKTEST_UI_API_BASE_URL=/api`).
  - `ag-ui-wiring` UI also uses different “live” market/finance endpoints (`/market/...`, `/finance/...`) than the current backend API (`/data/...`).
- When merging later, reconcile `services/backtest_api/app.py` carefully:
  - `ag-ui-wiring` adds `/api/system/health` and a monitoring package.
  - Postgres Phase 3 adds Postgres run-store readiness (`/readyz` pings the configured store).

