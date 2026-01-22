### 1. Executive Summary
- The `ag-ui-wiring` branch introduces UI and backtest-service changes that intersect with the Postgres serving-split work, and the merge risk is concentrated in **API contract drift** and **shared-file overlap**.
- The highest-risk issue is an apparent mismatch between `ag-ui-wiring` UI “live” endpoints (`/market/...`, `/finance/...`, `/strategies`) and the current backend API routing (`/data/...`), which can produce immediate UI 404s after merge.
- `ag-ui-wiring` also enables **TypeScript strict mode**, which can turn previously-warning-only type issues into build failures and should be treated as a gated migration.
- The backtest service will require a careful conflict resolution between `ag-ui-wiring`’s `/system/health` + monitoring wiring and the Postgres run-store readiness logic (Phase 3).
- Recommended near-term priority: decide the canonical API contract for UI market/finance reads and implement a compatibility layer (or revert UI endpoints) before merging branches.

### 2. System Map (High-Level)
- **Backend API (`backend/api`)**
  - Exposes data reads under `/data` (market/earnings/price-target + finance subdomains) and ranking under `/ranking`.
  - Includes `/health` for basic liveness.
- **Backtest Service (`asset_allocation/backtest/service`)**
  - Exposes run lifecycle and artifact endpoints under `/backtests/*`.
  - Serves UI runtime config via `/config.js` (writes `window.__BACKTEST_UI_CONFIG__`).
  - In `ag-ui-wiring`, also exposes `/system/health` and depends on a monitoring package.
- **UI (`asset_allocation/ui2.0`)**
  - Uses `VITE_API_BASE_URL` for backend API calls (market/finance/strategies).
  - Uses a runtime-injected `window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl` (or `VITE_BACKTEST_API_BASE_URL`) for backtest API calls.
- **Data Stores**
  - ADLS/Delta remains canonical for bronze/silver/gold/platinum datasets.
  - Postgres is a rebuildable serving/state layer (signals + backtest runs).

### 3. Findings (Triaged)

#### 3.1 Critical (Must Fix)
- **[UI ↔ Backend API contract mismatch for market/finance reads]**
  - **Evidence:**
    - `ag-ui-wiring` UI calls `GET {VITE_API_BASE_URL}/market/{layer}/{ticker}` and `GET {VITE_API_BASE_URL}/finance/{layer}/{subDomain}/{ticker}` in `asset_allocation/ui2.0/src/services/DataService.ts:124` and `asset_allocation/ui2.0/src/services/DataService.ts:130`.
    - Current backend mounts data router at `/data` (`backend/api/main.py:30`) with handlers expecting `/data/{layer}/{domain}?ticker=...` (`backend/api/endpoints/data.py:9`) and `/data/{layer}/finance/{sub_domain}?ticker=...` (`backend/api/endpoints/data.py:62`).
  - **Why it matters:**
    - After merging `ag-ui-wiring`, UI “live” mode can hard-fail with 404s for core market/finance charts, creating an immediate perceived outage even if the underlying data lake is healthy.
  - **Recommendation:**
    - Choose one of:
      - Add **compatibility routes** in `backend/api` to support `/market/{layer}/{ticker}` and `/finance/{layer}/{subDomain}/{ticker}` (thin wrappers calling the existing `/data/...` logic), while keeping `/data/...` stable.
      - Or revert/adjust UI `DataService` endpoints to `/data/...` so the UI matches the existing API.
    - Document the canonical contract in one place (UI README or backend OpenAPI docs) and lock it with a small integration test.
  - **Acceptance Criteria:**
    - UI can fetch market + finance data successfully using the chosen contract in a local run.
    - No 404s for market/finance endpoints in “live” mode.
    - Existing `/data/...` endpoints remain functional (backward compatibility) unless explicitly removed with a migration plan.
  - **Owner Suggestion:** Delivery Engineer Agent (+ QA Release Gate Agent for verification)

- **[TypeScript strict-mode merge risk]**
  - **Evidence:**
    - `ag-ui-wiring` enables strict TS compilation in `asset_allocation/ui2.0/tsconfig.json:16` and `asset_allocation/ui2.0/tsconfig.json:17`.
  - **Why it matters:**
    - A strict-mode flip can block CI/CD if any implicit `any`/unsafe typing exists in UI code (even if runtime is fine).
  - **Recommendation:**
    - Treat strict-mode adoption as a gated rollout:
      - run `pnpm exec tsc --noEmit` and fix surfaced issues,
      - keep any required module/window declarations co-located in `vite-env.d.ts`.
  - **Acceptance Criteria:**
    - `pnpm exec tsc --noEmit` succeeds with strict enabled.
    - `pnpm build` succeeds in CI.
  - **Owner Suggestion:** Delivery Engineer Agent + QA Release Gate Agent

#### 3.2 Major
- **[Backtest API merge conflict surface: `/system/health` vs Postgres run-store readiness]**
  - **Evidence:**
    - `ag-ui-wiring` adds `/system/health` at `asset_allocation/backtest/service/app.py:232` and conditions `/readyz` DB pinging on ADLS mode at `asset_allocation/backtest/service/app.py:224`.
    - Postgres Phase 3 expects `/readyz` to validate the configured store (including Postgres) (current repo: `asset_allocation/backtest/service/app.py:207` calls `store.ping()` when present).
  - **Why it matters:**
    - Incorrect readiness behavior can cause Container Apps to restart/route traffic while Postgres is unavailable, or report Ready when DB connectivity is broken.
    - UI in `ag-ui-wiring` adds a client method for `GET /system/health` (`asset_allocation/ui2.0/src/services/backtestApi.ts:269`), so missing or broken endpoint becomes user-visible.
  - **Recommendation:**
    - During merge, take `ag-ui-wiring`’s `app.py` as the base (to preserve `/system/health`) and re-apply the Postgres run-store wiring:
      - update `/readyz` to ping for `run_store_mode == "postgres"` as well (or ping whenever the active store implements `ping()`).
      - ensure auth requirements for `/system/health` remain consistent with your chosen auth mode.
    - Keep Phase 4 (Postgres signal reads) isolated from this file to avoid churn.
  - **Acceptance Criteria:**
    - `/readyz` returns 200 only when the configured run store (Postgres) is reachable and initialized.
    - `/system/health` returns 200 for authorized callers (and does not leak secrets in the response).
  - **Owner Suggestion:** Delivery Engineer Agent (+ QA Release Gate Agent)

- **[Signals loader is currently tied to `price_source`, limiting Phase 4]**
  - **Evidence:**
    - Signals routing follows `data.price_source` in `asset_allocation/backtest/data_access/loader.py:213`–`asset_allocation/backtest/data_access/loader.py:219`.
    - `DataConfig` lacks a `signal_source` selector (`asset_allocation/backtest/config.py:162`–`asset_allocation/backtest/config.py:167`).
  - **Why it matters:**
    - Phase 4 (Postgres signal reads) needs a clean, low-conflict way to switch signal reads without also changing how prices are loaded.
  - **Recommendation:**
    - Add `data.signal_source` with default `auto` (signals follow price_source) and add a `postgres` option for Phase 4.
  - **Acceptance Criteria:**
    - Backtests can read prices from ADLS while reading signals from Postgres (or vice versa) when configured.
  - **Owner Suggestion:** Delivery Engineer Agent

#### 3.3 Minor
- **[UI runtime config typing should match actual `/config.js` payload]**
  - **Evidence:**
    - Backtest API writes `window.__BACKTEST_UI_CONFIG__ = {...}` in `asset_allocation/backtest/service/app.py:747` (current repo).
    - UI reads from `window.__BACKTEST_UI_CONFIG__` in `asset_allocation/ui2.0/src/services/backtestApi.ts:13` and `asset_allocation/ui2.0/src/contexts/AuthContext.tsx:17` (current repo).
  - **Why it matters:**
    - Type mismatches become CI failures under strict TS; runtime config is a cross-cutting integration point.
  - **Recommendation:**
    - Ensure `asset_allocation/ui2.0/src/vite-env.d.ts` declares `window.__BACKTEST_UI_CONFIG__` and includes optional auth fields used by `AuthContext`.
  - **Acceptance Criteria:**
    - UI builds without TS errors with strict mode enabled.
  - **Owner Suggestion:** Delivery Engineer Agent / Code Hygiene Agent

### 4. Architectural Recommendations
- **Stabilize API contracts before merging:** decide whether UI “live” endpoints are `/data/...` or `/market/...` + `/finance/...`, and implement compatibility wrappers if you need both.
- **Minimize cross-branch churn:** keep Phase 4 (Postgres signal reads) changes in `asset_allocation/backtest/config.py` + `asset_allocation/backtest/data_access/loader.py` only, avoiding `asset_allocation/backtest/service/app.py` (high overlap with `ag-ui-wiring`).
- **Make merge intent explicit:** capture a short “merge checklist” that lists the 3 high-conflict files (`asset_allocation/backtest/service/app.py`, `.github/workflows/run_tests.yml`, `asset_allocation/ui2.0` runtime-config files) and how to resolve each.
- **Prefer additive compatibility over flag days:** add new endpoints/config options without breaking existing consumers, then deprecate deliberately once the merged branch is stable.

### 5. Operational Readiness & Observability
- The user explicitly deferred “signal freshness drift monitoring”, so no additional drift probes are required for Phase 4.
- Ensure readiness semantics remain correct after the merge:
  - `/readyz` must validate the active run store (Postgres in Phase 3).
  - If `/system/health` remains, treat it as a privileged endpoint (auth + no-store caching, and avoid including secrets/resource IDs by default).
- If the merged UI depends on `/system/health`, include a simple contract test to avoid accidental breaking changes.

### 6. Refactoring Examples (Targeted)
- **Before:**
  ```ts
  // ag-ui-wiring UI (live market endpoint)
  await fetch(`${apiBaseUrl}/market/${layer}/${ticker}`)
  ```
  ```py
  # current backend API mount point
  app.include_router(data.router, prefix="/data", tags=["Data"])
  ```

### 7. Evidence & Telemetry
- Files reviewed:
  - `asset_allocation/ui2.0/src/services/DataService.ts` (ag-ui-wiring)
  - `asset_allocation/ui2.0/tsconfig.json` (ag-ui-wiring)
  - `backend/api/main.py`
  - `backend/api/endpoints/data.py`
  - `asset_allocation/backtest/service/app.py` (current repo + ag-ui-wiring)
  - `asset_allocation/backtest/data_access/loader.py`
  - `asset_allocation/backtest/config.py`
- Commands run (local-only evidence):
  - `git log --oneline -n 5` (ag-ui-wiring)
  - `git diff --name-only 19202fe..HEAD` (ag-ui-wiring)
  - `sed` / `nl -ba` reads for line-anchored evidence
  - `rg` searches for route and config usage (`/system/health`, `__BACKTEST_UI_CONFIG__`, `/market/`, `/data/`)

