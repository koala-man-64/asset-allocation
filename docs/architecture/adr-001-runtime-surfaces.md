# ADR 001: Runtime Surface Boundaries for Modular Monolith Extraction

## Status
- Accepted

## Context
- The repository currently mixes transport, ETL, monitoring, provider, and shared-contract concerns across `api/`, `core/`, `monitoring/`, and `tasks/`.
- Several high-churn modules are oversized and act as cross-surface coordination points, including:
  - `tasks/finance_data/silver_finance_data.py`
  - `api/endpoints/system.py`
  - `monitoring/system_health.py`
- The target architecture remains a single deployable repository for now, but the internal module boundaries must support later extraction into multiple repositories with minimal rewrite.

## Decision
- Organize the codebase around runtime surfaces:
  - `core/`: shared foundation and shared internal contracts
  - `tasks/`: ETL jobs and job orchestration
  - `api/`: FastAPI transport, auth, and read orchestration
  - `monitoring/`: health/status collection and Azure monitoring logic
  - `ui/`: feature-driven frontend and typed API clients
  - provider adapters remain exposed through stable modules consumed by `api/` and `tasks/`
- Preserve current external contracts during the refactor:
  - API routes
  - UI routes
  - `python -m tasks...` entrypoints
  - env var names
  - deploy manifests
  - Postgres and Delta storage contracts

## Boundary Rules
- `api/` must not import from `tasks.*`.
- `monitoring/` must not import from `tasks.*`.
- `core/` must not import from `tasks.*`, except for temporary compatibility shims that expose shared contracts while call sites migrate.
- `tasks/` may depend on `core/`.
- Shared contracts used by multiple runtime surfaces should be reached through `core/`, not `tasks.common.*`.

## Compatibility Strategy
- Temporary shims in `core/` may proxy legacy `tasks.common.*` modules while downstream callers migrate.
- Those shims are transitional and should be removed once the underlying shared logic is moved fully out of `tasks.common`.
- New cross-surface code should target the `core/` interface first.

## Initial Extraction Priorities
1. Replace direct `api/`, `monitoring/`, and shared `core/` imports from `tasks.common.*` with `core/*` interfaces.
2. Break oversized finance ETL modules into focused parsing, normalization, indexing, and orchestration units.
3. Split `api/endpoints/system.py` and `monitoring/system_health.py` by responsibility.
4. Move the UI toward feature folders while keeping `ui/src/app/App.tsx` as the shell.

## Consequences
- Short term: a thin compatibility layer exists in `core/`.
- Medium term: boundary tests can enforce the new dependency rules in CI.
- Long term: `ui`, `api-service`, `etl-jobs`, and shared/provider surfaces can be extracted into separate repositories without changing external contracts first.
