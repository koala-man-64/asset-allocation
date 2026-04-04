# Runtime Surface Extraction Manifest

This manifest records the current extraction-ready surface boundaries after the runtime-surface refactor.

## Surface Inventory

### API System Surface

- Public facade: `api/endpoints/system.py`
- Extracted modules:
  - `api/endpoints/system_modules/status_read.py`
  - `api/endpoints/system_modules/domain_metadata.py`
  - `api/endpoints/system_modules/domain_columns.py`
  - `api/endpoints/system_modules/purge.py`
  - `api/endpoints/system_modules/runtime_ops.py`
  - `api/endpoints/system_modules/container_apps.py`
  - `api/endpoints/system_modules/jobs.py`
- Compatibility rule:
  - keep `api.endpoints.system` as the route import, monkeypatch, and helper re-export surface until downstream tests and callers no longer depend on it directly

### Monitoring Health Surface

- Public facade: `monitoring/system_health.py`
- Extracted modules:
  - `monitoring/system_health_modules/env_config.py`
  - `monitoring/system_health_modules/signals.py`
  - `monitoring/system_health_modules/job_queries.py`
  - `monitoring/system_health_modules/freshness.py`
  - `monitoring/system_health_modules/alerts.py`
  - `monitoring/system_health_modules/snapshot.py`
- Compatibility rule:
  - keep `monitoring.system_health` as the import and patch surface for snapshot orchestration, helper access, constants, and Azure client seams

### Shared Runtime Contracts

- Public shared surface:
  - `core/bronze_bucketing.py`
  - `core/layer_bucketing.py`
  - `core/domain_artifacts.py`
  - `core/domain_metadata_snapshots.py`
  - `core/finance_contracts.py`
  - `core/market_symbols.py`
  - `core/gold_sync_contracts.py`
- Compatibility rule:
  - `api/`, `monitoring/`, and non-shim `core/` consumers import shared contracts through `core/*`, not `tasks.common.*`

### Finance Silver Surface

- Public entrypoint: `tasks/finance_data/silver_finance_data.py`
- Extracted helpers:
  - `tasks/finance_data/silver_parsing.py`
  - `tasks/finance_data/silver_frames.py`
- Compatibility rule:
  - keep `silver_finance_data.py` as the orchestration entrypoint and exported helper surface while delegating parsing/frame logic to extracted modules

### UI Application Surface

- Public shell:
  - `ui/src/app/App.tsx`
  - `ui/src/app/routes.tsx`
- Extracted feature entrypoints:
  - `ui/src/features/data-explorer/DataExplorerPage.tsx`
  - `ui/src/features/regimes/RegimeMonitorPage.tsx`
  - `ui/src/features/system-status/SystemStatusPage.tsx`
  - `ui/src/features/data-quality/DataQualityPage.tsx`
  - `ui/src/features/data-profiling/DataProfilingPage.tsx`
  - `ui/src/features/debug-symbols/DebugSymbolsPage.tsx`
  - `ui/src/features/runtime-config/RuntimeConfigPage.tsx`
  - `ui/src/features/symbol-purge/SymbolPurgeByCriteriaPage.tsx`
  - `ui/src/features/stocks/StockExplorerPage.tsx`
  - `ui/src/features/stocks/StockDetailPage.tsx`
  - `ui/src/features/postgres-explorer/PostgresExplorerPage.tsx`
  - `ui/src/features/strategies/StrategyConfigPage.tsx`
  - `ui/src/features/universes/UniverseConfigPage.tsx`
  - `ui/src/features/rankings/RankingConfigPage.tsx`
  - `ui/src/features/strategy-exploration/StrategyDataCatalogPage.tsx`
- Compatibility wrappers:
  - `ui/src/app/components/pages/*.tsx` for the routed page entry files
- Compatibility rule:
  - keep `App.tsx` as the providers/auth/layout shell
  - keep legacy `ui/src/app/components/pages/*` entry imports available until downstream tests and imports are fully migrated

## Extraction Readiness Notes

- Backend extraction is now facade-first, not package-split. The stable extraction boundaries are the facade modules, not the internal helper files.
- UI extraction is route-entry-first. Feature folders own routed pages, while shared layout, auth, common components, and low-level UI primitives remain under `ui/src/app/components/*`.
- The remaining extraction work after this document is organizational or packaging-oriented, not structural code splitting within the audited runtime surfaces.
