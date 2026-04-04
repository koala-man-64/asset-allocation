# Runtime Surface Test Targets

This document lists the canonical, runnable validation commands for the refactored runtime surfaces.

## Conventions

- Run Python commands from the repository root: `C:\Users\rdpro\Projects\AssetAllocation`
- Run UI commands from the UI package root: `C:\Users\rdpro\Projects\AssetAllocation\ui`
- Use these commands as the handoff-safe validation set for the current refactor baseline

## Canonical Commands

### Full Python Closeout

```powershell
python -m pytest
```

Purpose:
- end-to-end regression gate for `WI-RSR-001` through `WI-RSR-004`
- required before marking backend/runtime work done

### Architecture Boundary Guardrail

```powershell
python -m pytest tests/architecture/test_python_module_boundaries.py -q
```

Purpose:
- verifies `api/`, `monitoring/`, and non-shim `core/` modules stay off direct `tasks.*` imports

### System Facade Compatibility

```powershell
python -m pytest tests/api/test_debug_symbols_endpoints.py tests/api/test_runtime_config_endpoints.py tests/api/test_system_container_apps_endpoints.py tests/api/test_system_domain_metadata_cache.py tests/api/test_system_job_logs_endpoints.py -q
```

Purpose:
- validates the `api.endpoints.system` facade after extraction into `api/endpoints/system_modules/*`
- catches missing monkeypatch surfaces and route-module runtime dependencies

### Monitoring Health Surface

```powershell
python -m pytest tests/monitoring/test_system_health.py tests/monitoring/test_system_health_staleness.py tests/monitoring/test_phase3b_signals.py tests/tasks/test_blob_freshness.py -q
```

Purpose:
- validates the `monitoring.system_health` facade after extraction into `monitoring/system_health_modules/*`

### Finance Silver Pilot

```powershell
python -m pytest tests/finance_data/test_silver_finance_data.py -q
```

Purpose:
- validates the silver finance entrypoint after extraction into `silver_parsing.py` and `silver_frames.py`

### Full UI Closeout

```powershell
pnpm exec vitest run
```

Purpose:
- end-to-end regression gate for `WI-RSR-005`
- validates `ui/src/app/routes.tsx`, `ui/src/features/*`, and compatibility wrappers under `ui/src/app/components/pages/*`
