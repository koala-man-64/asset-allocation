# Production Deployment Scope (Unified App + Jobs)

As of 2026-02-22, this repository's production deployment target includes:
- a single Azure Container App (`asset-allocation-api`) running both API and UI containers, and
- all data pipeline Container Apps Jobs.

## In Scope
- Build and publish three images:
  - `asset-allocation-scraper` (jobs)
  - `asset-allocation-api` (FastAPI)
  - `asset-allocation-ui` (Nginx SPA)
- Deploy the unified app from `deploy/app_api.yaml`:
  - ingress routed to UI container on port `80`
  - UI proxies API paths to sidecar on `127.0.0.1:8000`
  - scale: `minReplicas=0`, `maxReplicas=1` (total app budget `2 CPU / 4Gi`)
- Deploy Container Apps Jobs from `deploy/job_*.yaml`:
  - Bronze jobs are schedule-triggered.
  - Silver and Gold jobs are manual and typically triggered downstream.

## Out of Scope (Legacy)
- Standalone UI app deployment from `deploy/app_ui.yaml` (legacy rollback artifact only).

## Notes
- Unified deployment is designed for lower idle cost while preserving one-host UI/API behavior.
- Jobs still specify their own `command` in job YAML templates (for example `python -m tasks.*`), so image `CMD` is not relied upon.
