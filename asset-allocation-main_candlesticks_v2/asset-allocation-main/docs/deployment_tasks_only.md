# Production Deployment Scope (Tasks-Only)

As of 2026-01-28, this repository's production deployment target is **tasks-only** (Azure Container Apps Jobs).

## In Scope
- Build and publish the **task/job** container image (currently `asset-allocation-scraper`).
- Deploy and run scheduled Container Apps **Jobs** using `deploy/job_*.yaml`.

## Out of Scope (Deprecated for Production)
- Deploying the **API** container app (`deploy/app_backtest_api.yaml`).
- Deploying the **UI** container app (`deploy/app_ui.yaml`).

## Notes
- Jobs specify their own `command` in the job YAML templates (e.g. `python -m tasks.*`), so the image `CMD` is not relied upon.
- Local development for API/UI may still be supported, but production CI/CD should not require or deploy API/UI artifacts.

