# API Endpoints Map (ASCII)
  
```text
API Root
├── /docs [GET] (app.docs_redirect) - Redirects to active Swagger UI path :: api/service/app.py
├── /openapi.json [GET] (app.openapi_redirect) - Redirects to active OpenAPI JSON path :: api/service/app.py
├── /healthz [GET] (app.healthz) - K8s Liveness Probe (Returns 200 OK) :: api/service/app.py
├── /readyz [GET] (app.readyz) - K8s Readiness Probe (Checks DB connectivity) :: api/service/app.py
├── /config.js [GET] (app.serve_runtime_config) - Serves runtime env vars to UI :: api/service/app.py <== ui/src/config.ts (implicit)
├── /api/ws/updates [WEBSOCKET] (app.websocket_endpoint) - Real-time updates for UI :: api/service/app.py
└── /api
    ├── /docs [GET] (app.swagger_ui) - Browser Swagger UI docs :: api/service/app.py
    ├── /openapi.json [GET] (app.openapi_json) - OpenAPI spec payload :: api/service/app.py
    ├── /ws/updates [WebSocket] (app.websocket_endpoint) - Real-time updates (system health/alerts) :: api/service/app.py <== ui/src/hooks/useRealtime.ts
    
    # System & Health (Matches ui/src/hooks/useDataQueries.ts)
    ├── /system
    │   ├── /health [GET] (system.system_health) - Returns overall system status, layer freshness, and active alerts :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /lineage [GET] (system.system_lineage) - Returns data lineage graph and dependencies :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /debug-symbols [GET] (system.get_debug_symbols) - Returns debug-symbol config from Postgres :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /debug-symbols [POST] (system.set_debug_symbols) - Updates debug-symbol config in Postgres :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /runtime-config/catalog [GET] (system.get_runtime_config_catalog) - Lists allowlisted runtime-config keys :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /runtime-config [GET] (system.get_runtime_config) - Lists runtime-config overrides for a scope :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /runtime-config [POST] (system.set_runtime_config) - Upserts a runtime-config override :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /runtime-config/{key} [DELETE] (system.remove_runtime_config) - Deletes a runtime-config override :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /alerts
    │   │   └── /{alert_id}
    │   │       ├── /ack [POST] (system.acknowledge_alert) - Acknowledges a system alert :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/AlertHistory.tsx
    │   │       ├── /snooze [POST] (system.snooze_alert) - Snoozes an alert for a specified duration :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/AlertHistory.tsx
    │   │       └── /resolve [POST] (system.resolve_alert) - Marks an alert as resolved :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/AlertHistory.tsx
    │   └── /jobs
    │       ├── /{job_name}/run [POST] (system.trigger_job_run) - Manually triggers an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobTrigger.ts
    │       ├── /{job_name}/suspend [POST] (system.suspend_job) - Suspends an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobSuspend.ts
    │       ├── /{job_name}/resume [POST] (system.resume_job) - Resumes an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobSuspend.ts
    │       └── /{job_name}/logs [GET] (system.get_job_logs) - Returns log tail for last N Job runs :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/JobLogDrawer.tsx

    # Backtest Data & Execution (Matches ui/src/services/backtestHooks.ts)
    ├── /backtests [GET] (backtests.list_backtests) - Lists historical backtest runs with filtering :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
    ├── /backtests [POST] (backtests.submit_backtest) - Submits a new backtest job :: api/endpoints/backtests.py (UI submit not wired)
    └── /backtests/{run_id}
        ├── /status [GET] (backtests.get_status) - Polls current status of a running backtest :: api/endpoints/backtests.py (not used by UI)
        ├── /summary [GET] (backtests.get_summary) - Returns performance summary (Sharpe, Returns, Drawdown) :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        ├── /trades [GET] (backtests.get_trades) - Returns list of executed trades for a run :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        ├── /metrics
        │   ├── /timeseries [GET] (backtests.get_timeseries) - Returns daily equity curve and drawdown series :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        │   └── /rolling [GET] (backtests.get_rolling_metrics) - Returns rolling metrics (volatility, beta etc) :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        ├── /artifacts [GET] (backtests.list_artifacts) - Lists generated files (logs, plots, csvs) for a run :: api/endpoints/backtests.py (UI artifacts not wired)
        └── /artifacts/{name:path} [GET] (backtests.get_artifact_content) - Downloads a specific artifact file :: api/endpoints/backtests.py (UI artifacts not wired)

    # Providers (ETL Gateway)
    ├── /providers
    │   └── /alpha-vantage
    │       ├── /listing-status [GET] (alpha_vantage.get_listing_status) - Alpha Vantage LISTING_STATUS CSV :: api/endpoints/alpha_vantage.py
    │       ├── /time-series/daily [GET] (alpha_vantage.get_daily_time_series) - Alpha Vantage TIME_SERIES_DAILY CSV :: api/endpoints/alpha_vantage.py
    │       ├── /earnings [GET] (alpha_vantage.get_earnings) - Alpha Vantage EARNINGS payload :: api/endpoints/alpha_vantage.py
    │       └── /finance/{report} [GET] (alpha_vantage.get_finance_report) - Alpha Vantage finance payload :: api/endpoints/alpha_vantage.py

    # Raw Data Layer
    ├── /data
    │   ├── /symbols [GET] (data.list_symbols) - Returns Postgres symbol universe :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /screener [GET] (data.get_stock_screener) - Daily screener snapshot (Silver+Gold+Postgres) :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /{layer}
    │   │   ├── /{domain} [GET] (data.get_data_generic) - generic accessor for Silver/Gold delta tables (prices, earnings) :: api/endpoints/data.py <== ui/src/services/DataService.ts
    │   │   └── /finance/{sub_domain} [GET] (data.get_finance_data) - Specialized accessor for financial statements :: api/endpoints/data.py <== ui/src/services/DataService.ts
```
