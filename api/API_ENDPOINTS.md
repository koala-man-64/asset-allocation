# API Endpoints Map (ASCII)
  
```text
API Root
├── /healthz [GET] (app.healthz) - K8s Liveness Probe (Returns 200 OK) :: api/service/app.py
├── /readyz [GET] (app.readyz) - K8s Readiness Probe (Checks DB connectivity) :: api/service/app.py
├── /config.js [GET] (app.serve_runtime_config) - Serves runtime env vars to UI :: api/service/app.py <== ui/src/config.ts (implicit)
├── /api/ws/updates [WEBSOCKET] (app.websocket_endpoint) - Real-time updates for UI :: api/service/app.py
└── /api
    ├── /ws/updates [WebSocket] (app.websocket_endpoint) - Real-time updates (system health/alerts) :: api/service/app.py <== ui/src/hooks/useRealtime.ts
    
    # System & Health (Matches ui/src/hooks/useDataQueries.ts)
    ├── /system
    │   ├── /health [GET] (system.system_health) - Returns overall system status, layer freshness, and active alerts :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /lineage [GET] (system.system_lineage) - Returns data lineage graph and dependencies :: api/endpoints/system.py <== ui/src/services/DataService.ts
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

    # Strategy & Signals (Matches ui/src/hooks/useDataQueries.ts)
    ├── /ranking
    │   ├── /strategies [GET] (ranking.get_strategies) - Lists all available strategies and their latest status :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   ├── /signals [GET] (ranking.get_signals) - Returns daily trading signals across all strategies :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   └── /{strategy_id} [GET] (ranking.get_strategy_details) - Returns detailed config and sub-models for a strategy :: api/endpoints/ranking.py <== ui/src/services/DataService.ts

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

    # Raw Data Layer
    ├── /data
    │   ├── /symbols [GET] (data.list_symbols) - Returns Postgres symbol universe :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /screener [GET] (data.get_stock_screener) - Daily screener snapshot (Silver+Gold+Postgres) :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /{layer}
    │   │   ├── /{domain} [GET] (data.get_data_generic) - generic accessor for Silver/Gold delta tables (prices, earnings) :: api/endpoints/data.py <== ui/src/services/DataService.ts
    │   │   └── /finance/{sub_domain} [GET] (data.get_finance_data) - Specialized accessor for financial statements :: api/endpoints/data.py <== ui/src/services/DataService.ts
```
