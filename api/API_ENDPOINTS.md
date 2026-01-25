# API Endpoints Map (ASCII)
  
```text
API Root
├── /healthz [GET] (app.healthz) - K8s Liveness Probe (Returns 200 OK) :: api/service/app.py
├── /readyz [GET] (app.readyz) - K8s Readiness Probe (Checks DB connectivity) :: api/service/app.py
├── /config.js [GET] (app.serve_runtime_config) - Serves runtime env vars to UI :: api/service/app.py <== ui/src/config.ts (implicit)
└── /api
    ├── /ws/updates [WebSocket] (app.websocket_endpoint) - Real-time job/status updates :: api/service/app.py <== ui/src/hooks/useRealtime.ts
    
    # System & Health (Matches useDataQueries.ts: System & Health)
    ├── /system
    │   ├── /health [GET] (system.system_health) - Returns overall system status, layer freshness, and active alerts :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /lineage [GET] (system.system_lineage) - Returns data lineage graph and dependencies :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   ├── /alerts
    │   │   └── /{alert_id}
    │   │       ├── /ack [POST] (system.acknowledge_alert) - Acknowledges a system alert :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   │       ├── /snooze [POST] (system.snooze_alert) - Snoozes an alert for a specified duration :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   │       └── /resolve [POST] (system.resolve_alert) - Marks an alert as resolved :: api/endpoints/system.py <== ui/src/services/DataService.ts
    │   └── /jobs
    │       └── /{job_name}/run [POST] (system.trigger_job_run) - Manually triggers an Azure Container App Job :: api/endpoints/system.py <== ui/src/services/DataService.ts

    # Strategy & Signals (Matches useDataQueries.ts: Strategy & Signals)
    ├── /ranking
    │   ├── /strategies [GET] (ranking.get_strategies) - Lists all available strategies and their latest status :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   ├── /signals [GET] (ranking.get_signals) - Returns daily trading signals across all strategies :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   └── /{strategy_id} [GET] (ranking.get_strategy_details) - Returns detailed config and sub-models for a strategy :: api/endpoints/ranking.py <== ui/src/services/DataService.ts

    # Backtest Data & Execution
    ├── /backtests
    │   ├── / [GET] (backtests.list_backtests) - Lists historical backtest runs with filtering :: api/endpoints/backtests.py <== ui/src/services/DataService.ts
    │   ├── / [POST] (backtests.submit_backtest) - Submits a new backtest job :: api/endpoints/backtests.py <== (Unused/Partial: ui/src/api/endpoints/backtest.ts)
    │   └── /{run_id}
    │       ├── /status [GET] (backtests.get_status) - Polls current status of a running backtest :: api/endpoints/backtests.py
    │       ├── /summary [GET] (backtests.get_summary) - Returns performance summary (Sharpe, Returns, Drawdown) :: api/endpoints/backtests.py <== ui/src/services/DataService.ts
    │       ├── /trades [GET] (backtests.get_trades) - Returns list of executed trades for a run :: api/endpoints/backtests.py <== ui/src/services/DataService.ts
    │       ├── /metrics
    │       │   ├── /timeseries [GET] (backtests.get_timeseries) - Returns daily equity curve and drawdown series :: api/endpoints/backtests.py <== ui/src/services/DataService.ts
    │       │   └── /rolling [GET] (backtests.get_rolling_metrics) - Returns rolling metrics (volatility, beta etc) :: api/endpoints/backtests.py <== ui/src/services/DataService.ts
    │       └── /artifacts
    │           ├── / [GET] (backtests.list_artifacts) - Lists generated files (logs, plots, csvs) for a run :: api/endpoints/backtests.py
    │           └── /{name:path} [GET] (backtests.get_artifact_content) - Downloads a specific artifact file :: api/endpoints/backtests.py

    # Raw Data Layer
    ├── /data
    │   ├── /{layer}
    │   │   ├── /{domain} [GET] (data.get_data_generic) - generic accessor for Silver/Gold delta tables (prices, earnings) :: api/endpoints/data.py <== ui/src/services/DataService.ts
    │   │   └── /finance/{sub_domain} [GET] (data.get_finance_data) - Specialized accessor for financial statements :: api/endpoints/data.py <== ui/src/services/DataService.ts
```
