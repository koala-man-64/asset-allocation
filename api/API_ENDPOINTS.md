# API Endpoints Map (ASCII)
  
```text
API Root
├── /healthz [GET] (app.healthz) - K8s Liveness Probe (Returns 200 OK) :: api/service/app.py
├── /readyz [GET] (app.readyz) - K8s Readiness Probe (Checks DB connectivity) :: api/service/app.py
├── /config.js [GET] (app.serve_runtime_config) - Serves runtime env vars to UI :: api/service/app.py <== ui/src/config.ts (implicit)
├── /api/ws/updates [WEBSOCKET] (app.websocket_endpoint) - Real-time updates for UI :: api/service/app.py
└── /api
    
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
    │       └── /{job_name}/logs [GET] (system.get_job_logs) - Returns log tail for last N Job runs :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/JobLogDrawer.tsx

    # Strategy & Signals (Matches ui/src/hooks/useDataQueries.ts)
    ├── /ranking
    │   ├── /strategies [GET] (ranking.get_strategies) - Lists all available strategies and their latest status :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   ├── /signals [GET] (ranking.get_signals) - Returns daily trading signals across all strategies :: api/endpoints/ranking.py <== ui/src/services/DataService.ts
    │   └── /{strategy_id} [GET] (ranking.get_strategy_details) - Returns detailed config and sub-models for a strategy :: api/endpoints/ranking.py <== ui/src/services/DataService.ts

    # Raw Data Layer
    ├── /data
    │   ├── /symbols [GET] (data.list_symbols) - Returns Postgres symbol universe :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /screener [GET] (data.get_stock_screener) - Daily screener snapshot (Silver+Gold+Postgres) :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    │   ├── /{layer}
    │   │   ├── /{domain} [GET] (data.get_data_generic) - generic accessor for Silver/Gold delta tables (prices, earnings) :: api/endpoints/data.py <== ui/src/services/DataService.ts
    │   │   └── /finance/{sub_domain} [GET] (data.get_finance_data) - Specialized accessor for financial statements :: api/endpoints/data.py <== ui/src/services/DataService.ts
```
