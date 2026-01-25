// Runtime UI config for the Backtest UI.
//
// - In production (Option A hosting), the FastAPI service serves a dynamic `/config.js`.
// - In local UI dev, this file is served by Vite and can be overridden by Vite env vars.
//
// This file must not contain secrets.
/* global window */
window.__BACKTEST_UI_CONFIG__ = window.__BACKTEST_UI_CONFIG__ || {};

// Local-dev defaults:
// - The backend mounts routers under `/api/*` (see `api/service/app.py`).
// - Vite dev server proxies `/api` to `http://localhost:8000` (see `ui/vite.config.ts`).
window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl =
  window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl || '/api';
