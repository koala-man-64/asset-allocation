// Runtime UI config.
//
// - In production (Option A hosting), the FastAPI service serves a dynamic `/config.js`.
// - In local UI dev, this file is served by Vite.
//
// This file must not contain secrets.
/* global window */

// Local-dev default: route API under a project-specific prefix so multiple apps can share the same host/port.
// The backend also mounts unprefixed `/api/*` for compatibility.
const defaultApiBaseUrl = '/asset-allocation/api';

window.__BACKTEST_UI_CONFIG__ = window.__BACKTEST_UI_CONFIG__ || {};
window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl =
  window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl || defaultApiBaseUrl;

// Compatibility for older/newer clients.
window.__API_UI_CONFIG__ = window.__API_UI_CONFIG__ || {};
window.__API_UI_CONFIG__.apiBaseUrl =
  window.__API_UI_CONFIG__.apiBaseUrl || window.__BACKTEST_UI_CONFIG__.backtestApiBaseUrl;
