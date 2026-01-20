// Runtime UI config for the Backtest UI.
//
// - In production (Option A hosting), the FastAPI service serves a dynamic `/config.js`.
// - In local UI dev, this file is served by Vite and can be overridden by Vite env vars.
//
// This file must not contain secrets.
window.__BACKTEST_UI_CONFIG__ = window.__BACKTEST_UI_CONFIG__ || {};
