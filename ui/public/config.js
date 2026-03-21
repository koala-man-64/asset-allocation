// Runtime UI config.
//
// - In production (Option A hosting), the FastAPI service serves a dynamic `/config.js`.
// - In local UI dev, this file is served by Vite.
//
// This file must not contain secrets.
/* global window */

// Local-dev default: keep API calls on `/api`.
// The backend mounts `/api/*` in all deployment modes.
const defaultApiBaseUrl = '/api';

window.__API_UI_CONFIG__ = {
  ...(window.__API_UI_CONFIG__ || {}),
  apiBaseUrl: (window.__API_UI_CONFIG__ || {}).apiBaseUrl || defaultApiBaseUrl,
};
