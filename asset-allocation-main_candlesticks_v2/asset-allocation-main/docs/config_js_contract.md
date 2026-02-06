# `/config.js` Runtime Config Contract

## Summary

The UI **always** loads runtime configuration from the domain root:

- `GET /config.js` (JavaScript that assigns `window.__BACKTEST_UI_CONFIG__` and `window.__API_UI_CONFIG__`)

If `/config.js` is missing or misrouted, the UI falls back to `'/api'` and may produce confusing downstream errors (e.g., repeated `404` on `GET /api/system/health`).

## Contract Requirements

1. **Root path**
   - `/config.js` must be reachable at the **domain root** (not only under a path prefix).

2. **No caching**
   - Response must include `Cache-Control: no-store` to avoid stale config after deploys.

3. **Required keys**
   - The emitted config object must include:
     - `apiBaseUrl`
     - `backtestApiBaseUrl` (alias; kept for backward compatibility)
     - `authMode` and OIDC fields when configured

## Environment Responsibilities

### API (FastAPI)
- Serves `GET /config.js` in `api/service/app.py`.
- Uses `API_ROOT_PREFIX` to determine the default `apiBaseUrl` (`/api` or `/<prefix>/api`).

### UI Container (Nginx)
- Must proxy `GET /config.js` to the API root `/config.js`.
- See `ui/nginx.conf`.

### Local UI Dev (Vite)
- By default, Vite serves `ui/public/config.js` at `GET /config.js`.
- If you want Vite to proxy `GET /config.js` to the API (recommended when testing auth/runtime config), set:
  - `VITE_PROXY_CONFIG_JS=true`
  - `VITE_API_PROXY_TARGET=http://127.0.0.1:8000`

### Ingress / Edge Routing (Front Door / App Gateway / Ingress)
If you route your app under a path prefix (e.g., `/asset-allocation/*`), you still must add a dedicated rule for:
- `/config.js` (exact match) → UI service (preferred if UI proxies it) **or** directly → API service.

Do not rely on prefix-only routing; it commonly breaks the UI’s absolute `/config.js` load.

## Verification (Smoke Checks)

Local:

```bash
curl -i http://127.0.0.1:8000/config.js
curl -i http://127.0.0.1:8000/api/system/health
```

Via UI (dev/prod domain):

```bash
curl -i https://<your-ui-domain>/config.js
curl -i https://<your-ui-domain>/api/system/health
```

