import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from api.endpoints import aliases, backtests, data, ranking, system
from backtest.service.adls_run_store import AdlsRunStore
from backtest.service.auth import AuthManager
from backtest.service.dependencies import get_settings, get_store
from backtest.service.job_manager import JobManager
from backtest.service.postgres_run_store import PostgresRunStore
from backtest.service.realtime import listen_to_postgres, manager
from backtest.service.run_store import RunStore
from backtest.service.settings import ServiceSettings
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("backtest.api")


def create_app() -> FastAPI:
    def _require_env(name: str) -> str:
        raw = os.environ.get(name)
        if raw is None or not raw.strip():
            raise ValueError(f"{name} is required.")
        return raw.strip()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = ServiceSettings.from_env()
        if settings.run_store_mode == "adls":
            store = AdlsRunStore(settings.adls_runs_dir or "")
        elif settings.run_store_mode == "postgres":
            store = PostgresRunStore(settings.postgres_dsn or "")
        else:
            store = RunStore(settings.db_path)
        store.init_db()
        reconciled = store.reconcile_incomplete_runs()
        if reconciled:
            logger.warning("Reconciled %d incomplete runs on startup.", reconciled)

        manager = JobManager(
            store=store,
            output_base_dir=settings.output_base_dir,
            max_workers=settings.max_concurrent_runs,
            default_adls_dir=settings.adls_runs_dir,
        )
        auth = AuthManager(settings)

        app.state.settings = settings
        app.state.store = store
        app.state.manager = manager
        app.state.auth = auth
        
        # Start Postgres listener for real-time updates
        app.state.listener_task = asyncio.create_task(listen_to_postgres(settings))
        
        # System Health Cache
        def _system_health_ttl_seconds() -> float:
            raw = _require_env("SYSTEM_HEALTH_TTL_SECONDS")
            try:
                ttl = float(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid float for SYSTEM_HEALTH_TTL_SECONDS={raw!r}") from exc
            if ttl <= 0:
                raise ValueError("SYSTEM_HEALTH_TTL_SECONDS must be > 0.")
            return ttl

        app.state.system_health_cache = TtlCache(ttl_seconds=_system_health_ttl_seconds())

        try:
            yield
        finally:
            if hasattr(app.state, "listener_task"):
                app.state.listener_task.cancel()
                try:
                    await app.state.listener_task
                except asyncio.CancelledError:
                    pass
            manager.shutdown()

    app = FastAPI(title="Backtest Service", version="0.1.0", lifespan=lifespan)
    print(">>> BACKTEST SERVICE APPLICATION STARTING <<<")

    content_security_policy = _require_env("BACKTEST_CSP")

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        path = request.url.path or ""
        # Trailing slash redirect for backtests
        if path.startswith("/backtests") and path.endswith("/"):
            url = request.url.replace(path=path.rstrip("/"))
            return RedirectResponse(url=str(url), status_code=307)

        response = await call_next(request)

        if path.startswith("/assets/") and response.status_code == 200:
            response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        response.headers.setdefault("Content-Security-Policy", content_security_policy)

        return response

    # CORS Configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:3000",
            "http://localhost:8000",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API Routers
    app.include_router(data.router, prefix="/data", tags=["Data"])
    app.include_router(ranking.router, prefix="/ranking", tags=["Ranking"])
    app.include_router(aliases.router, tags=["Aliases"])
    app.include_router(system.router, prefix="/system", tags=["System"])
    app.include_router(backtests.router, prefix="/backtests", tags=["Backtests"])

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz(request: Request) -> JSONResponse:
        settings = get_settings(request)
        store = get_store(request)
        try:
            store.init_db()
            if hasattr(store, "ping"):
                store.ping()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"DB not ready: {exc}") from exc
        if not settings.output_base_dir.exists():
            raise HTTPException(status_code=503, detail="Output dir not ready.")
        return JSONResponse({"status": "ok"})

    # Setup UI Static Files (Catch-all at root must be last)
    from fastapi.staticfiles import StaticFiles

    # We can't query settings from request here easily, but we have app.state.settings
    # However, create_app context: settings is loaded in lifespan.
    # We need to add routes dynamically or check env var now?
    # Original logic used os.getenv matching ServiceSettings.from_env() logic or similar.
    # The safest way is to read env directly here for setup.
    ui_dist_dir = os.environ.get("BACKTEST_UI_DIST_DIR")
    if ui_dist_dir and os.path.exists(ui_dist_dir):
        logger.info(f"Serving UI from {ui_dist_dir}")
        dist_path = Path(ui_dist_dir)
        assets_path = dist_path / "assets"
        if assets_path.exists():
             app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")
        
        @app.get("/config.js")
        def get_ui_config(request: Request) -> Response:
             s: ServiceSettings = get_settings(request)
             # Reuse logic from original app.py to build config
             cfg = {
                 "backtestApiBaseUrl": s.ui_oidc_config.get("apiBaseUrl") or "",
                 "authMode": s.auth_mode, 
             }
             if s.auth_mode == "oidc":
                 cfg.update({
                     "oidcAuthority": s.ui_oidc_config.get("authority"),
                     "oidcClientId": s.ui_oidc_config.get("clientId"),
                     "oidcScope": s.ui_oidc_config.get("scope"),
                     "oidcRedirectUri": s.ui_oidc_config.get("redirectUri") or "/oauth2-callback",
                 })
             content = f"window.__BACKTEST_UI_CONFIG__ = {json.dumps(cfg)};"
             return Response(content=content, media_type="application/javascript", headers={"Cache-Control": "no-store"})

        @app.get("/{rest_of_path:path}")
        async def serve_index(rest_of_path: str):
             # Fallback to index.html for SPA routing
             # But if it matches a file in dist, serve it?
             # Simplest SPA pattern: 
             file_path = dist_path / rest_of_path
             if rest_of_path and file_path.exists() and file_path.is_file():
                 return FileResponse(file_path)
             return FileResponse(dist_path / "index.html", headers={"Cache-Control": "no-store"})

    else:
        logger.warning("BACKTEST_UI_DIST_DIR not set or invalid. UI will not be served.")


    # Websocket endpoint remains here or moves?
    # It communicates with manager. It's concise. I will leave it here or move to a realtime router?
    # Logic: manager.connect(websocket). 
    # Decision: Keep here for now as it's a root /ws/updates.
    from fastapi import WebSocket, WebSocketDisconnect

    @app.websocket("/ws/updates")
    async def websocket_endpoint(websocket: WebSocket):
        # We need to get manager from app state, but websocket doesn't have request.app directly in same way?
        # WebSocket is a subclass of Request roughly (Starlette). 
        # app.state is accessible via websocket.app.state
        mgr: JobManager = websocket.app.state.manager
        await mgr.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            mgr.disconnect(websocket)
            
    return app


# Uvicorn entrypoint (used by `uvicorn backtest.service.app:app` and Dockerfile.backtest_api).
# Note: tests should import and call `create_app()` directly when they need to control env vars.
app = create_app()
