import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from api.endpoints import data, system, postgres
from api.service.auth import AuthManager
from api.service.dependencies import get_settings
from api.service.settings import ServiceSettings
from api.service.realtime import manager as realtime_manager
from api.service.alert_state_store import PostgresAlertStateStore
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("asset-allocation.api")

def _request_context(request: Request) -> dict[str, str]:
    return {
        "client": request.client.host if request.client else "unknown",
        "method": request.method,
        "path": request.url.path or "",
        "query": request.url.query or "",
        "host": request.headers.get("host", ""),
        "forwarded_for": request.headers.get("x-forwarded-for", ""),
        "forwarded_proto": request.headers.get("x-forwarded-proto", ""),
        "forwarded_host": request.headers.get("x-forwarded-host", ""),
        "request_id": request.headers.get("x-request-id", "") or request.headers.get("x-correlation-id", ""),
        "user_agent": request.headers.get("user-agent", ""),
    }

def create_app() -> FastAPI:
    # ... (existing inner functions) ...

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = ServiceSettings.from_env()

        app.state.settings = settings
        app.state.auth = AuthManager(settings)

        if settings.postgres_dsn:
            app.state.alert_state_store = PostgresAlertStateStore(settings.postgres_dsn)

        def _system_health_ttl_seconds() -> float:
            raw = os.environ.get("SYSTEM_HEALTH_TTL_SECONDS", "300")
            try:
                ttl = float(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid float for SYSTEM_HEALTH_TTL_SECONDS={raw!r}") from exc
            if ttl <= 0:
                raise ValueError("SYSTEM_HEALTH_TTL_SECONDS must be > 0.")
            return ttl

        app.state.system_health_cache = TtlCache(ttl_seconds=_system_health_ttl_seconds())

        yield

    app = FastAPI(title="Asset Allocation API", version="0.1.0", lifespan=lifespan)
    print(">>> SERVICE APPLICATION STARTING <<<")

    content_security_policy = os.environ.get("API_CSP") or "default-src 'self'; base-uri 'none';"

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        try:
            start = time.monotonic()
            path = request.url.path or ""
            method = request.method
            
            # Simple context extraction for logging (safe access)
            client_host = request.client.host if request.client else "unknown"
            
            response = await call_next(request)
            
            elapsed_ms = (time.monotonic() - start) * 1000.0
            
            # Safe logic for headers
            if path.startswith("/assets/") and response.status_code == 200:
                response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "DENY")
            
            return response
            
        except Exception:
            # Let Starlette's ServerErrorMiddleware handle it, but log it first if needed
            raise

    # CORS Configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:5174",
            "http://127.0.0.1:5174",
            "http://localhost:3000",
            "*",  # Fallback for dev
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API Routers with /api prefix
    app.include_router(data.router, prefix="/api/data", tags=["Data"])
    app.include_router(system.router, prefix="/api/system", tags=["System"])
    app.include_router(postgres.router, prefix="/api/system/postgres", tags=["Postgres"])

    @app.websocket("/api/ws/updates")
    async def websocket_endpoint(websocket: WebSocket):
        await realtime_manager.connect(websocket)
        try:
            while True:
                # Receive generic text (ping/pong) or JSON (subscribe/unsubscribe)
                data_str = await websocket.receive_text()
                
                # Health Check Protocol
                if data_str == "ping":
                    await websocket.send_text("pong")
                    continue
                
                # Command Protocol
                try:
                    msg = json.loads(data_str)
                    action = msg.get("action")
                    topics = msg.get("topics", [])
                    
                    if not isinstance(topics, list):
                        continue

                    if action == "subscribe":
                        await realtime_manager.subscribe(websocket, topics)
                    elif action == "unsubscribe":
                        await realtime_manager.unsubscribe(websocket, topics)

                except json.JSONDecodeError:
                    pass  # Ignore non-JSON messages (unless it was ping)
                    
        except WebSocketDisconnect:
            realtime_manager.disconnect(websocket)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ready"})

    @app.get("/config.js")
    async def get_ui_config(request: Request):
        settings: ServiceSettings = app.state.settings
        cfg = {
            "authMode": settings.ui_auth_mode,
            "apiBaseUrl": settings.ui_oidc_config.get("apiBaseUrl") or "/api",
            "oidcAuthority": settings.ui_oidc_config.get("authority"),
            "oidcClientId": settings.ui_oidc_config.get("clientId"),
            "oidcScopes": settings.ui_oidc_config.get("scope") or settings.ui_oidc_config.get("scopes"),
            "oidcRedirectUri": settings.ui_oidc_config.get("redirectUri") or "/oauth2-callback",
            "oidcAudience": settings.oidc_audience,
        }
        logger.info(
            "Serving /config.js: authMode=%s apiBaseUrl=%s",
            cfg.get("authMode"),
            cfg.get("apiBaseUrl"),
        )
        content = f"window.__API_UI_CONFIG__ = {json.dumps(cfg)};"
        return Response(
            content=content,
            media_type="application/javascript",
            headers={"Cache-Control": "no-store"},
        )

    ui_dist_env = os.environ.get("UI_DIST_DIR")
    if ui_dist_env:
        dist_path = Path(ui_dist_env).resolve()
        if dist_path.exists() and dist_path.is_dir():
            logger.info("Serving UI from %s", dist_path)
            from fastapi.staticfiles import StaticFiles
            assets_path = dist_path / "assets"
            if assets_path.exists():
                app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")

            @app.get("/{rest_of_path:path}")
            async def serve_index(rest_of_path: str):
                file_path = dist_path / rest_of_path
                if rest_of_path and file_path.exists() and file_path.is_file():
                    return FileResponse(file_path)
                return FileResponse(dist_path / "index.html", headers={"Cache-Control": "no-store"})
        else:
            logger.warning("UI_DIST_DIR set but invalid: %s", ui_dist_env)
    else:
        logger.info("UI_DIST_DIR not set. UI will not be served.")


            
    return app

app = create_app()
