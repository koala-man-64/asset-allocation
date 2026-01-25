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

from api.endpoints import backtests, data, ranking, system
from api.service.adls_run_store import AdlsRunStore
from api.service.auth import AuthManager
from api.service.dependencies import get_settings, get_store
from api.service.job_manager import JobManager
from api.service.postgres_run_store import PostgresRunStore
from api.service.realtime import listen_to_postgres, manager
from api.service.run_store import RunStore
from api.service.settings import ServiceSettings
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

        manager_instance = JobManager(
            store=store,
            output_base_dir=settings.output_base_dir,
            max_workers=settings.max_concurrent_runs,
            default_adls_dir=settings.adls_runs_dir,
        )

        app.state.settings = settings
        app.state.store = store
        app.state.manager = manager_instance
        app.state.auth = AuthManager(settings)

        if settings.postgres_dsn and settings.run_store_mode == "postgres":
            app.state.listener_task = asyncio.create_task(
                listen_to_postgres(settings)
            )

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

        try:
            yield
        finally:
            if hasattr(app.state, "listener_task"):
                app.state.listener_task.cancel()
                try:
                    await app.state.listener_task
                except asyncio.CancelledError:
                    pass
            manager_instance.shutdown()

    app = FastAPI(title="Backtest Service", version="0.1.0", lifespan=lifespan)
    print(">>> BACKTEST SERVICE APPLICATION STARTING <<<")

    content_security_policy = _require_env("BACKTEST_CSP")

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        ctx = _request_context(request)
        start = time.monotonic()
        logger.info(
            "HTTP request start: method=%s path=%s query=%s client=%s host=%s fwd=%s proto=%s req_id=%s ua=%s",
            ctx["method"],
            ctx["path"],
            ctx["query"],
            ctx["client"],
            ctx["host"],
            ctx["forwarded_for"],
            ctx["forwarded_proto"],
            ctx["request_id"],
            ctx["user_agent"],
        )
        path = request.url.path or ""
        # Trailing slash redirect for backtests
        if path.startswith("/api/backtests") and path.endswith("/"):
            url = request.url.replace(path=path.rstrip("/"))
            elapsed_ms = (time.monotonic() - start) * 1000.0
            logger.info(
                "HTTP redirect: method=%s from=%s to=%s status=%s elapsed_ms=%.2f",
                ctx["method"],
                path,
                url.path,
                307,
                elapsed_ms,
            )
            return RedirectResponse(url=str(url), status_code=307)

        response = await call_next(request)
        elapsed_ms = (time.monotonic() - start) * 1000.0
        logger.info(
            "HTTP request end: method=%s path=%s status=%s elapsed_ms=%.2f cache=%s stale=%s",
            ctx["method"],
            ctx["path"],
            response.status_code,
            elapsed_ms,
            response.headers.get("X-System-Health-Cache", ""),
            response.headers.get("X-System-Health-Stale", ""),
        )

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
            "*",  # Fallback for dev
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Include API Routers with /api prefix
    app.include_router(data.router, prefix="/api/data", tags=["Data"])
    app.include_router(ranking.router, prefix="/api/ranking", tags=["Ranking"])
    app.include_router(system.router, prefix="/api/system", tags=["System"])
    app.include_router(backtests.router, prefix="/api/backtests", tags=["Backtests"])

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz(request: Request) -> JSONResponse:
        try:
            get_store(request).ping()
            return JSONResponse({"status": "ready"})
        except Exception as exc:
            logger.error("Readiness check failed: %s", exc)
            return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)

    @app.get("/config.js")
    async def serve_runtime_config(request: Request):
        settings = get_settings(request)
        cfg = {
            "authMode": settings.ui_auth_mode,
            "oidcIssuer": settings.oidc_issuer,
            "oidcAudience": settings.oidc_audience,
            "oidcClientId": settings.ui_oidc_config.get("clientId"),
            "oidcAuthority": settings.ui_oidc_config.get("authority"),
            "oidcScopes": settings.ui_oidc_config.get("scope") or settings.ui_oidc_config.get("scopes"),
            "backtestApiBaseUrl": settings.ui_oidc_config.get("apiBaseUrl") or "/api",
            "oidcRedirectUri": settings.ui_oidc_config.get("redirectUri") or "/oauth2-callback",
        }
        logger.info(
            "Serving /config.js: authMode=%s apiBaseUrl=%s oidcAuthority=%s oidcClientId=%s",
            cfg.get("authMode"),
            cfg.get("backtestApiBaseUrl"),
            cfg.get("oidcAuthority"),
            cfg.get("oidcClientId"),
        )
        content = f"window.__BACKTEST_UI_CONFIG__ = {json.dumps(cfg)};"
        return Response(
            content=content,
            media_type="application/javascript",
            headers={"Cache-Control": "no-store"},
        )

    ui_dist_env = os.environ.get("BACKTEST_UI_DIST_DIR")
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
            logger.warning("BACKTEST_UI_DIST_DIR set but invalid: %s", ui_dist_env)
    else:
        logger.info("BACKTEST_UI_DIST_DIR not set. UI will not be served.")

    @app.websocket("/api/ws/updates")
    async def websocket_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            manager.disconnect(websocket)
            
    return app

app = create_app()
