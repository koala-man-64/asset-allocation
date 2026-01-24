from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

from api.endpoints import aliases, backtests, data, ranking, system
from backtest.service.adls_run_store import AdlsRunStore
from backtest.service.auth import AuthManager
from backtest.service.dependencies import get_settings, get_store
from backtest.service.job_manager import JobManager
from backtest.service.postgres_run_store import PostgresRunStore
from backtest.service.realtime import listen_to_postgres, manager as realtime_manager
from backtest.service.run_store import RunStore
from backtest.service.settings import ServiceSettings
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("backtest.api")


def create_app() -> FastAPI:
    api_prefix = os.environ.get("ASSET_ALLOCATION_API_PREFIX", "/api").strip()
    if not api_prefix:
        api_prefix = "/api"
    if not api_prefix.startswith("/"):
        api_prefix = f"/{api_prefix}"
    api_prefix = api_prefix.rstrip("/")

    def _require_env(name: str) -> str:
        raw = os.environ.get(name)
        if raw is None or not raw.strip():
            raise ValueError(f"{name} is required.")
        return raw.strip()

    content_security_policy = _require_env("BACKTEST_CSP")

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

        job_manager = JobManager(
            store=store,
            output_base_dir=settings.output_base_dir,
            max_workers=settings.max_concurrent_runs,
            default_adls_dir=settings.adls_runs_dir,
        )
        auth = AuthManager(settings)

        app.state.settings = settings
        app.state.store = store
        app.state.manager = job_manager
        app.state.auth = auth

        app.state.listener_task = asyncio.create_task(listen_to_postgres(settings))

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
            task = getattr(app.state, "listener_task", None)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            job_manager.shutdown()

    app = FastAPI(title="Backtest Service", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        path = request.url.path or ""
        if path.startswith(f"{api_prefix}/backtests") and path.endswith("/"):
            url = request.url.replace(path=path.rstrip("/"))
            return RedirectResponse(url=str(url), status_code=307)

        response = await call_next(request)

        if path.startswith("/assets/") and response.status_code == 200:
            response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

        response.headers.setdefault("Content-Security-Policy", content_security_policy)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")

        return response

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

    app.include_router(data.router, prefix=f"{api_prefix}/data", tags=["Data"])
    app.include_router(ranking.router, prefix=f"{api_prefix}/ranking", tags=["Ranking"])
    app.include_router(aliases.router, prefix=api_prefix, tags=["Aliases"])
    app.include_router(system.router, prefix=f"{api_prefix}/system", tags=["System"])
    app.include_router(backtests.router, prefix=f"{api_prefix}/backtests", tags=["Backtests"])

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

    ui_dist_dir = os.environ.get("BACKTEST_UI_DIST_DIR")
    if ui_dist_dir and os.path.exists(ui_dist_dir):
        logger.info("Serving UI from %s", ui_dist_dir)
        dist_path = Path(ui_dist_dir)
        assets_path = dist_path / "assets"
        if assets_path.exists():
            app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")

        @app.get("/config.js")
        def get_ui_config(request: Request) -> Response:
            settings: ServiceSettings = get_settings(request)
            api_base_url_raw = settings.ui_oidc_config.get("apiBaseUrl")
            api_base_url = api_base_url_raw.strip() if isinstance(api_base_url_raw, str) else ""
            if not api_base_url:
                api_base_url = api_prefix
            cfg = {
                "backtestApiBaseUrl": api_base_url,
                "authMode": settings.ui_auth_mode,
            }
            if settings.ui_auth_mode == "oidc":
                scope = settings.ui_oidc_config.get("scope")
                cfg.update(
                    {
                        "oidcAuthority": settings.ui_oidc_config.get("authority"),
                        "oidcClientId": settings.ui_oidc_config.get("clientId"),
                        "oidcScope": scope,
                        "oidcScopes": scope,
                        "oidcRedirectUri": settings.ui_oidc_config.get("redirectUri") or "/oauth2-callback",
                    }
                )
            content = f"window.__BACKTEST_UI_CONFIG__ = {json.dumps(cfg)};"
            return Response(content=content, media_type="application/javascript", headers={"Cache-Control": "no-store"})

        @app.get("/{rest_of_path:path}")
        async def serve_index(rest_of_path: str):
            if rest_of_path.startswith(api_prefix.lstrip("/") + "/"):
                raise HTTPException(status_code=404, detail="Not found.")
            file_path = dist_path / rest_of_path
            if rest_of_path and file_path.exists() and file_path.is_file():
                return FileResponse(file_path)
            return FileResponse(dist_path / "index.html", headers={"Cache-Control": "no-store"})
    else:
        logger.warning("BACKTEST_UI_DIST_DIR not set or invalid. UI will not be served.")

    @app.websocket(f"{api_prefix}/ws/updates")
    async def websocket_endpoint(websocket: WebSocket):
        await realtime_manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            realtime_manager.disconnect(websocket)

    return app


app: FastAPI = create_app()
