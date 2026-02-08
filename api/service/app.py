import json
import logging
import os
import time
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html, get_swagger_ui_oauth2_redirect_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

from api.endpoints import alpha_vantage, data, massive, postgres, strategies, system
from api.service.auth import AuthManager
from api.service.alpha_vantage_gateway import AlphaVantageGateway
from api.service.massive_gateway import MassiveGateway
from api.service.settings import ServiceSettings
from api.service.realtime import manager as realtime_manager
from api.service.alert_state_store import PostgresAlertStateStore
from monitoring.ttl_cache import TtlCache

logger = logging.getLogger("asset-allocation.api")

def _is_truthy(raw: str | None) -> bool:
    return (raw or "").strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def _is_test_environment() -> bool:
    return "PYTEST_CURRENT_TEST" in os.environ or _is_truthy(os.environ.get("TEST_MODE"))

def _normalize_root_prefix(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw or raw == "/":
        return ""
    return "/" + raw.strip("/")

def _parse_env_list(value: str | None) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []

    # Accept either JSON array syntax or a comma-separated list.
    if raw.startswith("["):
        try:
            decoded = json.loads(raw)
        except Exception:
            decoded = None
        else:
            if isinstance(decoded, list):
                return [str(item).strip() for item in decoded if str(item).strip()]

    return [item.strip() for item in raw.split(",") if item.strip()]


def _get_cors_allow_origins() -> list[str]:
    configured = _parse_env_list(os.environ.get("API_CORS_ALLOW_ORIGINS"))
    if configured:
        origins = configured
    else:
        origins = [
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:5174",
            "http://127.0.0.1:5174",
            "http://localhost:3000",
        ]

    # CORSMiddleware does not allow credentials with wildcard origins.
    if "*" in origins:
        logger.warning(
            "API_CORS_ALLOW_ORIGINS contains '*', which is incompatible with allow_credentials=true. "
            "Dropping '*' and keeping explicit origins only."
        )
        origins = [origin for origin in origins if origin != "*"]

    # De-dup while preserving order.
    return list(dict.fromkeys(origins))

def create_app() -> FastAPI:
    # ... (existing inner functions) ...

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = ServiceSettings.from_env()

        app.state.settings = settings
        app.state.auth = AuthManager(settings)
        app.state.alpha_vantage_gateway = AlphaVantageGateway()
        app.state.massive_gateway = MassiveGateway()

        stop_refresh = asyncio.Event()
        refresh_task: asyncio.Task[None] | None = None

        if settings.postgres_dsn:
            app.state.alert_state_store = PostgresAlertStateStore(settings.postgres_dsn)
            try:
                from core.config import reload_settings
                from core.debug_symbols import refresh_debug_symbols_from_db
                from core.runtime_config import DEFAULT_ENV_OVERRIDE_KEYS, apply_runtime_config_to_env

                if not _is_test_environment():
                    baseline_env: dict[str, str | None] = {
                        key: os.environ.get(key) for key in sorted(DEFAULT_ENV_OVERRIDE_KEYS)
                    }
                    app.state.runtime_config_baseline = baseline_env

                    def _apply_and_reconcile() -> dict[str, str]:
                        applied = apply_runtime_config_to_env(
                            dsn=settings.postgres_dsn,
                            scopes_by_precedence=["global"],
                            raise_on_error=True,
                        )

                        # If a key is no longer overridden (deleted/disabled), revert to its baseline value.
                        for key in DEFAULT_ENV_OVERRIDE_KEYS:
                            if key in applied:
                                continue
                            baseline = baseline_env.get(key)
                            if baseline is None:
                                os.environ.pop(key, None)
                            else:
                                os.environ[key] = baseline

                        reload_settings()
                        debug_symbols = refresh_debug_symbols_from_db(dsn=settings.postgres_dsn)
                        app.state.runtime_config_applied = applied

                        try:
                            import hashlib

                            digest = hashlib.sha256(
                                json.dumps(applied, sort_keys=True, separators=(",", ":")).encode(
                                    "utf-8"
                                )
                            ).hexdigest()
                            if getattr(app.state, "runtime_config_hash", None) != digest:
                                app.state.runtime_config_hash = digest
                                logger.info(
                                    "Runtime config refreshed: keys=%s hash=%s",
                                    sorted(applied.keys()),
                                    digest[:12],
                                )
                        except Exception:
                            pass

                        try:
                            import hashlib

                            digest = hashlib.sha256(
                                json.dumps(list(debug_symbols), separators=(",", ":")).encode("utf-8")
                            ).hexdigest()
                            if getattr(app.state, "debug_symbols_hash", None) != digest:
                                app.state.debug_symbols_hash = digest
                                logger.info(
                                    "Debug symbols refreshed: count=%s hash=%s",
                                    len(debug_symbols),
                                    digest[:12],
                                )
                        except Exception:
                            pass
                        return applied

                    _apply_and_reconcile()

                    def _get_refresh_seconds() -> float:
                        raw = os.environ.get("RUNTIME_CONFIG_REFRESH_SECONDS", "60")
                        try:
                            seconds = float(str(raw).strip() or "60")
                        except Exception:
                            return 60.0
                        return seconds if seconds >= 5 else 5.0

                    async def _periodic_refresh() -> None:
                        while not stop_refresh.is_set():
                            try:
                                await asyncio.wait_for(stop_refresh.wait(), timeout=_get_refresh_seconds())
                                break
                            except asyncio.TimeoutError:
                                pass

                            try:
                                _apply_and_reconcile()
                                # Update TTL cache if config changed.
                                ttl = _system_health_ttl_seconds()
                                cache = getattr(app.state, "system_health_cache", None)
                                if cache is not None and getattr(cache, "ttl_seconds", None) is not None:
                                    if abs(float(cache.ttl_seconds) - ttl) > 1e-9:
                                        cache.set_ttl_seconds(ttl)
                            except Exception as exc:
                                logger.warning("Periodic runtime config refresh failed: %s", exc)

                    refresh_task = asyncio.create_task(_periodic_refresh())
            except Exception as exc:
                logger.warning("Runtime config overrides not applied: %s", exc)

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

        stop_refresh.set()
        if refresh_task is not None:
            refresh_task.cancel()
            try:
                await refresh_task
            except Exception:
                pass

        try:
            app.state.alpha_vantage_gateway.close()
        except Exception:
            pass

        try:
            app.state.massive_gateway.close()
        except Exception:
            pass

    app = FastAPI(
        title="Asset Allocation API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    logger.info("Service application starting")

    content_security_policy = (os.environ.get("API_CSP") or "").strip()

    @app.middleware("http")
    async def _http_middleware(request: Request, call_next):
        try:
            start = time.monotonic()
            path = request.url.path or ""

            response = await call_next(request)
            _ = (time.monotonic() - start) * 1000.0

            # Safe logic for headers
            if path.startswith("/assets/") and response.status_code == 200:
                response.headers.setdefault("Cache-Control", "public, max-age=31536000, immutable")

            response.headers.setdefault("X-Content-Type-Options", "nosniff")
            response.headers.setdefault("X-Frame-Options", "DENY")
            if content_security_policy:
                response.headers.setdefault("Content-Security-Policy", content_security_policy)
            
            return response
            
        except Exception:
            # Let Starlette's ServerErrorMiddleware handle it, but log it first if needed
            raise

    # CORS Configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_allow_origins(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    api_root_prefix = _normalize_root_prefix(os.environ.get("API_ROOT_PREFIX"))
    api_prefixes = ["/api"]
    if api_root_prefix:
        api_prefixes.append(f"{api_root_prefix}/api")

    def _get_openapi_schema() -> dict:
        if app.openapi_schema is None:
            app.openapi_schema = get_openapi(
                title=app.title,
                version=app.version,
                routes=app.routes,
            )
        return app.openapi_schema

    def _register_docs_routes(api_prefix: str) -> None:
        docs_path = f"{api_prefix}/docs"
        openapi_path = f"{api_prefix}/openapi.json"
        oauth2_redirect_path = f"{docs_path}/oauth2-redirect"

        async def openapi_json() -> JSONResponse:
            return JSONResponse(_get_openapi_schema())

        async def swagger_ui() -> Response:
            return get_swagger_ui_html(
                openapi_url=openapi_path,
                title=f"{app.title} - Swagger UI",
                oauth2_redirect_url=oauth2_redirect_path,
            )

        async def swagger_ui_redirect() -> Response:
            return get_swagger_ui_oauth2_redirect_html()

        app.add_api_route(
            openapi_path,
            openapi_json,
            methods=["GET"],
            include_in_schema=False,
            name=f"openapi:{api_prefix}",
        )
        app.add_api_route(
            docs_path,
            swagger_ui,
            methods=["GET"],
            include_in_schema=False,
            name=f"swagger:{api_prefix}",
        )
        app.add_api_route(
            oauth2_redirect_path,
            swagger_ui_redirect,
            methods=["GET"],
            include_in_schema=False,
            name=f"swagger-oauth2:{api_prefix}",
        )

    for api_prefix in api_prefixes:
        _register_docs_routes(api_prefix)

    primary_api_prefix = f"{api_root_prefix}/api" if api_root_prefix else "/api"

    @app.get("/docs", include_in_schema=False)
    def docs_redirect() -> RedirectResponse:
        return RedirectResponse(
            url=f"{primary_api_prefix}/docs",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    @app.get("/openapi.json", include_in_schema=False)
    def openapi_redirect() -> RedirectResponse:
        return RedirectResponse(
            url=f"{primary_api_prefix}/openapi.json",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    for api_prefix in api_prefixes:
        app.include_router(data.router, prefix=f"{api_prefix}/data", tags=["Data"])
        app.include_router(system.router, prefix=f"{api_prefix}/system", tags=["System"])
        app.include_router(postgres.router, prefix=f"{api_prefix}/system/postgres", tags=["Postgres"])
        app.include_router(strategies.router, prefix=f"{api_prefix}/strategies", tags=["Strategies"])
        app.include_router(
            alpha_vantage.router,
            prefix=f"{api_prefix}/providers/alpha-vantage",
            tags=["AlphaVantage"],
        )
        app.include_router(
            massive.router,
            prefix=f"{api_prefix}/providers/massive",
            tags=["Massive"],
        )

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

    for api_prefix in api_prefixes:
        app.add_api_websocket_route(f"{api_prefix}/ws/updates", websocket_endpoint)

    @app.get("/healthz")
    def healthz() -> JSONResponse:
        return JSONResponse({"status": "ok"})

    @app.get("/readyz")
    def readyz(request: Request) -> JSONResponse:
        return JSONResponse({"status": "ready"})

    @app.get("/config.js")
    async def get_ui_config(request: Request):
        settings: ServiceSettings = app.state.settings
        default_api_base = f"{api_root_prefix}/api" if api_root_prefix else "/api"
        api_base_url = settings.ui_oidc_config.get("apiBaseUrl") or default_api_base

        cfg = {
            "authMode": settings.ui_auth_mode,
            "apiBaseUrl": api_base_url,
            # Backwards-compatible alias used by the UI runtime config loader.
            "backtestApiBaseUrl": api_base_url,
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
        content = "\n".join(
            [
                f"window.__BACKTEST_UI_CONFIG__ = {json.dumps(cfg)};",
                f"window.__API_UI_CONFIG__ = {json.dumps(cfg)};",
            ]
        )
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
