from __future__ import annotations

import os
from types import ModuleType
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def build_router(
    *,
    runtime: ModuleType,
    symbol_sync_state_response_model: Any,
    system_status_view_response_model: Any,
) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/health")
    def system_health(request: Request, refresh: bool = Query(False)) -> JSONResponse:
        logger = _runtime_attr(runtime, "logger")
        validate_auth = _runtime_attr(runtime, "validate_auth")
        resolve_system_health_payload = _runtime_attr(runtime, "_resolve_system_health_payload")

        request_id = request.headers.get("x-request-id", "")
        logger.info(
            "System health request: refresh=%s path=%s host=%s fwd=%s request_id=%s",
            refresh,
            request.url.path,
            request.headers.get("host", ""),
            request.headers.get("x-forwarded-for", ""),
            request_id,
        )
        validate_auth(request)
        payload, cache_hit, refresh_error = resolve_system_health_payload(request, refresh=bool(refresh))

        headers: dict[str, str] = {
            "Cache-Control": "no-store",
            "X-System-Health-Cache": "hit" if cache_hit else "miss",
        }
        if refresh_error:
            headers["X-System-Health-Cache-Degraded"] = "1"
        return JSONResponse(payload, headers=headers)

    @router.get("/symbol-sync-state", response_model=symbol_sync_state_response_model)
    def get_symbol_sync_state_endpoint(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_settings = _runtime_attr(runtime, "get_settings")
        get_symbol_sync_state = _runtime_attr(runtime, "get_symbol_sync_state")
        iso = _runtime_attr(runtime, "_iso")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        settings = get_settings(request)
        dsn = (settings.postgres_dsn or os.environ.get("POSTGRES_DSN") or "").strip()
        if not dsn:
            raise HTTPException(status_code=503, detail="Postgres is not configured (POSTGRES_DSN).")

        try:
            state = get_symbol_sync_state(dsn)
        except Exception as exc:
            logger.exception("Failed to load symbol sync state.")
            raise HTTPException(status_code=500, detail=f"Failed to load symbol sync state: {exc}") from exc

        if not state:
            return JSONResponse(
                {
                    "id": 1,
                    "last_refreshed_at": None,
                    "last_refreshed_sources": None,
                    "last_refresh_error": None,
                },
                headers={"Cache-Control": "no-store"},
            )

        return JSONResponse(
            {
                "id": state["id"],
                "last_refreshed_at": iso(state["last_refreshed_at"]),
                "last_refreshed_sources": state["last_refreshed_sources"],
                "last_refresh_error": state["last_refresh_error"],
            },
            headers={"Cache-Control": "no-store"},
        )

    @router.get("/status-view", response_model=system_status_view_response_model)
    def system_status_view(request: Request, refresh: bool = Query(False)) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        build_system_status_view = _runtime_attr(runtime, "build_system_status_view")

        validate_auth(request)
        payload = build_system_status_view(request, refresh=bool(refresh))
        return JSONResponse(
            payload,
            headers={
                "Cache-Control": "no-store",
                "X-System-Health-Cache": "hit"
                if payload.get("sources", {}).get("systemHealth") == "cache"
                else "miss",
                "X-Domain-Metadata-Source": "persisted-snapshot",
            },
        )

    @router.get("/lineage")
    def system_lineage(request: Request) -> JSONResponse:
        validate_auth = _runtime_attr(runtime, "validate_auth")
        get_lineage_snapshot = _runtime_attr(runtime, "get_lineage_snapshot")
        logger = _runtime_attr(runtime, "logger")

        validate_auth(request)
        payload = get_lineage_snapshot()
        logger.info(
            "System lineage generated: layers=%s strategies=%s domains=%s",
            len(payload.get("layers") or []),
            len(payload.get("strategies") or []),
            len((payload.get("impactsByDomain") or {}).keys()),
        )
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})

    return router, {
        "system_health": system_health,
        "get_symbol_sync_state_endpoint": get_symbol_sync_state_endpoint,
        "system_status_view": system_status_view,
        "system_lineage": system_lineage,
    }
