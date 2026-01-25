import logging
from typing import Any, Dict
from fastapi import Request
from monitoring.ttl_cache import TtlCache

from api.service.auth import AuthManager
from api.service.job_manager import JobManager
from api.service.run_store import RunStore
from api.service.settings import ServiceSettings

logger = logging.getLogger("backtest.api.auth")

def get_settings(request: Request) -> ServiceSettings:
    return request.app.state.settings


def get_store(request: Request) -> RunStore:
    return request.app.state.store


def get_manager(request: Request) -> JobManager:
    return request.app.state.manager


def get_auth_manager(request: Request) -> AuthManager:
    return request.app.state.auth


def get_system_health_cache(request: Request) -> TtlCache[Dict[str, Any]]:
    return request.app.state.system_health_cache


def get_alert_state_store(request: Request):
    return getattr(request.app.state, "alert_state_store", None)


from fastapi import HTTPException
from api.service.auth import AuthError


def validate_auth(request: Request) -> None:
    settings = get_settings(request)
    auth = get_auth_manager(request)
    
    if settings.auth_mode == "none":
        logger.info(
            "Auth skipped (mode=none): path=%s host=%s",
            request.url.path,
            request.headers.get("host", ""),
        )
        return
        
    try:
        ctx = auth.authenticate_headers(dict(request.headers))
        logger.info(
            "Auth ok: mode=%s subject=%s path=%s",
            ctx.mode,
            ctx.subject or "-",
            request.url.path,
        )
    except AuthError as exc:
        headers: Dict[str, str] = {}
        if exc.www_authenticate:
            headers["WWW-Authenticate"] = exc.www_authenticate
        logger.warning(
            "Auth failed: status=%s detail=%s path=%s",
            exc.status_code,
            exc.detail,
            request.url.path,
        )
        raise HTTPException(status_code=exc.status_code, detail=exc.detail, headers=headers) from exc

