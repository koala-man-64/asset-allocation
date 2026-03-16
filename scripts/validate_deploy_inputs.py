from __future__ import annotations

import os
from urllib.parse import urlparse


REQUIRED_ENV_NAMES = (
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "AZURE_SUBSCRIPTION_ID",
    "AZURE_STORAGE_CONNECTION_STRING",
    "ALPHA_VANTAGE_API_KEY",
    "NASDAQ_API_KEY",
    "POSTGRES_DSN",
    "SERVICE_ACCOUNT_NAME",
    "ASSET_ALLOCATION_API_BASE_URL",
)

AUTH_MODE_ALIASES = {
    "none": "none",
    "noauth": "none",
    "disabled": "none",
    "api_key": "api_key",
    "apikey": "api_key",
    "key": "api_key",
    "oidc": "oidc",
    "jwt": "oidc",
    "bearer": "oidc",
    "api_key_or_oidc": "api_key_or_oidc",
    "apikey_or_oidc": "api_key_or_oidc",
    "key_or_oidc": "api_key_or_oidc",
}


def fail(message: str) -> None:
    print(f"::error::{message}")
    raise SystemExit(1)


def require_value(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        fail(f"Missing required GitHub Actions value: {name}")
    return value


def parse_bool(name: str, *, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    fail(f"{name} must be a boolean (true/false)")
    raise AssertionError("unreachable")


def parse_auth_mode() -> str:
    raw = (os.environ.get("API_AUTH_MODE") or "").strip().lower() or "none"
    resolved = AUTH_MODE_ALIASES.get(raw)
    if resolved is None:
        fail("API_AUTH_MODE must be one of: none|api_key|oidc|api_key_or_oidc")
    return resolved


def parse_ui_auth_mode() -> str:
    raw = (os.environ.get("UI_AUTH_MODE") or "").strip().lower()
    if not raw:
        return "none"
    if raw in {"none", "noauth", "disabled"}:
        return "none"
    if raw in {"api_key", "apikey", "key"}:
        return "api_key"
    if raw in {"oidc", "jwt", "bearer"}:
        return "oidc"
    fail("UI_AUTH_MODE must be one of: none|api_key|oidc")
    raise AssertionError("unreachable")


def parse_postgres_url(name: str) -> tuple[str, int, str]:
    value = require_value(name)
    parsed = urlparse(value)
    if parsed.scheme not in {"postgresql", "postgres"}:
        fail(f"{name} must be a postgresql:// URL")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        fail(f"{name} is missing host")
    port = parsed.port or 5432
    database = (parsed.path or "").lstrip("/").strip()
    if not database:
        fail(f"{name} is missing database name")
    return host, int(port), database


def validate_api_base_url() -> None:
    parsed = urlparse(require_value("ASSET_ALLOCATION_API_BASE_URL"))
    host = (parsed.hostname or "").strip().lower()
    if parsed.scheme not in {"http", "https"} or not host:
        fail(
            "ASSET_ALLOCATION_API_BASE_URL must be an http(s) URL "
            "(e.g., http://asset-allocation-api)"
        )
    if host in {"localhost", "127.0.0.1", "::1"}:
        fail(
            "ASSET_ALLOCATION_API_BASE_URL must not point to localhost in production. "
            "For Azure Container Apps Jobs, use http://asset-allocation-api (no port) or the API app FQDN."
        )
    if parsed.port == 8000:
        fail(
            "ASSET_ALLOCATION_API_BASE_URL must not include :8000 in production. "
            "Container Apps ingress listens on 80/443; use http://asset-allocation-api "
            "(no port) or the API app FQDN."
        )


def validate_log_level() -> None:
    log_level = (os.environ.get("LOG_LEVEL") or "").strip().upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        fail("LOG_LEVEL must be one of: DEBUG|INFO|WARNING|ERROR|CRITICAL")


def validate_log_analytics() -> None:
    if parse_bool("SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED", default=False):
        workspace_id = (os.environ.get("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID") or "").strip()
        if not workspace_id:
            fail(
                "SYSTEM_HEALTH_LOG_ANALYTICS_ENABLED=true requires "
                "SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID."
            )
    parse_bool("REALTIME_LOG_STREAM_ENABLED", default=True)


def validate_auth_modes(auth_mode: str) -> None:
    if auth_mode == "api_key":
        require_value("API_KEY")

    if auth_mode in {"oidc", "api_key_or_oidc"}:
        require_value("API_OIDC_ISSUER")
        require_value("API_OIDC_AUDIENCE")


def validate_external_ingress(auth_mode: str, ui_auth_mode: str) -> None:
    if not parse_bool("INGRESS_EXTERNAL", default=False):
        return

    if auth_mode not in {"oidc", "api_key_or_oidc"}:
        fail(
            "INGRESS_EXTERNAL=true requires API_AUTH_MODE to be oidc or api_key_or_oidc."
        )

    if ui_auth_mode != "oidc":
        fail("INGRESS_EXTERNAL=true requires UI_AUTH_MODE=oidc.")

    require_value("API_OIDC_ISSUER")
    require_value("API_OIDC_AUDIENCE")
    require_value("UI_OIDC_CLIENT_ID")
    require_value("UI_OIDC_AUTHORITY")
    require_value("UI_OIDC_SCOPES")
    require_value("UI_OIDC_REDIRECT_URI")


def main() -> int:
    for name in REQUIRED_ENV_NAMES:
        require_value(name)

    parse_postgres_url("POSTGRES_DSN")
    validate_api_base_url()

    auth_mode = parse_auth_mode()
    ui_auth_mode = parse_ui_auth_mode()

    validate_log_level()
    validate_log_analytics()
    validate_auth_modes(auth_mode)
    validate_external_ingress(auth_mode, ui_auth_mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
