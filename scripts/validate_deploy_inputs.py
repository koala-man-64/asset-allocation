from __future__ import annotations

import json
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


def fail(message: str) -> None:
    print(f"::error::{message}")
    raise SystemExit(1)


def require_value(name: str) -> str:
    value = (os.environ.get(name) or "").strip()
    if not value:
        fail(f"Missing required GitHub Actions value: {name}")
    return value


def optional_value(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def parse_float(name: str, *, default: float, min_value: float = 0.0, max_value: float = 86400.0) -> float:
    raw = optional_value(name)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        fail(f"{name} must be a number.")
    if not (min_value <= value <= max_value):
        fail(f"{name} must be between {min_value} and {max_value}.")
    return value


def parse_int(name: str, *, default: int, min_value: int = 0, max_value: int = 86400) -> int:
    raw = optional_value(name)
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        fail(f"{name} must be an integer.")
    if not (min_value <= value <= max_value):
        fail(f"{name} must be between {min_value} and {max_value}.")
    return value


def parse_json_array(name: str) -> list[object]:
    raw = optional_value(name)
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        fail(f"{name} must be valid JSON.")
    if not isinstance(payload, list):
        fail(f"{name} must be a JSON array.")
    return payload


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
        fail("ASSET_ALLOCATION_API_BASE_URL must be an http(s) URL (e.g., http://asset-allocation-api)")
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
    log_level = optional_value("LOG_LEVEL").upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        fail("LOG_LEVEL must be one of: DEBUG|INFO|WARNING|ERROR|CRITICAL")


def validate_log_analytics() -> None:
    workspace_id = optional_value("SYSTEM_HEALTH_LOG_ANALYTICS_WORKSPACE_ID")
    parse_float("SYSTEM_HEALTH_ARM_TIMEOUT_SECONDS", default=5.0, min_value=0.5, max_value=30.0)
    parse_int("SYSTEM_HEALTH_JOB_EXECUTIONS_PER_JOB", default=3, min_value=1, max_value=25)
    parse_float("SYSTEM_HEALTH_LOG_ANALYTICS_TIMEOUT_SECONDS", default=5.0, min_value=0.1, max_value=300.0)
    parse_int("SYSTEM_HEALTH_LOG_ANALYTICS_TIMESPAN_MINUTES", default=15, min_value=1, max_value=1440)
    parse_json_array("SYSTEM_HEALTH_LOG_ANALYTICS_QUERIES_JSON")

    if workspace_id:
        parse_float("REALTIME_LOG_STREAM_POLL_SECONDS", default=5.0, min_value=1.0, max_value=300.0)
        parse_int("REALTIME_LOG_STREAM_LOOKBACK_SECONDS", default=30, min_value=10, max_value=86400)
        parse_int("REALTIME_LOG_STREAM_BATCH_SIZE", default=200, min_value=10, max_value=500)


def validate_auth_configuration() -> None:
    for deprecated_name in ("API_KEY", "ASSET_ALLOCATION_API_KEY", "VITE_BACKTEST_API_BASE_URL"):
        if optional_value(deprecated_name):
            fail(
                f"{deprecated_name} is no longer supported. Remove the stale API-key/backtest compatibility setting."
            )

    api_oidc_issuer = optional_value("API_OIDC_ISSUER")
    api_oidc_audience = optional_value("API_OIDC_AUDIENCE")
    api_oidc_jwks_url = optional_value("API_OIDC_JWKS_URL")
    api_oidc_required_scopes = optional_value("API_OIDC_REQUIRED_SCOPES")
    api_oidc_required_roles = optional_value("API_OIDC_REQUIRED_ROLES")
    api_oidc_inputs_present = any(
        (
            api_oidc_issuer,
            api_oidc_audience,
            api_oidc_jwks_url,
            api_oidc_required_scopes,
            api_oidc_required_roles,
        )
    )

    if not api_oidc_inputs_present:
        fail("Production deploy requires API OIDC configuration.")
    if not api_oidc_issuer:
        fail("API_OIDC_ISSUER is required for the production deploy workflow.")
    if not api_oidc_audience:
        fail("API_OIDC_AUDIENCE is required for the production deploy workflow.")

    ui_oidc_values = {
        "UI_OIDC_CLIENT_ID": optional_value("UI_OIDC_CLIENT_ID"),
        "UI_OIDC_AUTHORITY": optional_value("UI_OIDC_AUTHORITY"),
        "UI_OIDC_SCOPES": optional_value("UI_OIDC_SCOPES"),
        "UI_OIDC_REDIRECT_URI": optional_value("UI_OIDC_REDIRECT_URI"),
    }

    if not any(ui_oidc_values.values()):
        fail(
            "Production deploy requires browser OIDC configuration for the UI. "
            "Set UI_OIDC_CLIENT_ID, UI_OIDC_AUTHORITY, UI_OIDC_SCOPES, and "
            "UI_OIDC_REDIRECT_URI. The deployed UI only supports OIDC."
        )

    missing_ui_oidc = [name for name, value in ui_oidc_values.items() if not value]
    if missing_ui_oidc:
        fail(
            "Production deploy requires complete browser OIDC configuration for the UI. "
            f"Missing: {', '.join(missing_ui_oidc)}. The deployed UI only supports OIDC."
        )

    parsed = urlparse(ui_oidc_values["UI_OIDC_REDIRECT_URI"])
    if parsed.scheme != "https" or not (parsed.hostname or "").strip():
        fail("UI_OIDC_REDIRECT_URI must be an absolute https:// URL.")

    if not optional_value("ASSET_ALLOCATION_API_SCOPE"):
        fail("ASSET_ALLOCATION_API_SCOPE is required for bronze job managed-identity callers.")


def main() -> int:
    for name in REQUIRED_ENV_NAMES:
        require_value(name)

    parse_postgres_url("POSTGRES_DSN")
    validate_api_base_url()
    validate_log_level()
    validate_log_analytics()
    validate_auth_configuration()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
