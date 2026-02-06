from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Literal, Optional

AuthMode = Literal["none", "api_key", "oidc", "api_key_or_oidc"]
UiAuthMode = Literal["none", "api_key", "oidc"]


def _parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")

def _require_env(name: str) -> str:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        raise ValueError(f"{name} is required.")
    return raw.strip()


def _split_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _get_optional_str(name: str) -> Optional[str]:
    raw = os.environ.get(name)
    value = raw.strip() if raw else ""
    return value or None


def _parse_auth_mode(value: str) -> AuthMode:
    raw = (value or "").strip().lower()
    if raw in {"none", "noauth", "disabled"}:
        return "none"
    if raw in {"api_key", "apikey", "key"}:
        return "api_key"
    if raw in {"oidc", "jwt", "bearer"}:
        return "oidc"
    if raw in {"api_key_or_oidc", "apikey_or_oidc", "key_or_oidc"}:
        return "api_key_or_oidc"
    raise ValueError(f"Invalid API_AUTH_MODE={value!r} (expected none|api_key|oidc|api_key_or_oidc).")


def _parse_ui_auth_mode(value: Optional[str]) -> UiAuthMode:
    raw = (value or "").strip().lower()
    if raw in {"", "none", "noauth", "disabled"}:
        return "none"
    if raw in {"api_key", "apikey", "key"}:
        return "api_key"
    if raw in {"oidc", "jwt", "bearer"}:
        return "oidc"
    raise ValueError(f"Invalid UI_AUTH_MODE={value!r} (expected none|api_key|oidc).")


@dataclass(frozen=True)
class ServiceSettings:
    api_key: Optional[str]
    api_key_header: str
    auth_mode: AuthMode
    oidc_issuer: Optional[str]
    oidc_audience: List[str]
    oidc_jwks_url: Optional[str]
    oidc_required_scopes: List[str]
    oidc_required_roles: List[str]
    postgres_dsn: Optional[str]
    ui_auth_mode: UiAuthMode
    ui_oidc_config: dict[str, Optional[str]]

    @staticmethod
    def from_env() -> "ServiceSettings":
        api_key = os.environ.get("API_KEY") or None
        api_key_header = os.environ.get("API_KEY_HEADER", "X-API-Key").strip()
        
        oidc_issuer = _get_optional_str("API_OIDC_ISSUER")
        oidc_audience = _split_csv(_get_optional_str("API_OIDC_AUDIENCE"))
        oidc_jwks_url = _get_optional_str("API_OIDC_JWKS_URL")
        oidc_required_scopes = _split_csv(_get_optional_str("API_OIDC_REQUIRED_SCOPES"))
        oidc_required_roles = _split_csv(_get_optional_str("API_OIDC_REQUIRED_ROLES"))

        auth_mode_env = os.environ.get("API_AUTH_MODE")
        if not auth_mode_env:
            # Fallback for dev if not set
            auth_mode = "none"
        else:
            auth_mode = _parse_auth_mode(auth_mode_env)

        if auth_mode in {"api_key", "api_key_or_oidc"} and not (api_key and str(api_key).strip()):
            if auth_mode == "api_key":
                raise ValueError("API_AUTH_MODE=api_key requires API_KEY to be set.")
            api_key = None

        if auth_mode in {"oidc", "api_key_or_oidc"}:
            if not oidc_issuer:
                raise ValueError(f"API_AUTH_MODE={auth_mode} requires API_OIDC_ISSUER to be set.")
            if not oidc_audience:
                raise ValueError(f"API_AUTH_MODE={auth_mode} requires API_OIDC_AUDIENCE to be set (csv).")

        postgres_dsn = _get_optional_str("POSTGRES_DSN")
        
        ui_auth_mode_raw = _get_optional_str("UI_AUTH_MODE")
        if ui_auth_mode_raw is None:
            if auth_mode in {"oidc", "api_key_or_oidc"}:
                ui_auth_mode = "oidc"
            elif auth_mode == "api_key":
                ui_auth_mode = "api_key"
            else:
                ui_auth_mode = "none"
        else:
            ui_auth_mode = _parse_ui_auth_mode(ui_auth_mode_raw)
        
        ui_authority = _get_optional_str("UI_OIDC_AUTHORITY") or _get_optional_str("API_OIDC_ISSUER")

        ui_oidc_config = {
            "authority": ui_authority,
            "clientId": _get_optional_str("UI_OIDC_CLIENT_ID"),
            "scope": _get_optional_str("UI_OIDC_SCOPES"),
            "redirectUri": _get_optional_str("UI_OIDC_REDIRECT_URI"),
            "apiBaseUrl": _get_optional_str("UI_API_BASE_URL"),
        }

        return ServiceSettings(
            api_key=api_key.strip() if isinstance(api_key, str) and api_key.strip() else None,
            api_key_header=api_key_header,
            auth_mode=auth_mode,
            oidc_issuer=oidc_issuer,
            oidc_audience=oidc_audience,
            oidc_jwks_url=oidc_jwks_url,
            oidc_required_scopes=oidc_required_scopes,
            oidc_required_roles=oidc_required_roles,
            postgres_dsn=postgres_dsn,
            ui_auth_mode=ui_auth_mode,
            ui_oidc_config=ui_oidc_config,
        )
