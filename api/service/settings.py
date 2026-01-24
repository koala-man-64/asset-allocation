from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional

from api.service.security import assert_allowed_container, parse_container_and_path


RunStoreMode = Literal["sqlite", "adls", "postgres"]
AuthMode = Literal["none", "api_key", "oidc", "api_key_or_oidc"]


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
    return raw


def _require_bool(name: str) -> bool:
    return _parse_bool(_require_env(name))


def _require_int(name: str, *, min_value: int = 1, max_value: int = 256) -> int:
    raw = _require_env(name)
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if parsed < min_value or parsed > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {parsed}).")
    return parsed


def _split_csv(value: Optional[str]) -> List[str]:
    return [item.strip() for item in (value or "").split(",") if item.strip()]


def _get_optional_str(name: str) -> Optional[str]:
    raw = os.environ.get(name)
    value = raw.strip() if raw else ""
    return value or None


def _get_path_list(name: str) -> List[Path]:
    raw_env = os.environ.get(name)
    raw = raw_env.strip() if raw_env else ""
    if not raw:
        return []
    paths = []
    for item in _split_csv(raw):
        path = Path(item).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve(strict=False)
        else:
            path = path.resolve(strict=False)
        paths.append(path)
    return paths


def _get_container_allowlist() -> List[str]:
    explicit = _require_env("BACKTEST_ADLS_CONTAINER_ALLOWLIST").strip()
    allowlist = _split_csv(explicit)
    env_names = [
        "AZURE_CONTAINER_MARKET",
        "AZURE_CONTAINER_FINANCE",
        "AZURE_CONTAINER_EARNINGS",
        "AZURE_CONTAINER_TARGETS",
        "AZURE_CONTAINER_COMMON",
        "AZURE_CONTAINER_RANKING",
        "AZURE_CONTAINER_BRONZE",
        "AZURE_CONTAINER_SILVER",
        "AZURE_CONTAINER_GOLD",
    ]
    for name in env_names:
        value_raw = os.environ.get(name)
        value = value_raw.strip() if value_raw else ""
        if value:
            allowlist.append(value)

    seen = set()
    ordered = []
    for item in allowlist:
        if item and item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


def _get_run_store_mode() -> RunStoreMode:
    raw = _require_env("BACKTEST_RUN_STORE_MODE").strip().lower()
    if raw in {"sqlite", "local"}:
        return "sqlite"
    if raw in {"adls", "blob"}:
        return "adls"
    if raw in {"postgres", "pg"}:
        return "postgres"
    raise ValueError(f"Invalid BACKTEST_RUN_STORE_MODE={raw!r} (expected sqlite|adls|postgres).")


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
    raise ValueError(f"Invalid BACKTEST_AUTH_MODE={value!r} (expected none|api_key|oidc|api_key_or_oidc).")


@dataclass(frozen=True)
class ServiceSettings:
    output_base_dir: Path
    db_path: Path
    max_concurrent_runs: int
    api_key: Optional[str]
    api_key_header: str
    auth_mode: AuthMode
    oidc_issuer: Optional[str]
    oidc_audience: List[str]
    oidc_jwks_url: Optional[str]
    oidc_required_scopes: List[str]
    oidc_required_roles: List[str]
    allow_local_data: bool
    allowed_local_data_dirs: List[Path]
    adls_container_allowlist: List[str]
    run_store_mode: RunStoreMode
    adls_runs_dir: Optional[str]
    postgres_dsn: Optional[str]
    ui_oidc_config: dict[str, Optional[str]]

    @staticmethod
    def from_env() -> "ServiceSettings":
        output_base_dir = Path(_require_env("BACKTEST_OUTPUT_DIR")).expanduser()
        if not output_base_dir.is_absolute():
            output_base_dir = (Path.cwd() / output_base_dir).resolve(strict=False)
        else:
            output_base_dir = output_base_dir.resolve(strict=False)
        output_base_dir.mkdir(parents=True, exist_ok=True)

        db_path_raw = _require_env("BACKTEST_DB_PATH").strip()
        db_path = Path(db_path_raw).expanduser()
        if not db_path.is_absolute():
            db_path = (Path.cwd() / db_path).resolve(strict=False)
        else:
            db_path = db_path.resolve(strict=False)

        max_concurrent_runs = _require_int("BACKTEST_MAX_CONCURRENT", min_value=1, max_value=64)
        api_key = os.environ.get("BACKTEST_API_KEY") or None
        api_key_header = _require_env("BACKTEST_API_KEY_HEADER").strip()
        if not api_key_header:
            raise ValueError("BACKTEST_API_KEY_HEADER must not be empty.")

        oidc_issuer = _get_optional_str("BACKTEST_OIDC_ISSUER")
        oidc_audience = _split_csv(_get_optional_str("BACKTEST_OIDC_AUDIENCE"))
        oidc_jwks_url = _get_optional_str("BACKTEST_OIDC_JWKS_URL")
        oidc_required_scopes = _split_csv(_get_optional_str("BACKTEST_OIDC_REQUIRED_SCOPES"))
        oidc_required_roles = _split_csv(_get_optional_str("BACKTEST_OIDC_REQUIRED_ROLES"))

        auth_mode = _parse_auth_mode(_require_env("BACKTEST_AUTH_MODE"))

        if auth_mode in {"api_key", "api_key_or_oidc"} and not (api_key and str(api_key).strip()):
            if auth_mode == "api_key":
                raise ValueError("BACKTEST_AUTH_MODE=api_key requires BACKTEST_API_KEY to be set.")
            api_key = None

        if auth_mode in {"oidc", "api_key_or_oidc"}:
            if not oidc_issuer:
                raise ValueError(f"BACKTEST_AUTH_MODE={auth_mode} requires BACKTEST_OIDC_ISSUER to be set.")
            if not oidc_audience:
                raise ValueError(f"BACKTEST_AUTH_MODE={auth_mode} requires BACKTEST_OIDC_AUDIENCE to be set (csv).")

        allow_local_data = _require_bool("BACKTEST_ALLOW_LOCAL_DATA")
        allowed_local_data_dirs = _get_path_list("BACKTEST_ALLOWED_DATA_DIRS")
        if allow_local_data and not allowed_local_data_dirs:
            raise ValueError(
                "BACKTEST_ALLOW_LOCAL_DATA=true requires BACKTEST_ALLOWED_DATA_DIRS to be set (comma-separated)."
            )

        adls_container_allowlist = _get_container_allowlist()

        run_store_mode = _get_run_store_mode()
        adls_runs_dir = _get_optional_str("BACKTEST_ADLS_RUNS_DIR")
        if run_store_mode == "adls" and not adls_runs_dir:
            raise ValueError("BACKTEST_RUN_STORE_MODE=adls requires BACKTEST_ADLS_RUNS_DIR to be set.")
        if adls_runs_dir:
            container, _ = parse_container_and_path(adls_runs_dir)
            assert_allowed_container(container, adls_container_allowlist)

        postgres_dsn = _get_optional_str("BACKTEST_POSTGRES_DSN")
        if run_store_mode == "postgres" and not postgres_dsn:
            raise ValueError("BACKTEST_RUN_STORE_MODE=postgres requires BACKTEST_POSTGRES_DSN to be set.")
        
        ui_oidc_config = {
            "authority": _get_optional_str("BACKTEST_OIDC_ISSUER"),
            "clientId": _get_optional_str("BACKTEST_UI_OIDC_CLIENT_ID"),
            "scope": _get_optional_str("BACKTEST_UI_OIDC_SCOPES"),
            "redirectUri": _get_optional_str("BACKTEST_UI_OIDC_REDIRECT_URI"),
            "apiBaseUrl": _get_optional_str("BACKTEST_UI_API_BASE_URL"),
        }

        return ServiceSettings(
            output_base_dir=output_base_dir,
            db_path=db_path,
            max_concurrent_runs=max_concurrent_runs,
            api_key=api_key.strip() if isinstance(api_key, str) and api_key.strip() else None,
            api_key_header=api_key_header,
            auth_mode=auth_mode,
            oidc_issuer=oidc_issuer,
            oidc_audience=oidc_audience,
            oidc_jwks_url=oidc_jwks_url,
            oidc_required_scopes=oidc_required_scopes,
            oidc_required_roles=oidc_required_roles,
            allow_local_data=allow_local_data,
            allowed_local_data_dirs=allowed_local_data_dirs,
            adls_container_allowlist=adls_container_allowlist,
            run_store_mode=run_store_mode,
            adls_runs_dir=adls_runs_dir,
            postgres_dsn=postgres_dsn,
            ui_oidc_config=ui_oidc_config,
        )
