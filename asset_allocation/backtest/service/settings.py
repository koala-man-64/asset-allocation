from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


def _parse_bool(value: str) -> bool:
    text = value.strip().lower()
    if text in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def _get_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return _parse_bool(raw)


def _get_int(name: str, default: int, *, min_value: int = 1, max_value: int = 256) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid int for {name}={raw!r}") from exc
    if parsed < min_value or parsed > max_value:
        raise ValueError(f"{name} must be in [{min_value}, {max_value}] (got {parsed}).")
    return parsed


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _get_path_list(name: str) -> List[Path]:
    raw = os.environ.get(name, "").strip()
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
    explicit = os.environ.get("BACKTEST_ADLS_CONTAINER_ALLOWLIST", "").strip()
    if explicit:
        return _split_csv(explicit)

    defaults = [
        "bronze",
        "silver",
        "gold",
        "platinum",
        "ranking-data",
        "common",
    ]
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
        value = os.environ.get(name, "").strip()
        if value:
            defaults.append(value)

    seen = set()
    ordered = []
    for item in defaults:
        if item and item not in seen:
            ordered.append(item)
            seen.add(item)
    return ordered


@dataclass(frozen=True)
class ServiceSettings:
    output_base_dir: Path
    db_path: Path
    max_concurrent_runs: int
    api_key: Optional[str]
    api_key_header: str
    allow_local_data: bool
    allowed_local_data_dirs: List[Path]
    adls_container_allowlist: List[str]

    @staticmethod
    def from_env() -> "ServiceSettings":
        output_base_dir = Path(os.environ.get("BACKTEST_OUTPUT_DIR", "./backtest_results")).expanduser()
        if not output_base_dir.is_absolute():
            output_base_dir = (Path.cwd() / output_base_dir).resolve(strict=False)
        else:
            output_base_dir = output_base_dir.resolve(strict=False)
        output_base_dir.mkdir(parents=True, exist_ok=True)

        db_path_raw = os.environ.get("BACKTEST_DB_PATH", "").strip()
        if db_path_raw:
            db_path = Path(db_path_raw).expanduser()
            if not db_path.is_absolute():
                db_path = (Path.cwd() / db_path).resolve(strict=False)
            else:
                db_path = db_path.resolve(strict=False)
        else:
            db_path = output_base_dir / "runs.sqlite3"

        max_concurrent_runs = _get_int("BACKTEST_MAX_CONCURRENT", 1, min_value=1, max_value=64)
        api_key = os.environ.get("BACKTEST_API_KEY") or None
        api_key_header = os.environ.get("BACKTEST_API_KEY_HEADER", "X-API-Key").strip() or "X-API-Key"

        allow_local_data = _get_bool("BACKTEST_ALLOW_LOCAL_DATA", False)
        allowed_local_data_dirs = _get_path_list("BACKTEST_ALLOWED_DATA_DIRS")
        if allow_local_data and not allowed_local_data_dirs:
            raise ValueError(
                "BACKTEST_ALLOW_LOCAL_DATA=true requires BACKTEST_ALLOWED_DATA_DIRS to be set (comma-separated)."
            )

        adls_container_allowlist = _get_container_allowlist()

        return ServiceSettings(
            output_base_dir=output_base_dir,
            db_path=db_path,
            max_concurrent_runs=max_concurrent_runs,
            api_key=api_key.strip() if isinstance(api_key, str) and api_key.strip() else None,
            api_key_header=api_key_header,
            allow_local_data=allow_local_data,
            allowed_local_data_dirs=allowed_local_data_dirs,
            adls_container_allowlist=adls_container_allowlist,
        )

