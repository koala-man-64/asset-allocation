from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from core.config import parse_debug_symbols
from core.postgres import PostgresError, connect

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DebugSymbolsState:
    enabled: bool
    symbols_raw: str
    symbols: list[str]
    updated_at: Optional[datetime]
    updated_by: Optional[str]


def _normalize_symbols_text(value: object) -> str:
    symbols = parse_debug_symbols(value)
    return ",".join(symbols)


def _resolve_dsn(dsn: Optional[str]) -> Optional[str]:
    raw = dsn or os.environ.get("POSTGRES_DSN")
    value = (raw or "").strip()
    return value or None


def read_debug_symbols_state(dsn: Optional[str] = None) -> DebugSymbolsState:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    with connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT enabled, symbols, updated_at, updated_by FROM core.debug_symbols WHERE id=1;"
            )
            row = cur.fetchone()

    if not row:
        return DebugSymbolsState(
            enabled=False,
            symbols_raw="",
            symbols=[],
            updated_at=None,
            updated_by=None,
        )

    enabled = bool(row[0])
    symbols_raw = str(row[1] or "")
    symbols = parse_debug_symbols(symbols_raw)
    return DebugSymbolsState(
        enabled=enabled,
        symbols_raw=symbols_raw,
        symbols=symbols,
        updated_at=row[2],
        updated_by=row[3],
    )


def update_debug_symbols_state(
    *,
    dsn: Optional[str],
    enabled: bool,
    symbols: object,
    actor: Optional[str] = None,
) -> DebugSymbolsState:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        raise PostgresError("POSTGRES_DSN is not configured.")

    normalized = _normalize_symbols_text(symbols)

    with connect(resolved) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.debug_symbols(id, enabled, symbols, updated_at, updated_by)
                VALUES (1, %s, %s, now(), %s)
                ON CONFLICT (id) DO UPDATE
                SET enabled = EXCLUDED.enabled,
                    symbols = EXCLUDED.symbols,
                    updated_at=now(),
                    updated_by=EXCLUDED.updated_by;
                """,
                (bool(enabled), normalized, actor),
            )

    return read_debug_symbols_state(resolved)


def refresh_debug_symbols_from_db(dsn: Optional[str] = None) -> list[str]:
    resolved = _resolve_dsn(dsn)
    if not resolved:
        logger.warning("POSTGRES_DSN not set; using DEBUG_SYMBOLS from environment.")
        return _apply_debug_symbols_from_env()

    try:
        state = read_debug_symbols_state(resolved)
    except Exception as exc:
        logger.warning("Failed to load debug symbols from Postgres; using env fallback. (%s)", exc)
        return _apply_debug_symbols_from_env()

    symbols = state.symbols if state.enabled else []
    _apply_debug_symbols_to_config(symbols)
    return symbols


def _apply_debug_symbols_from_env() -> list[str]:
    env_value = os.environ.get("DEBUG_SYMBOLS")
    symbols = parse_debug_symbols(env_value or "")
    _apply_debug_symbols_to_config(symbols)
    return symbols


def _apply_debug_symbols_to_config(symbols: list[str]) -> None:
    try:
        from core import config as cfg

        cfg.settings.DEBUG_SYMBOLS = list(symbols)
        cfg.DEBUG_SYMBOLS = list(symbols)
    except Exception as exc:
        logger.warning("Failed to update runtime DEBUG_SYMBOLS config: %s", exc)
