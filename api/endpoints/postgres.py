import importlib.util
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, inspect, text

from api.service.dependencies import get_settings

router = APIRouter()
_HIDDEN_EXPLORER_SCHEMAS = frozenset({"information_schema", "public"})


class QueryRequest(BaseModel):
    schema_name: str
    table_name: str
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)


class TableRequest(BaseModel):
    schema_name: str
    table_name: str


def _resolve_postgres_dsn(request: Request) -> Optional[str]:
    """
    Resolves the Postgres DSN from environment variables or settings.
    Normalizes SQLAlchemy-style DSNs (postgresql+asyncpg://) and prefers an installed sync driver.
    """
    raw = os.environ.get("POSTGRES_DSN")
    # Helper to strip whitespace or return None
    def _strip_or_none(value: object) -> Optional[str]:
        if value is None:
            return None
        t = str(value).strip()
        return t or None

    def _has_module(name: str) -> bool:
        return importlib.util.find_spec(name) is not None

    def _normalize_sync_driver(value: str) -> str:
        has_psycopg = _has_module("psycopg")
        has_psycopg2 = _has_module("psycopg2")

        if value.startswith("postgresql+psycopg2://"):
            if has_psycopg2:
                return value
            if has_psycopg:
                return "postgresql+psycopg://" + value.removeprefix("postgresql+psycopg2://")
            return value

        if value.startswith("postgresql+psycopg://"):
            if has_psycopg:
                return value
            if has_psycopg2:
                return "postgresql+psycopg2://" + value.removeprefix("postgresql+psycopg://")
            return value

        if value.startswith("postgresql://"):
            if has_psycopg2:
                return value
            if has_psycopg:
                return "postgresql+psycopg://" + value.removeprefix("postgresql://")
            return value

        if value.startswith("postgres://"):
            if has_psycopg2:
                return value
            if has_psycopg:
                return "postgresql+psycopg://" + value.removeprefix("postgres://")
            return value

        return value

    dsn = _strip_or_none(raw) or _strip_or_none(get_settings(request).postgres_dsn)
    
    if not dsn:
        return None
    
    # SQLAlchemy create_engine with psycopg2 (default for postgresql://) works well for sync usage here.
    # If the app uses asyncpg elsewhere, we might need to strictly ensure we use the right driver.
    # For introspection/pandas, using the standard driver is safest.
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn.removeprefix("postgresql+asyncpg://")

    return _normalize_sync_driver(dsn)


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _validate_table_target(insp: Any, *, schema_name: str, table_name: str) -> None:
    schema_names = insp.get_schema_names()
    if schema_name not in schema_names:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found.")

    if table_name not in insp.get_table_names(schema=schema_name):
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in schema '{schema_name}'.",
        )


@router.get("/schemas")
def list_schemas(request: Request) -> List[str]:
    """
    List all available schemas in the database.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        schemas = [
            schema
            for schema in insp.get_schema_names()
            if str(schema or "").strip().lower() not in _HIDDEN_EXPLORER_SCHEMAS
        ]
        return sorted(schemas)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch schemas: {str(e)}")
    finally:
        engine.dispose()


@router.get("/schemas/{schema_name}/tables")
def list_tables(schema_name: str, request: Request) -> List[str]:
    """
    List all tables in a specific schema.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        if schema_name not in insp.get_schema_names():
            raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found.")

        tables = insp.get_table_names(schema=schema_name)
        return sorted(tables)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(e)}")
    finally:
        engine.dispose()


@router.post("/query")
def query_table(payload: QueryRequest, request: Request) -> List[Dict[str, Any]]:
    """
    Executes a safe SELECT query on a specific table.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        _validate_table_target(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )

        qualified_table = (
            f"{_quote_identifier(payload.schema_name)}.{_quote_identifier(payload.table_name)}"
        )
        query = f"SELECT * FROM {qualified_table} LIMIT {payload.limit} OFFSET {payload.offset}"

        df = pd.read_sql(query, engine)
        records = df.where(pd.notnull(df), None).to_dict(orient="records")
        return records
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
    finally:
        engine.dispose()


@router.post("/purge")
def purge_table(payload: TableRequest, request: Request) -> Dict[str, Any]:
    """
    Delete all rows from a specific table after schema/table validation.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        _validate_table_target(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )

        qualified_table = (
            f"{_quote_identifier(payload.schema_name)}.{_quote_identifier(payload.table_name)}"
        )
        with engine.begin() as conn:
            result = conn.execute(text(f"DELETE FROM {qualified_table}"))

        return {
            "schema_name": payload.schema_name,
            "table_name": payload.table_name,
            "row_count": max(int(result.rowcount or 0), 0),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to purge table: {str(e)}")
    finally:
        engine.dispose()
