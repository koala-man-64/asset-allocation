import importlib.util
import os
from datetime import date, datetime, time as time_value
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import MetaData, String as SqlString, Table, and_, cast, create_engine, inspect, select, text

from api.service.dependencies import get_settings

router = APIRouter()
_HIDDEN_EXPLORER_SCHEMAS = frozenset({"information_schema", "public"})
_BOOLEAN_TRUE_VALUES = frozenset({"1", "true", "t", "yes", "y", "on"})
_BOOLEAN_FALSE_VALUES = frozenset({"0", "false", "f", "no", "n", "off"})
_SUPPORTED_GOLD_EXPLORER_TABLES = frozenset(
    {
        "market_data",
        "finance_data",
        "earnings_data",
        "price_target_data",
        "regime_inputs_daily",
        "regime_history",
        "regime_latest",
        "regime_transitions",
    }
)


class QueryFilter(BaseModel):
    column_name: str = Field(min_length=1)
    operator: Literal[
        "eq",
        "neq",
        "contains",
        "starts_with",
        "ends_with",
        "gt",
        "gte",
        "lt",
        "lte",
        "is_null",
        "is_not_null",
    ]
    value: Optional[Any] = None


class QueryRequest(BaseModel):
    schema_name: str
    table_name: str
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    filters: List[QueryFilter] = Field(default_factory=list)


class TableRequest(BaseModel):
    schema_name: str
    table_name: str


class PostgresColumnMetadata(BaseModel):
    name: str
    data_type: str
    nullable: bool
    primary_key: bool
    editable: bool
    edit_reason: Optional[str] = None


class TableMetadataResponse(BaseModel):
    schema_name: str
    table_name: str
    primary_key: List[str]
    can_edit: bool
    edit_reason: Optional[str] = None
    columns: List[PostgresColumnMetadata]


class UpdateRowRequest(BaseModel):
    schema_name: str
    table_name: str
    match: Dict[str, Any] = Field(default_factory=dict)
    values: Dict[str, Any] = Field(default_factory=dict)


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
    if dsn.startswith("postgresql+asyncpg://"):
        dsn = "postgresql://" + dsn.removeprefix("postgresql+asyncpg://")

    return _normalize_sync_driver(dsn)


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier or "").replace('"', '""') + '"'


def _is_hidden_explorer_table(*, schema_name: str, table_name: str) -> bool:
    normalized_schema = str(schema_name or "").strip().lower()
    normalized_table = str(table_name or "").strip().lower()
    return normalized_schema == "gold" and normalized_table not in _SUPPORTED_GOLD_EXPLORER_TABLES


def _visible_table_names(insp: Any, *, schema_name: str) -> List[str]:
    return sorted(
        table_name
        for table_name in insp.get_table_names(schema=schema_name)
        if not _is_hidden_explorer_table(schema_name=schema_name, table_name=str(table_name or ""))
    )


def _validate_table_target(insp: Any, *, schema_name: str, table_name: str) -> None:
    schema_names = insp.get_schema_names()
    if schema_name not in schema_names:
        raise HTTPException(status_code=404, detail=f"Schema '{schema_name}' not found.")

    if table_name not in _visible_table_names(insp, schema_name=schema_name):
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_name}' not found in schema '{schema_name}'.",
        )


def _reflect_table(engine: Any, *, schema_name: str, table_name: str) -> Table:
    metadata = MetaData()
    return Table(table_name, metadata, schema=schema_name, autoload_with=engine)


def _load_table_metadata(
    insp: Any,
    *,
    schema_name: str,
    table_name: str,
) -> TableMetadataResponse:
    _validate_table_target(
        insp,
        schema_name=schema_name,
        table_name=table_name,
    )

    pk_constraint = insp.get_pk_constraint(table_name, schema=schema_name) or {}
    primary_key = [
        str(name)
        for name in (pk_constraint.get("constrained_columns") or [])
        if str(name or "").strip()
    ]
    primary_key_set = set(primary_key)

    columns: List[PostgresColumnMetadata] = []
    has_editable_columns = False
    for column in insp.get_columns(table_name, schema=schema_name):
        name = str(column.get("name") or "").strip()
        if not name:
            continue

        is_generated = bool(column.get("computed")) or bool(column.get("identity"))
        editable = not is_generated
        if editable:
            has_editable_columns = True

        columns.append(
            PostgresColumnMetadata(
                name=name,
                data_type=str(column.get("type") or ""),
                nullable=bool(column.get("nullable", True)),
                primary_key=name in primary_key_set,
                editable=editable,
                edit_reason=None if editable else "Generated or identity column is read-only.",
            )
        )

    can_edit = bool(primary_key) and has_editable_columns
    edit_reason: Optional[str] = None
    if not primary_key:
        edit_reason = "Table has no primary key; row editing is disabled."
    elif not has_editable_columns:
        edit_reason = "Table exposes no editable columns."

    return TableMetadataResponse(
        schema_name=schema_name,
        table_name=table_name,
        primary_key=primary_key,
        can_edit=can_edit,
        edit_reason=edit_reason,
        columns=columns,
    )


def _normalize_data_type(value: object) -> str:
    return str(value or "").strip().lower()


def _is_text_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(token in normalized for token in ("char", "text", "uuid", "citext"))


def _is_integer_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(token in normalized for token in ("smallint", "integer", "bigint", "serial"))


def _is_numeric_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return any(
        token in normalized
        for token in ("smallint", "integer", "bigint", "numeric", "decimal", "real", "double", "float", "serial", "money")
    )


def _is_boolean_type(data_type: str) -> bool:
    return "bool" in _normalize_data_type(data_type)


def _is_datetime_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "timestamp" in normalized or "datetime" in normalized


def _is_date_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "date" in normalized and "time" not in normalized and "stamp" not in normalized


def _is_time_type(data_type: str) -> bool:
    normalized = _normalize_data_type(data_type)
    return "time" in normalized and "stamp" not in normalized


def _operator_requires_value(operator: str) -> bool:
    return operator not in {"is_null", "is_not_null"}


def _allowed_query_operators(data_type: str) -> set[str]:
    allowed = {"eq", "neq", "is_null", "is_not_null"}
    if _is_text_type(data_type):
        allowed.update({"contains", "starts_with", "ends_with"})
    if _is_numeric_type(data_type) or _is_datetime_type(data_type) or _is_date_type(data_type) or _is_time_type(data_type):
        allowed.update({"gt", "gte", "lt", "lte"})
    return allowed


def _parse_iso_datetime(raw_value: Any) -> datetime:
    candidate = str(raw_value or "").strip()
    if not candidate:
        raise ValueError("datetime value is required")
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    return datetime.fromisoformat(candidate)


def _coerce_filter_value(column: PostgresColumnMetadata, raw_value: Any) -> Any:
    if raw_value is None:
        raise ValueError(f'Filter "{column.name}" requires a value.')

    data_type = column.data_type
    normalized_value = str(raw_value).strip()

    if not normalized_value:
        raise ValueError(f'Filter "{column.name}" requires a value.')

    if _is_boolean_type(data_type):
        lowered = normalized_value.lower()
        if lowered in _BOOLEAN_TRUE_VALUES:
            return True
        if lowered in _BOOLEAN_FALSE_VALUES:
            return False
        raise ValueError(
            f'Filter "{column.name}" must be a boolean value (true/false, yes/no, 1/0).'
        )

    if _is_numeric_type(data_type):
        try:
            if _is_integer_type(data_type):
                return int(normalized_value)
            return float(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be numeric.') from exc

    if _is_datetime_type(data_type):
        try:
            return _parse_iso_datetime(normalized_value)
        except ValueError as exc:
            raise ValueError(
                f'Filter "{column.name}" must be an ISO-8601 timestamp.'
            ) from exc

    if _is_date_type(data_type):
        try:
            return date.fromisoformat(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be an ISO-8601 date.') from exc

    if _is_time_type(data_type):
        try:
            return time_value.fromisoformat(normalized_value)
        except ValueError as exc:
            raise ValueError(f'Filter "{column.name}" must be an ISO-8601 time.') from exc

    return normalized_value


def _build_query_conditions(
    table: Table,
    filters: List[QueryFilter],
    metadata: TableMetadataResponse,
) -> List[Any]:
    if not filters:
        return []

    metadata_by_name = {column.name: column for column in metadata.columns}
    conditions: List[Any] = []

    for filter_item in filters:
        column_metadata = metadata_by_name.get(filter_item.column_name)
        if not column_metadata:
            raise HTTPException(
                status_code=400,
                detail=f'Unknown query filter column "{filter_item.column_name}".',
            )

        allowed_operators = _allowed_query_operators(column_metadata.data_type)
        if filter_item.operator not in allowed_operators:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'Operator "{filter_item.operator}" is not supported for column '
                    f'"{filter_item.column_name}" ({column_metadata.data_type}).'
                ),
            )

        column = table.c[filter_item.column_name]

        if filter_item.operator == "is_null":
            conditions.append(column.is_(None))
            continue
        if filter_item.operator == "is_not_null":
            conditions.append(column.is_not(None))
            continue

        try:
            coerced_value = _coerce_filter_value(column_metadata, filter_item.value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        if filter_item.operator == "eq":
            conditions.append(column == coerced_value)
        elif filter_item.operator == "neq":
            conditions.append(column != coerced_value)
        elif filter_item.operator == "contains":
            conditions.append(cast(column, SqlString).ilike(f"%{coerced_value}%"))
        elif filter_item.operator == "starts_with":
            conditions.append(cast(column, SqlString).ilike(f"{coerced_value}%"))
        elif filter_item.operator == "ends_with":
            conditions.append(cast(column, SqlString).ilike(f"%{coerced_value}"))
        elif filter_item.operator == "gt":
            conditions.append(column > coerced_value)
        elif filter_item.operator == "gte":
            conditions.append(column >= coerced_value)
        elif filter_item.operator == "lt":
            conditions.append(column < coerced_value)
        elif filter_item.operator == "lte":
            conditions.append(column <= coerced_value)

    return conditions


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

        return _visible_table_names(insp, schema_name=schema_name)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch tables: {str(e)}")
    finally:
        engine.dispose()


@router.get("/schemas/{schema_name}/tables/{table_name}/metadata")
def get_table_metadata(schema_name: str, table_name: str, request: Request) -> TableMetadataResponse:
    """
    Return column metadata and editing capability for a specific table.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        return _load_table_metadata(
            insp,
            schema_name=schema_name,
            table_name=table_name,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load table metadata: {str(e)}")
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
        table = _reflect_table(
            engine,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        metadata = _load_table_metadata(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        conditions = _build_query_conditions(table, payload.filters, metadata)
        statement = select(table).limit(payload.limit).offset(payload.offset)
        if conditions:
            statement = statement.where(and_(*conditions))

        with engine.connect() as conn:
            rows = conn.execute(statement).mappings().all()

        return [dict(row) for row in rows]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution failed: {str(e)}")
    finally:
        engine.dispose()


@router.post("/update")
def update_row(payload: UpdateRowRequest, request: Request) -> Dict[str, Any]:
    """
    Update a single row using primary-key match values.
    """
    dsn = _resolve_postgres_dsn(request)
    if not dsn:
        raise HTTPException(status_code=500, detail="Database connection string not configured.")

    if not payload.values:
        raise HTTPException(status_code=400, detail="At least one field value is required.")

    engine = create_engine(dsn)
    try:
        insp = inspect(engine)
        metadata = _load_table_metadata(
            insp,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        if not metadata.can_edit:
            raise HTTPException(
                status_code=400,
                detail=metadata.edit_reason or "Row editing is disabled for this table.",
            )

        missing_match_columns = [
            column_name for column_name in metadata.primary_key if column_name not in payload.match
        ]
        if missing_match_columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Primary-key match values are required for row updates: "
                    + ", ".join(missing_match_columns)
                ),
            )

        column_lookup = {column.name: column for column in metadata.columns}
        unknown_columns = [
            column_name for column_name in payload.values.keys() if column_name not in column_lookup
        ]
        if unknown_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown columns requested for update: {', '.join(sorted(unknown_columns))}",
            )

        read_only_columns = [
            column_name
            for column_name in payload.values.keys()
            if not column_lookup[column_name].editable
        ]
        if read_only_columns:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Read-only columns cannot be updated: "
                    + ", ".join(sorted(read_only_columns))
                ),
            )

        table = _reflect_table(
            engine,
            schema_name=payload.schema_name,
            table_name=payload.table_name,
        )
        conditions = []
        for column_name in metadata.primary_key:
            column = table.c[column_name]
            match_value = payload.match.get(column_name)
            if match_value is None:
                conditions.append(column.is_(None))
            else:
                conditions.append(column == match_value)

        statement = table.update().where(and_(*conditions)).values(**payload.values)
        with engine.begin() as conn:
            result = conn.execute(statement)

        row_count = max(int(result.rowcount or 0), 0)
        if row_count == 0:
            raise HTTPException(
                status_code=404,
                detail="No row matched the provided primary-key values.",
            )

        return {
            "schema_name": payload.schema_name,
            "table_name": payload.table_name,
            "row_count": row_count,
            "updated_columns": sorted(payload.values.keys()),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update row: {str(e)}")
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
