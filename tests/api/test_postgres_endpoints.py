from unittest.mock import MagicMock, patch
import pandas as pd
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table

from api.service.app import create_app
from tests.api._client import get_test_client

# Helper to mock settings if needed, but endpoint uses resolve_postgres_dsn which checks ENV first.

@pytest.mark.asyncio
async def test_list_schemas(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public", "information_schema", "core", "gold"]
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas")
                 
    assert resp.status_code == 200
    assert resp.json() == ["core", "gold"]

@pytest.mark.asyncio
async def test_list_tables(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public"]
    mock_inspector.get_table_names.return_value = ["table1", "table2"]
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/public/tables")
                 
    assert resp.status_code == 200
    assert resp.json() == ["table1", "table2"]

@pytest.mark.asyncio
async def test_list_tables_404_schema(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public"]
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/missing_schema/tables")
                 
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]

@pytest.mark.asyncio
async def test_query_table_success(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public"]
    mock_inspector.get_table_names.return_value = ["test_table"]
    
    # Mock DataFrame result
    df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
             with patch("api.endpoints.postgres.pd.read_sql", return_value=df) as mock_read:
                app = create_app()
                async with get_test_client(app) as client:
                    resp = await client.post(
                        "/api/system/postgres/query",
                        json={
                            "schema_name": "public",
                            "table_name": "test_table",
                            "limit": 10,
                        },
                    )
    
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["col1"] == 1
    
    # Verify query structure roughly (optional)
    args, _ = mock_read.call_args
    query = args[0]
    assert 'FROM "public"."test_table"' in query
    assert 'LIMIT 10' in query

@pytest.mark.asyncio
async def test_query_table_security_fail(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public"]
    mock_inspector.get_table_names.return_value = ["test_table"]
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                # Try a missing table
                resp = await client.post(
                    "/api/system/postgres/query",
                    json={
                        "schema_name": "public",
                        "table_name": "missing_table",
                    },
                )
    
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_get_table_metadata_success(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol", "date"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "date", "type": "DATE", "nullable": False},
        {"name": "surprise", "type": "DOUBLE PRECISION", "nullable": True},
        {"name": "source_hash", "type": "TEXT", "nullable": False, "computed": {"sqltext": "md5('x')"}},
    ]

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas/gold/tables/market_data/metadata")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["schema_name"] == "gold"
    assert payload["table_name"] == "market_data"
    assert payload["primary_key"] == ["symbol", "date"]
    assert payload["can_edit"] is True
    assert any(
        col["name"] == "source_hash" and col["editable"] is False for col in payload["columns"]
    )


@pytest.mark.asyncio
async def test_update_row_success(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["symbol"]}
    mock_inspector.get_columns.return_value = [
        {"name": "symbol", "type": "TEXT", "nullable": False},
        {"name": "surprise", "type": "INTEGER", "nullable": True},
    ]

    mock_result = MagicMock()
    mock_result.rowcount = 1
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_begin = MagicMock()
    mock_begin.__enter__.return_value = mock_conn
    mock_begin.__exit__.return_value = False
    mock_engine.begin.return_value = mock_begin

    reflected_table = Table(
        "market_data",
        MetaData(),
        Column("symbol", String, primary_key=True),
        Column("surprise", Integer),
        schema="gold",
    )

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            with patch("api.endpoints.postgres._reflect_table", return_value=reflected_table):
                app = create_app()
                async with get_test_client(app) as client:
                    resp = await client.post(
                        "/api/system/postgres/update",
                        json={
                            "schema_name": "gold",
                            "table_name": "market_data",
                            "match": {"symbol": "AAPL"},
                            "values": {"surprise": 7},
                        },
                    )

    assert resp.status_code == 200
    assert resp.json() == {
        "schema_name": "gold",
        "table_name": "market_data",
        "row_count": 1,
        "updated_columns": ["surprise"],
    }
    assert mock_conn.execute.call_count == 1


@pytest.mark.asyncio
async def test_update_row_requires_primary_key(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_inspector.get_pk_constraint.return_value = {"constrained_columns": []}
    mock_inspector.get_columns.return_value = [
        {"name": "surprise", "type": "INTEGER", "nullable": True},
    ]

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/update",
                    json={
                        "schema_name": "gold",
                        "table_name": "market_data",
                        "match": {},
                        "values": {"surprise": 7},
                    },
                )

    assert resp.status_code == 400
    assert "primary key" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_purge_table_success(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]
    mock_result = MagicMock()
    mock_result.rowcount = 7
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_begin = MagicMock()
    mock_begin.__enter__.return_value = mock_conn
    mock_begin.__exit__.return_value = False
    mock_engine.begin.return_value = mock_begin

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/purge",
                    json={
                        "schema_name": "gold",
                        "table_name": "market_data",
                    },
                )

    assert resp.status_code == 200
    assert resp.json() == {
        "schema_name": "gold",
        "table_name": "market_data",
        "row_count": 7,
    }
    statement = mock_conn.execute.call_args[0][0]
    assert str(statement) == 'DELETE FROM "gold"."market_data"'


@pytest.mark.asyncio
async def test_purge_table_security_fail(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["gold"]
    mock_inspector.get_table_names.return_value = ["market_data"]

    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.post(
                    "/api/system/postgres/purge",
                    json={
                        "schema_name": "gold",
                        "table_name": "missing_table",
                    },
                )

    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"]
