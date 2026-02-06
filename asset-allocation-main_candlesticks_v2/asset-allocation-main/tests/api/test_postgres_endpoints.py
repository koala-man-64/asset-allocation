from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from api.service.app import create_app
from tests.api._client import get_test_client

# Helper to mock settings if needed, but endpoint uses resolve_postgres_dsn which checks ENV first.

@pytest.mark.asyncio
async def test_list_schemas(monkeypatch):
    mock_engine = MagicMock()
    mock_inspector = MagicMock()
    mock_inspector.get_schema_names.return_value = ["public", "information_schema"]
    
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://user:pass@localhost/db")

    with patch("api.endpoints.postgres.create_engine", return_value=mock_engine):
        with patch("api.endpoints.postgres.inspect", return_value=mock_inspector):
            app = create_app()
            async with get_test_client(app) as client:
                resp = await client.get("/api/system/postgres/schemas")
                 
    assert resp.status_code == 200
    assert resp.json() == ["information_schema", "public"] # endpoint sorts results

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
