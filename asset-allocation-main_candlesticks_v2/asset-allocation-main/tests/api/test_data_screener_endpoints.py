from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import pytest

from api.service.app import create_app
from tests.api._client import get_test_client


@dataclass
class _FakeConn:
    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
        return None


@pytest.mark.asyncio
async def test_symbols_endpoint_requires_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/symbols")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_screener_endpoint_requires_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get("/api/data/screener")
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_screener_endpoint_returns_joined_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://test:test@localhost:5432/asset_allocation")
    monkeypatch.setenv("AZURE_CONTAINER_SILVER", "test-container")
    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "test-container")

    symbols_df = pd.DataFrame(
        [
            {"symbol": "AAPL", "name": "Apple", "sector": "Tech", "industry": "Hardware", "country": "US", "is_optionable": True},
            {"symbol": "MSFT", "name": "Microsoft", "sector": "Tech", "industry": "Software", "country": "US", "is_optionable": True},
            {"symbol": "ZZZZ", "name": "No Data Co", "sector": "Other", "industry": "Other", "country": "US", "is_optionable": False},
        ]
    )

    as_of = datetime(2025, 1, 2)
    ym = "2025-01"

    gold_df = pd.DataFrame(
        [
            {"year_month": ym, "date": as_of, "symbol": "AAPL", "return_1d": 0.02, "return_5d": 0.04, "vol_20d": 0.25, "drawdown_1y": -0.1, "atr_14d": 3.2, "gap_atr": 0.5, "sma_50d": 180.0, "sma_200d": 150.0, "trend_50_200": 0.2, "above_sma_50": 1, "bb_width_20d": 0.08, "compression_score": 0.3, "volume_z_20d": 1.1, "volume_pct_rank_252d": 0.9},
            {"year_month": ym, "date": as_of, "symbol": "MSFT", "return_1d": -0.01, "return_5d": 0.01, "vol_20d": 0.18, "drawdown_1y": -0.05, "atr_14d": 2.1, "gap_atr": 0.2, "sma_50d": 330.0, "sma_200d": 310.0, "trend_50_200": 0.06, "above_sma_50": 0, "bb_width_20d": 0.05, "compression_score": 0.7, "volume_z_20d": -0.2, "volume_pct_rank_252d": 0.2},
        ]
    )

    silver_df = pd.DataFrame(
        [
            {"year_month": ym, "Date": as_of, "Symbol": "AAPL", "Open": 187.0, "High": 190.0, "Low": 185.0, "Close": 189.0, "Volume": 55_000_000},
            {"year_month": ym, "Date": as_of, "Symbol": "MSFT", "Open": 330.0, "High": 334.0, "Low": 328.0, "Close": 331.0, "Volume": 24_000_000},
        ]
    )

    def fake_connect(_dsn: str) -> _FakeConn:
        return _FakeConn()

    def fake_query_symbols(_conn: Any, *, q: Optional[str] = None) -> pd.DataFrame:
        if not q:
            return symbols_df.copy()
        needle = str(q).strip().upper()
        mask = symbols_df["symbol"].astype(str).str.upper().str.contains(needle, na=False) | symbols_df["name"].astype(str).str.upper().str.contains(needle, na=False)
        return symbols_df[mask].copy()

    def fake_load_delta(_container: str, path: str, version: int = None, columns=None, filters=None):  # type: ignore[no-untyped-def]
        if path == "market_by_date":
            return gold_df.copy()
        if path == "market-data-by-date":
            return silver_df.copy()
        raise AssertionError(f"Unexpected delta path: {path}")

    monkeypatch.setattr("api.endpoints.data.connect", fake_connect)
    monkeypatch.setattr("api.endpoints.data._query_symbols", fake_query_symbols)
    monkeypatch.setattr("api.endpoints.data.load_delta", fake_load_delta)

    app = create_app()
    async with get_test_client(app) as client:
        resp = await client.get(
            "/api/data/screener",
            params={"as_of": "2025-01-02", "limit": 3, "offset": 0, "sort": "return_1d"},
        )
    assert resp.status_code == 200
    payload: Dict[str, Any] = resp.json()

    assert payload["asOf"] == "2025-01-02"
    assert payload["total"] == 3
    assert payload["limit"] == 3
    assert payload["offset"] == 0
    assert len(payload["rows"]) == 3

    row0 = payload["rows"][0]
    assert row0["symbol"] == "AAPL"
    assert row0["close"] == 189.0
    assert row0["return1d"] == 0.02
    assert row0["hasSilver"] == 1
    assert row0["hasGold"] == 1

    # Missing data stays represented (left-join from symbols universe).
    missing = next((r for r in payload["rows"] if r["symbol"] == "ZZZZ"), None)
    assert missing is not None
    assert missing["hasSilver"] == 0
    assert missing["hasGold"] == 0
