import pandas as pd

from core.core import _parse_alpha_vantage_listing_status_csv, merge_symbol_sources, upsert_symbols_to_db


def test_parse_alpha_vantage_listing_status_filters_active_stock():
    csv_text = """symbol,name,exchange,assetType,ipoDate,delistingDate,status
AAPL,Apple Inc,NASDAQ,Stock,1980-12-12,null,Active
ETF1,Example ETF,NYSE,ETF,2000-01-01,null,Active
OLD,Old Co,NYSE,Stock,1990-01-01,2020-01-01,Delisted
"""
    df = _parse_alpha_vantage_listing_status_csv(csv_text)
    assert set(df["Symbol"].tolist()) == {"AAPL"}
    assert "Exchange" in df.columns
    assert "AssetType" in df.columns
    assert "Status" in df.columns


def test_merge_symbol_sources_prefers_nasdaq_name_and_keeps_massive_metadata():
    df_nasdaq = pd.DataFrame([{"Symbol": "AAPL", "Name": "Apple Inc", "Sector": "Tech"}])
    df_massive = pd.DataFrame(
        [{"Symbol": "AAPL", "Name": "APPLE", "Exchange": "NASDAQ", "AssetType": "CS"}]
    )
    merged = merge_symbol_sources(df_nasdaq, df_massive)
    row = merged[merged["Symbol"] == "AAPL"].iloc[0]

    assert row["Name"] == "Apple Inc"
    assert row["Exchange"] == "NASDAQ"
    assert row["source_nasdaq"] == True
    assert row["source_massive"] == True
    assert row["source_alpha_vantage"] == False
    assert "source" not in merged.columns


def test_merge_symbol_sources_ignores_alpha_only_symbols():
    df_nasdaq = pd.DataFrame(columns=["Symbol", "Name"])
    df_massive = pd.DataFrame(columns=["Symbol", "Name"])
    df_av = pd.DataFrame(
        [{"Symbol": "NEW", "Name": "New Co", "Exchange": "NYSE", "AssetType": "Stock", "Status": "Active"}]
    )

    merged = merge_symbol_sources(df_nasdaq, df_massive, df_alpha_vantage=df_av)
    assert merged.empty


class _FakeCursor:
    def __init__(self) -> None:
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def executemany(self, sql: str, rows) -> None:
        self.executemany_calls.append((sql, list(rows)))


def test_upsert_symbols_coerces_source_flags_to_bool():
    cur = _FakeCursor()
    df_symbols = pd.DataFrame(
        [
            {"Symbol": "AAPL", "source_nasdaq": True, "source_alpha_vantage": False},
            {"Symbol": "SPY", "source_nasdaq": float("nan"), "source_alpha_vantage": float("nan")},
            {"Symbol": "QQQ", "source_nasdaq": 1.0, "source_alpha_vantage": 0.0},
        ]
    )

    upsert_symbols_to_db(df_symbols, cur=cur)

    assert len(cur.executemany_calls) == 1
    sql, rows = cur.executemany_calls[0]
    assert "source_nasdaq" in sql
    assert "source_alpha_vantage" in sql

    # row tuple shape: (symbol, source_nasdaq, source_alpha_vantage)
    assert rows[0][1] is True
    assert rows[0][2] is False
    assert rows[1][1] is False
    assert rows[1][2] is False
    assert rows[2][1] is True
    assert rows[2][2] is False
