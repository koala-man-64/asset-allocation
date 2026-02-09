import pandas as pd

from core.core import _parse_alpha_vantage_listing_status_csv, merge_symbol_sources


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


def test_merge_symbol_sources_prefers_nasdaq_name_and_keeps_alpha_metadata():
    df_nasdaq = pd.DataFrame([{"Symbol": "AAPL", "Name": "Apple Inc", "Sector": "Tech"}])
    df_av = pd.DataFrame([{"Symbol": "AAPL", "Name": "APPLE", "Exchange": "NASDAQ", "AssetType": "Stock", "Status": "Active"}])

    df_massive = pd.DataFrame(columns=["Symbol", "Name"])
    merged = merge_symbol_sources(df_nasdaq, df_av, df_massive)
    row = merged[merged["Symbol"] == "AAPL"].iloc[0]

    assert row["Name"] == "Apple Inc"
    assert row["Exchange"] == "NASDAQ"
    assert row["source_nasdaq"] == True
    assert row["source_alpha_vantage"] == True


def test_merge_symbol_sources_includes_alpha_only_symbols():
    df_nasdaq = pd.DataFrame(columns=["Symbol", "Name"])
    df_av = pd.DataFrame([{"Symbol": "NEW", "Name": "New Co", "Exchange": "NYSE", "AssetType": "Stock", "Status": "Active"}])

    df_massive = pd.DataFrame(columns=["Symbol", "Name"])
    merged = merge_symbol_sources(df_nasdaq, df_av, df_massive)
    row = merged[merged["Symbol"] == "NEW"].iloc[0]

    assert row["Name"] == "New Co"
    assert row["source_nasdaq"] == False
    assert row["source_alpha_vantage"] == True
