import pandas as pd

from scripts.market_data import materialize_silver_market_by_date as materialize


def test_extract_tickers_from_market_data_blobs_filters_delta_logs():
    blob_names = [
        "market-data/AAPL/_delta_log/00000000000000000000.json",
        "market-data/AAPL/part-000.parquet",
        "market-data/MSFT/_delta_log/00000000000000000001.checkpoint.parquet",
        "market-data/EMPTY/_delta_log/",
        "market-data/EMPTY/_delta_log/_last_checkpoint",
        "market-data/NOLOG/part-000.parquet",
        "other-data/XYZ/_delta_log/00000000000000000000.json",
    ]

    assert materialize._extract_tickers_from_market_data_blobs(blob_names) == ["AAPL", "MSFT"]


def test_materialize_prefers_container_listing(monkeypatch):
    cfg = materialize.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="market-data-by-date",
        max_tickers=None,
    )

    monkeypatch.setattr(materialize, "_try_load_tickers_from_silver_container", lambda _container: ["AAPL"])
    monkeypatch.setattr(materialize, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    load_calls = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_calls.append((container, path))
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "Open": [1.0]})

    captured = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(materialize, "load_delta", fake_load_delta)
    monkeypatch.setattr(materialize, "store_delta", fake_store_delta)

    assert materialize.materialize_silver_market_by_date(cfg) == 0
    assert load_calls == [("silver", "market-data/AAPL")]
    assert captured["container"] == "silver"
    assert captured["path"] == "market-data-by-date"
    assert captured["predicate"] == "year_month = '2026-01'"
    assert "year_month" in captured["df"].columns


def test_materialize_no_tickers_in_container_exits_without_fallback(monkeypatch):
    cfg = materialize.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="market-data-by-date",
        max_tickers=None,
    )

    monkeypatch.setattr(materialize, "_try_load_tickers_from_silver_container", lambda _container: [])
    monkeypatch.setattr(materialize, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setattr(materialize, "load_delta", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError()))

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(materialize, "store_delta", fake_store_delta)

    assert materialize.materialize_silver_market_by_date(cfg) == 0
    assert store_calls["count"] == 0


def test_materialize_falls_back_to_symbol_universe_when_listing_unavailable(monkeypatch):
    cfg = materialize.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="market-data-by-date",
        max_tickers=None,
    )

    monkeypatch.setattr(materialize, "_try_load_tickers_from_silver_container", lambda _container: None)
    monkeypatch.setattr(materialize, "_load_ticker_universe", lambda: ["AAPL"])

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("silver", "market-data/AAPL")
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "Open": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(materialize, "load_delta", fake_load_delta)
    monkeypatch.setattr(materialize, "store_delta", fake_store_delta)

    assert materialize.materialize_silver_market_by_date(cfg) == 0
    assert store_calls["count"] == 1

