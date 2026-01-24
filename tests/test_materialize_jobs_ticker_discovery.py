import pandas as pd


def test_materialize_gold_market_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.market_data import materialize_gold_market_by_date as job

    cfg = job.MaterializeConfig(
        container="market",
        year_month="2026-01",
        output_path="market_by_date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    load_calls = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_calls.append((container, path))
        return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "symbol": ["AAPL"], "feature": [1.0]})

    captured = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_market_by_date(cfg) == 0
    assert load_calls == [("market", "market/AAPL")]
    assert captured["container"] == "market"
    assert captured["path"] == "market_by_date"
    assert captured["predicate"] == "year_month = '2026-01'"
    assert "year_month" in captured["df"].columns


def test_materialize_gold_earnings_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.earnings_data import materialize_gold_earnings_by_date as job

    cfg = job.MaterializeConfig(
        container="gold",
        year_month="2026-01",
        output_path="earnings_by_date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("gold", "earnings/AAPL")
        return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_earnings_by_date(cfg) == 0
    assert store_calls["count"] == 1


def test_materialize_gold_finance_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.finance_data import materialize_gold_finance_by_date as job

    cfg = job.MaterializeConfig(
        container="gold",
        year_month="2026-01",
        output_path="finance_by_date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("gold", "finance/AAPL")
        return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_finance_by_date(cfg) == 0
    assert store_calls["count"] == 1


def test_materialize_gold_price_target_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.price_target_data import materialize_gold_price_target_by_date as job

    cfg = job.MaterializeConfig(
        container="gold",
        year_month="2026-01",
        output_path="targets_by_date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("gold", "targets/AAPL")
        return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_targets_by_date(cfg) == 0
    assert store_calls["count"] == 1


def test_materialize_silver_earnings_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.earnings_data import materialize_silver_earnings_by_date as job

    cfg = job.MaterializeConfig(
        container="earnings",
        year_month="2026-01",
        output_path="earnings-data-by-date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("earnings", "earnings-data/AAPL")
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_earnings_by_date(cfg) == 0
    assert store_calls["count"] == 1


def test_materialize_silver_price_target_by_date_prefers_container_listing(monkeypatch):
    from asset_allocation.tasks.price_target_data import materialize_silver_price_target_by_date as job

    cfg = job.MaterializeConfig(
        container="targets",
        year_month="2026-01",
        output_path="price-target-data-by-date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("targets", "price-target-data/AAPL")
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_targets_by_date(cfg) == 0
    assert store_calls["count"] == 1


def test_materialize_silver_finance_by_date_prefers_container_listing_and_skips_missing_tables(monkeypatch):
    from asset_allocation.tasks.finance_data import materialize_silver_finance_by_date as job

    cfg = job.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="finance-data-by-date",
        max_tickers=None,
    )

    available_roots = {"finance-data/balance_sheet/AAPL_quarterly_balance-sheet"}
    monkeypatch.setattr(job, "_try_load_finance_table_roots_from_container", lambda _container: available_roots)
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    load_calls = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_calls.append((container, path))
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "Total Assets": [123.0]})

    store_calls = {"count": 0}

    def fake_store_delta(*_args, **_kwargs):
        store_calls["count"] += 1

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_finance_by_date(cfg) == 0
    assert load_calls == [("silver", "finance-data/balance_sheet/AAPL_quarterly_balance-sheet")]
    assert store_calls["count"] == 1
