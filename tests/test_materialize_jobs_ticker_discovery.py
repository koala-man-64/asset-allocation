import pandas as pd


def test_materialize_gold_market_resolve_container_prefers_gold_env(monkeypatch):
    from tasks.market_data import materialize_gold_market_by_date as job

    monkeypatch.setenv("AZURE_CONTAINER_GOLD", "gold-container")
    monkeypatch.setenv("AZURE_FOLDER_MARKET", "market-folder")

    assert job._resolve_container(None) == "gold-container"


def test_materialize_gold_market_resolve_container_falls_back_to_market_env(monkeypatch):
    from tasks.market_data import materialize_gold_market_by_date as job

    monkeypatch.delenv("AZURE_CONTAINER_GOLD", raising=False)
    monkeypatch.setenv("AZURE_FOLDER_MARKET", "market-folder")

    assert job._resolve_container(None) == "market-folder"


def test_materialize_gold_market_by_date_prefers_container_listing(monkeypatch):
    from tasks.market_data import materialize_gold_market_by_date as job

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


def test_discover_gold_market_year_months_from_data(monkeypatch):
    from tasks.market_data import materialize_gold_market_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"date": [pd.Timestamp("2026-01-10"), pd.Timestamp("2026-02-05")]})
        return pd.DataFrame({"date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="market")

    assert months == ["2026-01", "2026-02"]


def test_discover_silver_market_year_months_from_data(monkeypatch):
    from tasks.market_data import materialize_silver_market_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_silver_container", lambda _container: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"Date": [pd.Timestamp("2026-01-10"), pd.Timestamp("2026-02-05")]})
        return pd.DataFrame({"Date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="silver")

    assert months == ["2026-01", "2026-02"]


def test_materialize_gold_earnings_by_date_prefers_container_listing(monkeypatch):
    from tasks.earnings_data import materialize_gold_earnings_by_date as job

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


def test_discover_gold_earnings_year_months_from_data(monkeypatch):
    from tasks.earnings_data import materialize_gold_earnings_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"date": [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-02-03")]})
        return pd.DataFrame({"date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="earnings")

    assert months == ["2026-01", "2026-02"]


def test_materialize_gold_finance_by_date_prefers_container_listing(monkeypatch):
    from tasks.finance_data import materialize_gold_finance_by_date as job

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


def test_discover_gold_finance_year_months_from_data(monkeypatch):
    from tasks.finance_data import materialize_gold_finance_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"date": [pd.Timestamp("2026-01-07"), pd.Timestamp("2026-02-11")]})
        return pd.DataFrame({"date": [pd.Timestamp("2026-02-21")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="finance")

    assert months == ["2026-01", "2026-02"]


def test_materialize_gold_finance_by_date_normalizes_object_columns(monkeypatch):
    from tasks.finance_data import materialize_gold_finance_by_date as job

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
        return pd.DataFrame(
            {
                "date": [pd.Timestamp("2026-01-15")],
                "all_null_obj": [None],
                "text_obj": ["1"],
                "feature": [1.0],
            }
        )

    captured = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_finance_by_date(cfg) == 0
    assert str(captured["df"]["all_null_obj"].dtype) == "string"
    assert str(captured["df"]["text_obj"].dtype) == "string"
    assert captured["df"]["all_null_obj"].isna().all()
    assert captured["predicate"] == "year_month = '2026-01'"
    assert "year_month" in captured["df"].columns


def test_materialize_gold_price_target_by_date_prefers_container_listing(monkeypatch):
    from tasks.price_target_data import materialize_gold_price_target_by_date as job

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


def test_discover_gold_price_target_year_months_from_data(monkeypatch):
    from tasks.price_target_data import materialize_gold_price_target_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))
    load_calls = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_calls.append({"columns": columns, "filters": filters})
        if path.endswith("/AAPL"):
            return pd.DataFrame({"obs_date": [pd.Timestamp("2026-01-04"), pd.Timestamp("2026-02-09")]})
        return pd.DataFrame({"obs_date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="targets")

    assert months == ["2026-01", "2026-02"]
    assert ["date", "obs_date"] not in [c["columns"] for c in load_calls]
    assert all(c["columns"] in (["obs_date"], ["date"]) for c in load_calls)


def test_discover_gold_price_target_year_months_from_data_falls_back_to_date(monkeypatch):
    from tasks.price_target_data import materialize_gold_price_target_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))
    load_columns = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_columns.append(tuple(columns or []))
        if columns == ["obs_date"]:
            return None
        if path.endswith("/AAPL"):
            return pd.DataFrame({"date": [pd.Timestamp("2026-01-04"), pd.Timestamp("2026-02-09")]})
        return pd.DataFrame({"date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="targets")

    assert months == ["2026-01", "2026-02"]
    assert load_columns.count(("obs_date",)) == 2
    assert load_columns.count(("date",)) == 2


def test_materialize_gold_price_target_by_date_falls_back_to_date_filter(monkeypatch):
    from tasks.price_target_data import materialize_gold_price_target_by_date as job

    cfg = job.MaterializeConfig(
        container="gold",
        year_month="2026-01",
        output_path="targets_by_date",
        max_tickers=None,
    )

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))
    filter_columns = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        assert (container, path) == ("gold", "targets/AAPL")
        assert columns is None
        assert filters is not None
        filter_col = filters[0][0]
        filter_columns.append(filter_col)
        if filter_col == "obs_date":
            return None
        if filter_col == "date":
            return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})
        raise AssertionError(f"Unexpected filter column: {filter_col}")

    captured = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_targets_by_date(cfg) == 0
    assert filter_columns == ["obs_date", "date"]
    assert captured["predicate"] == "year_month = '2026-01'"
    assert "year_month" in captured["df"].columns
    assert (captured["df"]["year_month"] == "2026-01").all()


def test_materialize_silver_earnings_by_date_prefers_container_listing(monkeypatch):
    from tasks.earnings_data import materialize_silver_earnings_by_date as job

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


def test_discover_silver_earnings_year_months_from_data(monkeypatch):
    from tasks.earnings_data import materialize_silver_earnings_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-02-03")]})
        return pd.DataFrame({"Date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="earnings")

    assert months == ["2026-01", "2026-02"]


def test_materialize_silver_price_target_by_date_prefers_container_listing(monkeypatch):
    from tasks.price_target_data import materialize_silver_price_target_by_date as job

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
        return pd.DataFrame({"obs_date": [pd.Timestamp("2026-01-15")], "feature": [1.0]})

    captured: dict = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_targets_by_date(cfg) == 0
    assert captured["container"] == "targets"
    assert captured["path"] == "price-target-data-by-date"
    assert captured["predicate"] == "year_month = '2026-01'"
    assert captured["partition_by"] == ["year_month", "Date"]
    assert "year_month" in captured["df"].columns
    assert "Date" in captured["df"].columns


def test_materialize_silver_price_target_by_date_normalizes_legacy_date_alias(monkeypatch):
    from tasks.price_target_data import materialize_silver_price_target_by_date as job

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
        return pd.DataFrame({"date": [pd.Timestamp("2026-01-15")], "feature": [2.0]})

    captured: dict = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_targets_by_date(cfg) == 0
    assert captured["partition_by"] == ["year_month", "Date"]
    assert "Date" in captured["df"].columns
    assert "date" not in captured["df"].columns


def test_discover_silver_price_target_year_months_from_data(monkeypatch):
    from tasks.price_target_data import materialize_silver_price_target_by_date as job

    monkeypatch.setattr(job, "_try_load_tickers_from_container", lambda _container, root_prefix: ["AAPL", "MSFT"])
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("/AAPL"):
            return pd.DataFrame({"obs_date": [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-02-03")]})
        return pd.DataFrame({"obs_date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="targets")

    assert months == ["2026-01", "2026-02"]


def test_materialize_silver_finance_by_date_prefers_container_listing_and_skips_missing_tables(monkeypatch):
    from tasks.finance_data import materialize_silver_finance_by_date as job

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


def test_discover_silver_finance_year_months_from_data(monkeypatch):
    from tasks.finance_data import materialize_silver_finance_by_date as job

    available_roots = {
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        "finance-data/income_statement/MSFT_quarterly_financials",
    }
    monkeypatch.setattr(job, "_try_load_finance_table_roots_from_container", lambda _container: available_roots)
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        if path.endswith("AAPL_quarterly_balance-sheet"):
            return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-02-03")]})
        return pd.DataFrame({"Date": [pd.Timestamp("2026-02-20")]})

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    months = job.discover_year_months_from_data(container="silver")

    assert months == ["2026-01", "2026-02"]


def test_materialize_silver_finance_by_date_uses_date_filters(monkeypatch):
    from tasks.finance_data import materialize_silver_finance_by_date as job

    cfg = job.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="finance-data-by-date",
        max_tickers=None,
    )

    available_roots = {"finance-data/balance_sheet/AAPL_quarterly_balance-sheet"}
    monkeypatch.setattr(job, "_try_load_finance_table_roots_from_container", lambda _container: available_roots)
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))
    monkeypatch.setenv("SILVER_FINANCE_BY_DATE_MAX_WORKERS", "1")

    load_calls = []

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        load_calls.append({"container": container, "path": path, "filters": filters})
        return pd.DataFrame({"Date": [pd.Timestamp("2026-01-15")], "Total Assets": [123.0]})

    captured = {}

    def fake_store_delta(df, **kwargs):
        captured["df"] = df.copy()
        captured.update(kwargs)

    monkeypatch.setattr(job, "load_delta", fake_load_delta)
    monkeypatch.setattr(job, "store_delta", fake_store_delta)

    assert job.materialize_silver_finance_by_date(cfg) == 0
    assert len(load_calls) == 1
    assert load_calls[0]["filters"] is not None
    assert load_calls[0]["filters"][0][0] == "Date"
    assert captured["predicate"] == "year_month = '2026-01'"


def test_materialize_silver_finance_by_date_parallel_matches_serial(monkeypatch):
    from tasks.finance_data import materialize_silver_finance_by_date as job

    cfg = job.MaterializeConfig(
        container="silver",
        year_month="2026-01",
        output_path="finance-data-by-date",
        max_tickers=None,
    )

    available_roots = {
        "finance-data/balance_sheet/AAPL_quarterly_balance-sheet",
        "finance-data/balance_sheet/MSFT_quarterly_balance-sheet",
    }
    monkeypatch.setattr(job, "_try_load_finance_table_roots_from_container", lambda _container: available_roots)
    monkeypatch.setattr(job, "_load_ticker_universe", lambda: (_ for _ in ()).throw(AssertionError()))

    def fake_load_delta(container, path, version=None, columns=None, filters=None):
        ticker = "AAPL" if "AAPL" in path else "MSFT"
        return pd.DataFrame(
            {
                "Date": [pd.Timestamp("2026-01-10"), pd.Timestamp("2026-01-20")],
                "metric": [1.0, 2.0] if ticker == "AAPL" else [3.0, 4.0],
            }
        )

    monkeypatch.setattr(job, "load_delta", fake_load_delta)

    def run_and_capture(max_workers: str) -> pd.DataFrame:
        monkeypatch.setenv("SILVER_FINANCE_BY_DATE_MAX_WORKERS", max_workers)
        captured = {}

        def fake_store_delta(df, **kwargs):
            captured["df"] = df.copy()
            captured.update(kwargs)

        monkeypatch.setattr(job, "store_delta", fake_store_delta)
        assert job.materialize_silver_finance_by_date(cfg) == 0
        out = captured["df"].copy()
        return out.sort_values(["Symbol", "Date"]).reset_index(drop=True)

    serial_df = run_and_capture("1")
    parallel_df = run_and_capture("4")

    assert serial_df.equals(parallel_df)
