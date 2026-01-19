import pandas as pd
import pytest

from scripts.ranking import runner
from scripts.ranking.strategies import BrokenGrowthImprovingInternalsStrategy
from scripts.common import delta_core


class DummyClient:
    pass


def _build_sample_dataframe():
    return pd.DataFrame(
        {
            "date": [pd.Timestamp("2023-01-01")],
            "symbol": ["ALPHA"],
            "return_60d": [0.15],
            "pe_ratio": [12.0],
            "drawdown_1y": [-0.4],
            "rev_yoy": [0.1],
            "rev_growth_slope_6q": [0.05],
            "ebitda_margin": [0.2],
            "margin_delta_qoq": [0.02],
            "tp_mean_change_30d": [1.2],
            "rev_net": [5.0],
            "disp_norm_change_30d": [-0.1],
        }
    )


def test_assemble_ranking_data_merges_sources(monkeypatch):
    market_df = pd.DataFrame(
        {
            "symbol": ["ALPHA"],
            "return_60d": [0],
        }
    )
    finance_df = pd.DataFrame(
        {
            "symbol": ["ALPHA"],
            "rev_yoy": [0.1],
            "rev_growth_slope_6q": [0.02],
        }
    )
    price_df = pd.DataFrame(
        {
            "symbol": ["ALPHA"],
            "tp_mean_change_30d": [0.5],
            "disp_norm_change_30d": [-0.1],
        }
    )

    monkeypatch.setattr(runner, "_get_whitelist_intersection", lambda *_: {"ALPHA"})
    monkeypatch.setattr(runner, "_load_market_data", lambda *_: market_df.copy())

    def fake_load_delta_source(source, whitelist):
        if source["name"] == "finance":
            return finance_df.copy()
        if source["name"] == "price_targets":
            return price_df.copy()
        return pd.DataFrame()

    monkeypatch.setattr(runner, "_load_delta_source", fake_load_delta_source)

    strategy = BrokenGrowthImprovingInternalsStrategy()
    merged = runner.assemble_strategy_data(strategy)

    assert "rev_yoy" in merged.columns
    assert "tp_mean_change_30d" in merged.columns
    assert merged["symbol"].iat[0] == "ALPHA"


def test_runner_invokes_save_rankings(monkeypatch):
    data = _build_sample_dataframe()
    calls = []

    monkeypatch.setenv("RANKING_BROKEN_DRAWDOWN_THRESHOLD", "-0.3")
    monkeypatch.setenv("RANKING_MARGIN_DELTA_THRESHOLD", "0.0")
    monkeypatch.setenv("AZURE_CONTAINER_RANKING", "ranking-data")
    monkeypatch.setattr(runner, "assemble_strategy_data", lambda *_: data)
    monkeypatch.setattr(runner, "_load_existing_ranking_dates", lambda *_: set())
    monkeypatch.setattr(runner.mdc, "log_environment_diagnostics", lambda: None)
    monkeypatch.setattr(runner, "write_line", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "save_rankings", lambda results, **kwargs: calls.append(results))

    runner.main()

    assert calls
    assert all(results for results in calls)


def test_load_market_data_prefers_wide_table(monkeypatch):
    monkeypatch.setenv("RANKING_MARKET_WIDE_DELTA_PATH", "market_by_date")

    wide = pd.DataFrame(
        {
            "date": [pd.Timestamp("2023-01-01")],
            "symbol": ["ALPHA"],
            "return_60d": [0.15],
        }
    )

    delta_core.store_delta(
        wide,
        container=runner.cfg.AZURE_CONTAINER_MARKET,
        path="market_by_date",
        mode="overwrite",
    )

    loaded = runner._load_market_data({"ALPHA"})
    assert not loaded.empty
    assert loaded["symbol"].astype(str).unique().tolist() == ["ALPHA"]
