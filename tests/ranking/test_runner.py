import pandas as pd
import pytest

from scripts.common import config as cfg
from scripts.ranking import runner


class DummyClient:
    pass


def _build_sample_dataframe():
    return pd.DataFrame(
        {
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

    monkeypatch.setattr(runner, "_build_blob_client", lambda _: DummyClient())

    def fake_load_parquet(*args, **kwargs):
        return market_df.copy()

    monkeypatch.setattr(runner, "load_parquet", fake_load_parquet)

    def fake_load_delta(container, path):
        if container == cfg.AZURE_CONTAINER_FINANCE:
            return finance_df.copy()
        if container == cfg.AZURE_CONTAINER_TARGETS:
            return price_df.copy()
        return pd.DataFrame()

    monkeypatch.setattr(runner, "load_delta", fake_load_delta)

    merged = runner.assemble_ranking_data()

    assert "rev_yoy" in merged.columns
    assert "tp_mean_change_30d" in merged.columns
    assert merged["symbol"].iat[0] == "ALPHA"


def test_runner_invokes_save_rankings(monkeypatch):
    data = _build_sample_dataframe()
    calls = []

    monkeypatch.setattr(runner, "assemble_ranking_data", lambda: data)
    monkeypatch.setattr(runner, "write_line", lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, "save_rankings", lambda results: calls.append(results))

    runner.main()

    assert calls
    assert all(results for results in calls)
