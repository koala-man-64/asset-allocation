import json

import pandas as pd
import pytest

from scripts.ranking import signals


def test_compute_ranking_signals_percentiles_and_year_month():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-02"), pd.Timestamp("2026-01-02")],
            "symbol": ["AAA", "BBB", "CCC"],
            "strategy": ["S1", "S1", "S2"],
            "rank": [1, 2, 1],
            "score": [10.0, 5.0, 1.0],
        }
    )

    out = signals.compute_ranking_signals(df)
    assert set(["date", "year_month", "symbol", "strategy", "rank", "rank_percentile", "n_symbols"]).issubset(
        out.columns
    )
    assert out["year_month"].nunique() == 1
    assert out["year_month"].iat[0] == "2026-01"

    # S1 has two symbols: rank 1 -> 1.0, rank 2 -> 0.0
    s1 = out[out["strategy"] == "S1"].set_index("symbol")
    assert s1.loc["AAA", "rank_percentile"] == pytest.approx(1.0)
    assert s1.loc["BBB", "rank_percentile"] == pytest.approx(0.0)
    assert s1.loc["AAA", "n_symbols"] == 2

    # S2 has one symbol: percentile defaults to 1.0
    s2 = out[out["strategy"] == "S2"].set_index("symbol")
    assert s2.loc["CCC", "rank_percentile"] == pytest.approx(1.0)
    assert s2.loc["CCC", "n_symbols"] == 1


def test_compute_composite_signals_equal_weights():
    df = pd.DataFrame(
        {
            "date": [pd.Timestamp("2026-01-02")] * 4,
            "year_month": ["2026-01"] * 4,
            "symbol": ["AAA", "AAA", "BBB", "BBB"],
            "strategy": ["S1", "S2", "S1", "S2"],
            "rank": [1, 2, 2, 1],
            "rank_percentile": [1.0, 0.0, 0.0, 1.0],
        }
    )

    out = signals.compute_composite_signals(df, weights={"S1": 1.0, "S2": 1.0}, top_n=50)
    assert out["symbol"].nunique() == 2
    assert set(["date", "year_month", "symbol", "composite_percentile", "composite_rank"]).issubset(out.columns)

    # Both symbols average to 0.5 -> same percentile -> dense rank 1 for both.
    assert out["composite_percentile"].tolist() == pytest.approx([0.5, 0.5])
    assert out["composite_rank"].tolist() == [1, 1]


def test_get_strategy_weights_env_override(monkeypatch):
    monkeypatch.setenv("RANKING_COMPOSITE_STRATEGY_WEIGHTS", json.dumps({"S1": 0.7, "S2": 0.3}))
    weights = signals.get_strategy_weights(["S1", "S2"])
    assert weights == {"S1": 0.7, "S2": 0.3}

    monkeypatch.setenv("RANKING_COMPOSITE_STRATEGY_WEIGHTS", json.dumps({"S1": 1.0}))
    with pytest.raises(ValueError, match="missing weights"):
        signals.get_strategy_weights(["S1", "S2"])

