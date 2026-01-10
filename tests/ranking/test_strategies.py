from datetime import date

import pandas as pd

from scripts.ranking.strategies import BrokenGrowthImprovingInternalsStrategy


def test_broken_growth_strategy_selects_valid_rows():
    df = pd.DataFrame(
        {
            "symbol": ["TIN", "SILVER"],
            "drawdown_1y": [-0.5, -0.1],
            "rev_yoy": [0.2, 0.3],
            "rev_growth_slope_6q": [0.05, 0.1],
            "ebitda_margin": [0.25, 0.15],
            "margin_delta_qoq": [0.01, -0.2],
            "tp_mean_change_30d": [1.0, -0.5],
            "rev_net": [25, -5],
            "disp_norm_change_30d": [-0.05, 0.1],
        }
    )

    strategy = BrokenGrowthImprovingInternalsStrategy()
    results = strategy.rank(df, date.today())

    assert len(results) == 1
    ranking = results[0]
    assert ranking.symbol == "TIN"
    assert ranking.score == 5.0
    assert ranking.meta["broken_drawdown"]
    assert ranking.meta["improving_revenue"]
    assert ranking.meta["margin_stable"]
    assert ranking.meta["analysts_improving"]
    assert ranking.meta["target_trending_up"]


def test_broken_growth_strategy_skips_missing_columns():
    df = pd.DataFrame({"symbol": ["X"]})
    strategy = BrokenGrowthImprovingInternalsStrategy()
    results = strategy.rank(df, date.today())
    assert results == []
