from __future__ import annotations

import pytest

from backtest.blend import normalize_alphas, normalize_exposure, weighted_sum


def test_normalize_alphas() -> None:
    assert normalize_alphas([2.0, 2.0]) == [0.5, 0.5]


def test_weighted_sum_aligns_symbols() -> None:
    out = weighted_sum([{"AAA": 1.0}, {"BBB": 1.0}], alphas=[0.5, 0.5])
    assert out == {"AAA": 0.5, "BBB": 0.5}


def test_normalize_exposure_gross() -> None:
    out = normalize_exposure({"AAA": 0.2, "BBB": -0.3}, mode="gross", target_gross=1.0)
    assert pytest.approx(sum(abs(v) for v in out.values())) == 1.0
    assert out["AAA"] == pytest.approx(0.4)
    assert out["BBB"] == pytest.approx(-0.6)


def test_normalize_exposure_net() -> None:
    out = normalize_exposure({"AAA": 0.2, "BBB": 0.3}, mode="net", target_net=1.0)
    assert pytest.approx(sum(out.values())) == 1.0
    assert out["AAA"] == pytest.approx(0.4)
    assert out["BBB"] == pytest.approx(0.6)

