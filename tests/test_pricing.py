import math
import pytest
from src.projection_engine.pricing import (
    fixed_bond_cashflows, pv_continuous, full_revaluation_pnl,
    taylor_pnl, interpolate_grid, ladder_pnl
)

def sample():
    return fixed_bond_cashflows(10_000_000, 0.06, 5, 1)

def test_cashflows_sum_to_principal_plus_coupons():
    cfs = sample()
    assert len(cfs) == 5
    assert sum(cf for _, cf in cfs) == pytest.approx(13_000_000)

def test_pv_is_positive_and_below_undiscounted_cashflows():
    pv = pv_continuous(sample(), 0.05)
    assert 0 < pv < 13_000_000

def test_rate_up_causes_bond_price_down():
    _, _, pnl = full_revaluation_pnl(sample(), 0.05, 10)
    assert pnl < 0

def test_rate_down_causes_bond_price_up():
    _, _, pnl = full_revaluation_pnl(sample(), 0.05, -10)
    assert pnl > 0

def test_grid_interpolation_exact_midpoint():
    grid = [(0.04, 100.0), (0.06, 80.0)]
    assert interpolate_grid(grid, 0.05) == pytest.approx(90.0)

def test_grid_rejects_extrapolation():
    with pytest.raises(ValueError):
        interpolate_grid([(0.04, 100.0), (0.06, 80.0)], 0.07)

def test_ladder_pnl():
    krd = [(1, 100.0), (5, 400.0)]
    shocks = [(1, 2.0), (5, 10.0)]
    assert ladder_pnl(krd, shocks) == pytest.approx(-4200.0)

def test_taylor_pnl_is_finite():
    pv = pv_continuous(sample(), 0.05)
    pnl = taylor_pnl(pv, 4700.0, 24.0, 10.0)
    assert math.isfinite(pnl)
