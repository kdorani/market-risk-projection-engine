from datetime import date

import numpy as np
import pytest

from projection_engine.curves import BP, ScenarioShocks, linear_interp_weights
from projection_engine.frtb import GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS
from projection_engine.methods import bumped_rf_sensitivities, frtb_vertex_deltas

from .conftest import RF_IDS, make_bond


def test_interp_weights_partition_of_unity_and_flat_extrapolation():
    grid = np.array([1.0, 2.0, 5.0])
    w = linear_interp_weights(grid, np.array([0.5, 1.0, 1.5, 4.0, 9.0]))
    assert np.allclose(w.sum(axis=0), 1.0)
    assert np.allclose(w[:, 0], [1, 0, 0]) and np.allclose(w[:, 4], [0, 0, 1])
    assert np.allclose(w[:, 3], [0, 1 / 3, 2 / 3])


def test_cashflows_dates_and_amounts(bond, market):
    cf = bond.cashflows(market)
    assert len(cf) == 5
    assert cf["pay_date"].iloc[-1] == date(2031, 11, 17)  # Saturday maturity paid Monday
    assert cf["amount"].sum() == pytest.approx(13_000_000)
    assert cf["pv"].sum() == pytest.approx(bond.present_value(market))


def test_accrued_interest_icma(bond, market, long_bond):
    assert bond.accrued_interest(market.valuation_date) == pytest.approx(600_000 * 9 / 365)
    assert long_bond.accrued_interest(market.valuation_date) == pytest.approx(106_250 * 101 / 184)


def test_short_position_has_negative_pv(market):
    short = make_bond(notional=-20_000_000.0, coupon_rate=0.035, coupon_frequency=2,
                      issue_date=date(2025, 11, 15), maturity_date=date(2028, 11, 15))
    assert short.present_value(market) < 0


def test_bumped_delta_and_gamma_match_analytic(bond, market):
    cf = bond.cashflows(market)
    t, a, df = cf["t_years"].to_numpy(), cf["amount"].to_numpy(), cf["discount_factor"].to_numpy()
    analytic_parallel_delta = -(t * a * df).sum() * BP
    sens = bumped_rf_sensitivities(bond, market, RF_IDS)
    # key-rate deltas add up to the parallel delta because interpolation weights sum to one
    assert sens["delta_per_bp"].sum() == pytest.approx(analytic_parallel_delta, rel=1e-6)
    parallel = ScenarioShocks(RF_IDS, np.array([[1.0] * 11, [-1.0] * 11]))
    up, down = bond.revalue(market, parallel)
    gamma_parallel = (up - 2 * bond.present_value(market) + down)
    assert gamma_parallel == pytest.approx((t**2 * a * df).sum() * BP**2, rel=1e-4)


def test_risk_factor_ids_only_where_exposed(bond, market):
    assert bond.risk_factor_ids(market) == [
        "IR.USD.TSY.ZERO.6M", "IR.USD.TSY.ZERO.1Y", "IR.USD.TSY.ZERO.2Y",
        "IR.USD.TSY.ZERO.3Y", "IR.USD.TSY.ZERO.5Y",
    ]


def test_frtb_vertex_deltas_sum_to_parallel(bond, market):
    v = frtb_vertex_deltas(bond, market, "USD_TSY", GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS)
    s = bumped_rf_sensitivities(bond, market, RF_IDS)
    # forward vs central differences differ only by ~gamma/2 per vertex
    assert v["delta_per_bp"].sum() == pytest.approx(s["delta_per_bp"].sum(), rel=5e-4)
