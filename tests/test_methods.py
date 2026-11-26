import numpy as np
import pytest

from projection_engine.curves import ScenarioShocks, linear_interp_weights
from projection_engine.frtb import GIRR_VERTEX_TENORS, GIRR_VERTICES
from projection_engine.methods import (
    apply_ladders,
    build_ladders,
    build_parallel_grid,
    bumped_rf_sensitivities,
    full_revaluation_pnl,
    interp_with_flag,
    taylor_pnl,
)

from .conftest import RF_IDS, TENORS

NODES = [-250, -200, -150, -100, -75, -50, -40, -30, -20, -10, -5, 0, 5, 10, 20, 30, 40, 50, 75, 100, 150, 200, 250]


def shocks(*rows):
    return ScenarioShocks(RF_IDS, np.array(rows, dtype=float))


def parallel(bp):
    return [bp] * len(RF_IDS)


def test_full_revaluation_zero_shock_is_zero(bond, market):
    assert full_revaluation_pnl(bond, market, shocks(parallel(0.0))) == pytest.approx([0.0], abs=1e-8)


def test_original_repo_scenarios_taylor_with_correct_sensitivities_is_near_exact(bond, market):
    s = bumped_rf_sensitivities(bond, market, RF_IDS)
    sc = shocks(parallel(10), parallel(25), parallel(-15), parallel(-30))
    fr = full_revaluation_pnl(bond, market, sc)
    tay, _, _ = taylor_pnl(s.delta_per_bp, sc.matrix(RF_IDS), s.gamma_per_bp2, sc.matrix(RF_IDS))
    # diagonal gamma misses only the small cross-gamma between neighbouring nodes
    assert np.max(np.abs(tay - fr)) < 50.0


def test_grid_exact_at_nodes_and_first_order_exact(bond, market):
    rf = bond.risk_factor_ids(market)
    s = bumped_rf_sensitivities(bond, market, rf)
    grid = build_parallel_grid(bond, market, rf, s.delta_per_bp.to_numpy(), NODES)
    assert grid.coord_weights.sum() == pytest.approx(1.0)
    sc = shocks(parallel(-30), parallel(50))
    pnl, coord, flag = grid.apply(sc)
    assert coord == pytest.approx([-30, 50])
    assert pnl == pytest.approx(full_revaluation_pnl(bond, market, sc), abs=1e-6)
    assert not flag.any()
    # tiny non-parallel move: the coordinate preserves sum(delta * shock); the remaining gap is
    # the chord (linear interpolation) error between the 0 and 5bp nodes, ~ gamma * 5bp / 2
    tiny = shocks(np.linspace(0.01, 0.05, len(RF_IDS)))
    pnl_tiny, coord, _ = grid.apply(tiny)
    first_order = tiny.matrix(rf) @ s.delta_per_bp.to_numpy()
    assert coord * s.delta_per_bp.sum() == pytest.approx(first_order, rel=1e-12)
    assert pnl_tiny == pytest.approx(first_order, rel=3e-3)


def test_ladder_exact_for_single_factor_moves(bond, market):
    rf = bond.risk_factor_ids(market)
    ladders = build_ladders(bond, market, rf, NODES)
    row = np.zeros(len(RF_IDS))
    row[RF_IDS.index("IR.USD.TSY.ZERO.5Y")] = 75.0
    sc = shocks(row)
    total, contrib, flag = apply_ladders(ladders, sc)
    assert total == pytest.approx(full_revaluation_pnl(bond, market, sc), abs=1e-6)
    assert contrib.shape == (1, len(rf)) and not flag.any()


def test_extrapolation_is_flagged_or_rejected():
    y, flag = interp_with_flag([0, 10], [0, 100], [5, 20, -10], "LINEAR_FLAG")
    assert y == pytest.approx([50, 200, -100]) and flag.tolist() == [False, True, True]
    y, _ = interp_with_flag([0, 10], [0, 100], [20], "FLAT_FLAG")
    assert y == pytest.approx([100])
    with pytest.raises(ValueError):
        interp_with_flag([0, 10], [0, 100], [20], "ERROR")


def test_beyond_grid_stress_is_flagged(bond, market):
    rf = bond.risk_factor_ids(market)
    s = bumped_rf_sensitivities(bond, market, rf)
    grid = build_parallel_grid(bond, market, rf, s.delta_per_bp.to_numpy(), NODES)
    pnl, _, flag = grid.apply(shocks(parallel(300)))
    fr = full_revaluation_pnl(bond, market, shocks(parallel(300)))
    assert flag.all()
    assert pnl[0] < fr[0]  # linear extrapolation of a convex profile overstates the loss


def test_vertex_basis_mapping_error_for_kinked_shock(long_bond, market):
    """Taylor on FRTB vertices (no 7Y vertex) is exact for shocks linear between 5Y and 10Y but
    not for a shock that kinks at the 7Y curve pillar: a real sensitivity-mapping effect."""
    from projection_engine.frtb import GIRR_VERTEX_LABELS
    from projection_engine.methods import frtb_vertex_deltas

    v = frtb_vertex_deltas(long_bond, market, "USD_TSY", GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS)
    rf_tenors = np.array([0.0833333, 0.25, 0.5, 1, 2, 3, 5, 7, 10, 20, 30])
    map_w = linear_interp_weights(rf_tenors, list(GIRR_VERTICES.values())).T  # vertex x rf

    def first_order_taylor(row):
        return float(v["delta_per_bp"].to_numpy() @ (map_w @ np.asarray(row)))

    linear = np.interp(rf_tenors, [5, 10], [10, 20])
    kinked = linear.copy()
    kinked[TENORS.index("7Y")] += 5.0
    for row, should_match in ((linear, True), (kinked, False)):
        fr = full_revaluation_pnl(long_bond, market, shocks(row))[0]
        err = abs(first_order_taylor(row) - fr)
        if should_match:
            assert err < 0.01 * abs(fr)
        else:
            assert err > 1_000  # ~5bp x 7Y exposure is missed
