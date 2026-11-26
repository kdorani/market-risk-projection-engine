import numpy as np
import pytest

from projection_engine.frtb import SENSITIVITY_CONVENTIONS, tenor_label_to_years
from projection_engine.risk_measures import (
    expected_shortfall,
    ks_statistic,
    plat_style_zone,
    spearman,
    value_at_risk,
    var_scenario_index,
)


def test_kth_worst_var_conventions():
    pnl = -np.arange(1, 261, dtype=float)  # losses 1..260
    assert value_at_risk(pnl, 0.99) == 258.0  # 3rd worst of 260
    assert value_at_risk(-np.arange(1, 101, dtype=float), 0.99) == 100.0  # 1st worst of 100 (float guard)
    assert value_at_risk(-np.arange(1, 251, dtype=float), 0.99) == 248.0  # 3rd worst of 250
    assert pnl[var_scenario_index(pnl, 0.99)] == -258.0


def test_numpy_linear_and_es():
    pnl = np.linspace(-100, 100, 201)
    assert value_at_risk(pnl, 0.99, "NUMPY_LINEAR") == pytest.approx(98.0)
    assert expected_shortfall(pnl, 0.975) == pytest.approx(np.mean([100, 99, 98, 97, 96, 95]))
    with pytest.raises(ValueError):
        value_at_risk(pnl, 0.99, "MAGIC")


def test_comparison_statistics():
    a = np.random.default_rng(1).normal(size=500)
    assert spearman(a, 3 * a + 1) == pytest.approx(1.0)
    assert ks_statistic(a, a) == 0.0
    assert ks_statistic(a, a + 10) == pytest.approx(1.0)
    assert plat_style_zone(0.95, 0.05) == "GREEN"
    assert plat_style_zone(0.75, 0.10) == "AMBER"
    assert plat_style_zone(0.65, 0.05) == "RED"


def test_tenor_labels_and_unit_conventions():
    assert tenor_label_to_years("6M") == 0.5
    assert tenor_label_to_years("0.25Y") == 0.25
    # FRTB GIRR delta is per unit of rate: -44,745,902 per 1.00 -> -4,474.59 per bp
    measure, factor = SENSITIVITY_CONVENTIONS[("FRTB", "GIRR_DELTA", "USD_PER_UNIT_RATE")]
    assert measure == "DELTA" and -44_745_902.35 * factor == pytest.approx(-4474.590235)
    # FO DV01 is quoted positive for a loss -> canonical delta is negative
    measure, factor = SENSITIVITY_CONVENTIONS[("FO", "DV01", "USD_PER_BP_LOSS_POSITIVE")]
    assert 4475.68 * factor == -4475.68


def test_spearman_handles_ties_and_matches_definition():
    a = np.array([1.0, 2.0, 2.0, 3.0, 5.0])
    b = np.array([10.0, 20.0, 20.0, 25.0, 4.0])
    ra, rb = np.array([1, 2.5, 2.5, 4, 5]), np.array([2, 3.5, 3.5, 5, 1])
    assert spearman(a, b) == pytest.approx(np.corrcoef(ra, rb)[0, 1])
