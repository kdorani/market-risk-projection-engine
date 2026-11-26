"""VaR / ES from PnL vectors and vector-comparison statistics (no scipy needed)."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

QUANTILE_METHODS = {"KTH_WORST", "NUMPY_LINEAR"}


def _k_worst(n: int, tail: float) -> int:
    # round() guards against 260 * 0.01 = 2.6000000000000001 style artefacts
    return max(1, math.ceil(round(n * tail, 9)))


def value_at_risk(pnl: np.ndarray, confidence: float = 0.99, method: str = "KTH_WORST") -> float:
    """Historical-simulation VaR as a positive loss number.

    KTH_WORST: the k-th largest loss with k = ceil(N * (1 - confidence)); e.g. N=260 -> 3rd.
    NUMPY_LINEAR: -quantile(pnl, 1 - confidence) with linear interpolation.
    """
    pnl = np.asarray(pnl, dtype=float)
    if method == "KTH_WORST":
        k = _k_worst(len(pnl), 1.0 - confidence)
        return float(-np.sort(pnl)[k - 1])
    if method == "NUMPY_LINEAR":
        return float(-np.quantile(pnl, 1.0 - confidence, method="linear"))
    raise ValueError(f"Unknown quantile method {method!r}")


def expected_shortfall(pnl: np.ndarray, confidence: float = 0.975) -> float:
    """Average of the ceil(N * (1 - confidence)) worst outcomes, as a positive loss."""
    pnl = np.sort(np.asarray(pnl, dtype=float))
    k = _k_worst(len(pnl), 1.0 - confidence)
    return float(-pnl[:k].mean())


def var_scenario_index(pnl: np.ndarray, confidence: float = 0.99) -> int:
    """Index of the scenario that sets KTH_WORST VaR."""
    pnl = np.asarray(pnl, dtype=float)
    k = _k_worst(len(pnl), 1.0 - confidence)
    return int(np.argsort(pnl, kind="stable")[k - 1])


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation = Pearson correlation of average ranks (ties handled).
    Implemented directly because pandas' method="spearman" silently requires scipy."""
    ra = pd.Series(np.asarray(a, float)).rank(method="average").to_numpy()
    rb = pd.Series(np.asarray(b, float)).rank(method="average").to_numpy()
    if ra.std() == 0 or rb.std() == 0:
        return float("nan")
    return float(np.corrcoef(ra, rb)[0, 1])


def ks_statistic(a: np.ndarray, b: np.ndarray) -> float:
    """Two-sample Kolmogorov-Smirnov statistic: max |ECDF_a - ECDF_b|."""
    a, b = np.sort(np.asarray(a, float)), np.sort(np.asarray(b, float))
    grid = np.concatenate([a, b])
    cdf_a = np.searchsorted(a, grid, side="right") / len(a)
    cdf_b = np.searchsorted(b, grid, side="right") / len(b)
    return float(np.max(np.abs(cdf_a - cdf_b)))


def plat_style_zone(spearman_corr: float, ks: float) -> str:
    """FRTB PLA test thresholds (MAR32), used here only as an analogy for method accuracy."""
    if spearman_corr >= 0.80 and ks <= 0.09:
        return "GREEN"
    if spearman_corr < 0.70 or ks > 0.12:
        return "RED"
    return "AMBER"
