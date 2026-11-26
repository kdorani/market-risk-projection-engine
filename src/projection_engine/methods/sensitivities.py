"""Bump-and-revalue sensitivities computed by the engine itself.

Used for (a) reconciling the FRTB / front-office sensitivity feeds, and (b) the delta
weights that define the grid coordinate. Canonical conventions:

* delta_per_bp  = dPV for a +1bp shift  (negative for a long bond)
* gamma_per_bp2 = d2PV / d(bp)^2
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from ..curves import MarketState, ScenarioShocks
from ..products.base import Instrument


def bumped_rf_sensitivities(
    instrument: Instrument, market: MarketState, rf_ids: list[str], bump_bp: float = 1.0
) -> pd.DataFrame:
    """Central-difference delta and diagonal gamma for each scenario risk factor."""
    n = len(rf_ids)
    rows = np.zeros((2 * n, n))
    for i in range(n):
        rows[2 * i, i] = +bump_bp
        rows[2 * i + 1, i] = -bump_bp
    pv = instrument.revalue(market, ScenarioShocks(rf_ids, rows))
    base = instrument.present_value(market)
    up, down = pv[0::2], pv[1::2]
    return pd.DataFrame(
        {
            "rf_id": rf_ids,
            "delta_per_bp": (up - down) / (2.0 * bump_bp),
            "gamma_per_bp2": (up - 2.0 * base + down) / bump_bp**2,
        }
    )


def frtb_vertex_deltas(
    instrument, market: MarketState, curve_id: str, vertex_labels: list[str],
    vertex_tenors: np.ndarray, bump_bp: float = 1.0,
) -> pd.DataFrame:
    """FRTB GIRR-style delta: forward 1bp bump of each vertex (triangular basis on the
    vertex grid), reported per bp. FRTB itself divides by 0.0001 (per unit of rate);
    the unit conversion happens in the sensitivity canonicalisation step.
    """
    if not hasattr(instrument, "pv_with_basis_shifts"):
        return pd.DataFrame(columns=["curve_id", "vertex", "delta_per_bp"])
    n = len(vertex_tenors)
    shifts = np.vstack([np.zeros(n), np.eye(n) * bump_bp])
    pv = instrument.pv_with_basis_shifts(market, np.asarray(vertex_tenors), shifts)
    return pd.DataFrame(
        {
            "curve_id": curve_id,
            "vertex": vertex_labels,
            "delta_per_bp": (pv[1:] - pv[0]) / bump_bp,
        }
    )
