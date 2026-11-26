"""Method 4: ladder-based (one revaluation ladder per risk factor, results added up).

For each risk factor i the position is revalued at a set of shifts of *that factor only*
(all others at base). Scenario PnL = sum_i interp_i(shock_{s,i}).

Captures the full non-linearity of each factor on its own; misses interactions between
factors (cross-gamma), which is the difference to a multi-dimensional grid.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ..curves import MarketState, ScenarioShocks
from ..products.base import Instrument
from .interpolation import interp_with_flag


@dataclass
class RevaluationLadder:
    position_id: str
    rf_id: str
    shifts: np.ndarray  # bp
    pnl: np.ndarray


def build_ladders(
    instrument: Instrument, market: MarketState, rf_ids: list[str], shifts_bp: list[float]
) -> list[RevaluationLadder]:
    shifts = np.asarray(sorted(shifts_bp), dtype=float)
    n_rf, n_sh = len(rf_ids), len(shifts)
    rows = np.zeros((n_rf * n_sh, n_rf))
    for i in range(n_rf):
        rows[i * n_sh:(i + 1) * n_sh, i] = shifts
    pnl = instrument.revalue(market, ScenarioShocks(rf_ids, rows)) - instrument.present_value(market)
    return [
        RevaluationLadder(instrument.position_id, rf, shifts, pnl[i * n_sh:(i + 1) * n_sh])
        for i, rf in enumerate(rf_ids)
    ]


def apply_ladders(
    ladders: list[RevaluationLadder], shocks: ScenarioShocks, policy: str = "LINEAR_FLAG"
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (total_pnl, contributions (n_scen x n_rf), extrapolated_any (n_scen,))."""
    contrib = np.zeros((shocks.n_scenarios, len(ladders)))
    flag = np.zeros(shocks.n_scenarios, dtype=bool)
    for j, lad in enumerate(ladders):
        x = shocks.matrix([lad.rf_id])[:, 0]
        contrib[:, j], f = interp_with_flag(lad.shifts, lad.pnl, x, policy)
        flag |= f
    return contrib.sum(axis=1), contrib, flag
