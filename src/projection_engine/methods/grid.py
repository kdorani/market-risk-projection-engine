"""Method 3: grid-based (pre-computed revaluation grid + interpolation).

For a rates product the grid axis is a parallel shift of all the risk factors the position
is exposed to. Each grid node is a full revaluation, done once per day. A scenario is mapped
to a single grid coordinate, the DV01-weighted average shock

    x_s = sum_i w_i * shock_{s,i},   w_i = delta_i / sum_j delta_j

which preserves the first-order (delta) PnL by construction and captures parallel convexity
from the revaluations, but compresses curve-shape information into one number. Linear
interpolation between nodes adds a chord error of about gamma * h^2 / 8 for node spacing h. Equity / FX options will use
multi-dimensional grids (spot x vol) through the same structures.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ..curves import MarketState, ScenarioShocks
from ..products.base import Instrument
from .interpolation import interp_with_flag


@dataclass
class RevaluationGrid:
    position_id: str
    axis: str
    nodes: np.ndarray  # grid coordinates (bp)
    pnl: np.ndarray  # full-revaluation PnL at each node
    coord_rf_ids: list[str]  # risk factors that define the coordinate
    coord_weights: np.ndarray  # weights w_i (sum to 1)

    def coordinate(self, shocks: ScenarioShocks) -> np.ndarray:
        return shocks.matrix(self.coord_rf_ids) @ self.coord_weights

    def apply(self, shocks: ScenarioShocks, policy: str = "LINEAR_FLAG"):
        """Return (pnl, coordinate, extrapolated_mask) for every scenario."""
        x = self.coordinate(shocks)
        y, flag = interp_with_flag(self.nodes, self.pnl, x, policy)
        return y, x, flag


def coordinate_weights(deltas: np.ndarray) -> np.ndarray:
    """Delta weights; falls back to equal weights for a (near) delta-neutral position."""
    deltas = np.asarray(deltas, dtype=float)
    total = deltas.sum()
    if abs(total) < 1e-9 * max(1.0, np.abs(deltas).sum()):
        return np.full(len(deltas), 1.0 / len(deltas))
    return deltas / total


def build_parallel_grid(
    instrument: Instrument,
    market: MarketState,
    rf_ids: list[str],
    rf_deltas: np.ndarray,
    nodes_bp: list[float],
) -> RevaluationGrid:
    nodes = np.asarray(sorted(nodes_bp), dtype=float)
    shocks = ScenarioShocks(rf_ids, np.repeat(nodes[:, None], len(rf_ids), axis=1))
    pnl = instrument.revalue(market, shocks) - instrument.present_value(market)
    return RevaluationGrid(
        position_id=instrument.position_id,
        axis="PARALLEL_SHIFT_BP",
        nodes=nodes,
        pnl=pnl,
        coord_rf_ids=list(rf_ids),
        coord_weights=coordinate_weights(rf_deltas),
    )
