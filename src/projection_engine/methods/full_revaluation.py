"""Method 1: full revaluation. Reprice the instrument under every scenario."""

from __future__ import annotations

import numpy as np

from ..curves import MarketState, ScenarioShocks
from ..products.base import Instrument


def full_revaluation_pnl(
    instrument: Instrument, market: MarketState, shocks: ScenarioShocks
) -> np.ndarray:
    """PnL_s = PV(shocked market_s) - PV(base market). Benchmark for the other methods."""
    base_pv = instrument.present_value(market)
    return instrument.revalue(market, shocks) - base_pv
