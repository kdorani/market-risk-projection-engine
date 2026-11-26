"""Instrument interface shared by all products.

The four PnL methods only use ``present_value`` and ``revalue``. Grids, ladders and bumped
sensitivities are all produced by calling ``revalue`` with synthetic shock sets, so a new
product only has to implement pricing; the methods come for free.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

import numpy as np

from ..curves import MarketState, ScenarioShocks


class Instrument(ABC):
    product_type: ClassVar[str]

    position_id: str
    trade_id: str
    currency: str

    @abstractmethod
    def present_value(self, market: MarketState) -> float:
        """Base PV (dirty, position-level, in position currency)."""

    @abstractmethod
    def revalue(self, market: MarketState, shocks: ScenarioShocks) -> np.ndarray:
        """PV under each scenario row of ``shocks`` (full revaluation)."""

    @abstractmethod
    def risk_factor_ids(self, market: MarketState) -> list[str]:
        """Scenario risk factors this instrument has non-zero exposure to."""

    def valuation_details(self, market: MarketState) -> dict:
        """Optional extra valuation fields for reporting (clean price, accrued, ...)."""
        return {}


class PricerNotImplemented(NotImplementedError):
    """Raised for product types that are planned but not yet implemented."""
