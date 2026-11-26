"""Curves, scenario shock containers and the market state used by pricers.

Key idea: every rate shift (scenario shock, bump, grid node, ladder step) is expressed as
shocks on a tenor basis and turned into shifts at cash-flow times with one linear
interpolation-weight matrix. Full revaluation of many scenarios is then a matrix product.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import numpy as np
import pandas as pd

from .conventions import HolidayCalendar

BP = 1e-4  # one basis point in decimal


def linear_interp_weights(grid: np.ndarray, x: np.ndarray) -> np.ndarray:
    """Weights W (len(grid) x len(x)) such that f(x) = W.T @ f(grid).

    Linear interpolation inside the grid, flat extrapolation outside.
    """
    grid = np.asarray(grid, dtype=float)
    x = np.atleast_1d(np.asarray(x, dtype=float))
    if grid.ndim != 1 or len(grid) == 0:
        raise ValueError("grid must be a non-empty 1-D array")
    if len(grid) > 1 and np.any(np.diff(grid) <= 0):
        raise ValueError("grid must be strictly increasing")
    w = np.zeros((len(grid), len(x)))
    if len(grid) == 1:
        w[0, :] = 1.0
        return w
    xc = np.clip(x, grid[0], grid[-1])
    right = np.clip(np.searchsorted(grid, xc, side="right"), 1, len(grid) - 1)
    left = right - 1
    frac = (xc - grid[left]) / (grid[right] - grid[left])
    cols = np.arange(len(x))
    w[left, cols] += 1.0 - frac
    w[right, cols] += frac
    return w


@dataclass(frozen=True)
class ZeroCurve:
    """Continuously compounded zero curve, linear interpolation on zero rates."""

    curve_id: str
    currency: str
    tenor_labels: tuple[str, ...]
    tenors: np.ndarray  # years, ACT/365F
    zero_rates: np.ndarray  # decimal
    compounding: str = "CONTINUOUS"
    interpolation: str = "LINEAR_ZERO"

    def __post_init__(self) -> None:
        if len(self.tenors) != len(self.zero_rates) or len(self.tenors) != len(self.tenor_labels):
            raise ValueError(f"{self.curve_id}: tenor/rate/label lengths differ")
        if np.any(np.diff(self.tenors) <= 0):
            raise ValueError(f"{self.curve_id}: tenors must be strictly increasing")
        if self.compounding != "CONTINUOUS":
            raise ValueError(f"{self.curve_id}: only CONTINUOUS compounding is implemented")
        if self.interpolation != "LINEAR_ZERO":
            raise ValueError(f"{self.curve_id}: only LINEAR_ZERO interpolation is implemented")

    def zero_rate(self, t: np.ndarray) -> np.ndarray:
        return linear_interp_weights(self.tenors, t).T @ self.zero_rates

    def discount_factor(self, t: np.ndarray) -> np.ndarray:
        t = np.atleast_1d(np.asarray(t, dtype=float))
        return np.exp(-self.zero_rate(t) * t)


@dataclass
class ScenarioShocks:
    """Scenario x risk-factor shock matrix in canonical units (bp for absolute rate shocks)."""

    rf_ids: list[str]
    values: np.ndarray  # shape (n_scenarios, n_rf)
    index: pd.DataFrame | None = None  # scenario_set, scenario_id, scenario_date per row

    def __post_init__(self) -> None:
        self.values = np.asarray(self.values, dtype=float)
        if self.values.ndim != 2 or self.values.shape[1] != len(self.rf_ids):
            raise ValueError("values must be (n_scenarios, len(rf_ids))")
        self._pos = {rf: i for i, rf in enumerate(self.rf_ids)}

    @property
    def n_scenarios(self) -> int:
        return self.values.shape[0]

    def matrix(self, rf_ids: list[str]) -> np.ndarray:
        """Columns for ``rf_ids``; risk factors not in the scenario set get a zero shock."""
        out = np.zeros((self.n_scenarios, len(rf_ids)))
        for j, rf in enumerate(rf_ids):
            i = self._pos.get(rf)
            if i is not None:
                out[:, j] = self.values[:, i]
        return out

    def subset(self, mask: np.ndarray) -> ScenarioShocks:
        idx = None if self.index is None else self.index[mask].reset_index(drop=True)
        return ScenarioShocks(self.rf_ids, self.values[mask], idx)

    @classmethod
    def from_long(cls, df: pd.DataFrame, value_col: str = "shock_value_canonical") -> ScenarioShocks:
        """Build from long format (scenario_set, scenario_id, scenario_date, rf_id, value)."""
        keys = ["scenario_set", "scenario_id", "scenario_date"]
        wide = (
            df.pivot_table(index=keys, columns="rf_id", values=value_col, aggfunc="sum")
            .sort_index()
            .fillna(0.0)
        )
        index = wide.index.to_frame(index=False)
        return cls(list(wide.columns), wide.to_numpy(), index)


@dataclass
class MarketState:
    """Everything a pricer needs: valuation date, curves, calendars and node -> RF mapping."""

    valuation_date: date
    curves: dict[str, ZeroCurve]
    calendars: dict[str, HolidayCalendar]
    # For each curve: (scenario rf_ids, weight matrix n_nodes x n_rf). Usually identity.
    curve_node_rf: dict[str, tuple[list[str], np.ndarray]] = field(default_factory=dict)

    def curve(self, curve_id: str) -> ZeroCurve:
        try:
            return self.curves[curve_id]
        except KeyError as exc:
            raise KeyError(f"Curve {curve_id!r} not available in market state") from exc

    def calendar(self, calendar_id: str) -> HolidayCalendar:
        try:
            return self.calendars[calendar_id]
        except KeyError as exc:
            raise KeyError(f"Holiday calendar {calendar_id!r} not available") from exc

    def node_rf_ids(self, curve_id: str) -> list[str]:
        return self.curve_node_rf[curve_id][0]

    def node_shock_matrix(self, curve_id: str, shocks: ScenarioShocks) -> np.ndarray:
        """Scenario shocks at the curve's nodes, shape (n_scenarios, n_nodes)."""
        rf_ids, weights = self.curve_node_rf[curve_id]
        return shocks.matrix(rf_ids) @ weights.T
