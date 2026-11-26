"""Fixed-coupon bullet bond priced by discounting contractual cash flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import ClassVar

import numpy as np
import pandas as pd

from ..conventions import (
    add_months,
    generate_periods,
    is_month_end,
    normalize_day_count,
    year_fraction,
)
from ..curves import BP, MarketState, ScenarioShocks, linear_interp_weights
from .base import Instrument


@dataclass
class FixedRateBond(Instrument):
    product_type: ClassVar[str] = "FIXED_RATE_BOND"

    position_id: str
    trade_id: str
    instrument_id: str
    currency: str
    notional: float  # signed face amount: + long, - short
    coupon_rate: float  # decimal, e.g. 0.06
    coupon_frequency: int
    issue_date: date
    maturity_date: date
    day_count: str
    business_day_convention: str
    calendar_id: str
    discount_curve_id: str
    _flow_cache: dict = field(default_factory=dict, repr=False, compare=False)

    def __post_init__(self) -> None:
        self.day_count = normalize_day_count(self.day_count)

    # ------------------------------------------------------------------ schedule
    def cashflows(self, market: MarketState) -> pd.DataFrame:
        """Future cash flows (pay date strictly after the valuation date) with PV detail."""
        key = market.valuation_date
        if key not in self._flow_cache:
            self._flow_cache[key] = self._build_cashflows(market)
        flows = self._flow_cache[key].copy()
        curve = market.curve(self.discount_curve_id)
        t = flows["t_years"].to_numpy()
        flows["zero_rate"] = curve.zero_rate(t)
        flows["discount_factor"] = curve.discount_factor(t)
        flows["pv"] = flows["amount"] * flows["discount_factor"]
        return flows

    def _build_cashflows(self, market: MarketState) -> pd.DataFrame:
        vd = market.valuation_date
        cal = market.calendar(self.calendar_id)
        periods = generate_periods(self.issue_date, self.maturity_date, self.coupon_frequency)
        rows = []
        for start, end in periods:
            pay = cal.adjust(end, self.business_day_convention)
            if pay <= vd:
                continue
            accrual = self._accrual_factor(start, end, end)
            coupon = self.notional * self.coupon_rate * accrual
            principal = self.notional if end == self.maturity_date else 0.0
            rows.append(
                {
                    "period_start": start,
                    "period_end": end,
                    "pay_date": pay,
                    "accrual_factor": accrual,
                    "coupon": coupon,
                    "principal": principal,
                    "amount": coupon + principal,
                    "t_years": (pay - vd).days / 365.0,  # curve time basis ACT/365F
                }
            )
        if not rows:
            raise ValueError(f"{self.position_id}: bond has no future cash flows at {vd}")
        return pd.DataFrame(rows)

    def _accrual_factor(self, start: date, end: date, period_end: date) -> float:
        """Accrual from ``start`` to ``end`` inside the period ending ``period_end``.

        ACT/ACT_ICMA uses the regular (notional) reference period ending on ``period_end``,
        which also handles a short first stub correctly.
        """
        if self.day_count == "ACT/ACT_ICMA":
            ref_start = add_months(
                period_end, -12 // self.coupon_frequency, is_month_end(self.maturity_date)
            )
            return year_fraction(
                start, end, self.day_count,
                period_start=ref_start, period_end=period_end, frequency=self.coupon_frequency,
            )
        return year_fraction(start, end, self.day_count)

    def accrued_interest(self, valuation_date: date) -> float:
        for start, end in generate_periods(self.issue_date, self.maturity_date, self.coupon_frequency):
            if start <= valuation_date < end:
                frac = self._accrual_factor(start, valuation_date, end)
                return self.notional * self.coupon_rate * frac
        return 0.0

    # ------------------------------------------------------------------ pricing
    def _times_amounts(self, market: MarketState) -> tuple[np.ndarray, np.ndarray]:
        if market.valuation_date not in self._flow_cache:
            self._flow_cache[market.valuation_date] = self._build_cashflows(market)
        flows = self._flow_cache[market.valuation_date]
        return flows["t_years"].to_numpy(), flows["amount"].to_numpy()

    def pv_with_basis_shifts(
        self, market: MarketState, basis_tenors: np.ndarray, shifts_bp: np.ndarray
    ) -> np.ndarray:
        """PV when the zero curve is shifted by ``shifts_bp`` given on ``basis_tenors``.

        ``shifts_bp`` has shape (n_scenarios, n_basis). The shift at each cash-flow time is
        linearly interpolated from the basis (flat outside), so a shift on one basis tenor
        is a triangular "key-rate" bump. All scenarios are priced in one vectorised step.
        """
        t, cf = self._times_amounts(market)
        curve = market.curve(self.discount_curve_id)
        r = curve.zero_rate(t)  # (n_flows,)
        w = linear_interp_weights(basis_tenors, t)  # (n_basis, n_flows)
        shift = np.atleast_2d(shifts_bp) @ w * BP  # (n_scenarios, n_flows)
        return (cf * np.exp(-(r + shift) * t)).sum(axis=1)

    def present_value(self, market: MarketState) -> float:
        t, cf = self._times_amounts(market)
        return float((cf * market.curve(self.discount_curve_id).discount_factor(t)).sum())

    def revalue(self, market: MarketState, shocks: ScenarioShocks) -> np.ndarray:
        curve = market.curve(self.discount_curve_id)
        node_shocks = market.node_shock_matrix(self.discount_curve_id, shocks)
        return self.pv_with_basis_shifts(market, curve.tenors, node_shocks)

    def risk_factor_ids(self, market: MarketState) -> list[str]:
        t, _ = self._times_amounts(market)
        curve = market.curve(self.discount_curve_id)
        node_used = linear_interp_weights(curve.tenors, t).sum(axis=1) > 0
        rf_ids, weights = market.curve_node_rf[self.discount_curve_id]
        rf_used = (weights[node_used] != 0).any(axis=0)
        return [rf for rf, used in zip(rf_ids, rf_used, strict=True) if used]

    def valuation_details(self, market: MarketState) -> dict:
        dirty = self.present_value(market)
        accrued = self.accrued_interest(market.valuation_date)
        clean = dirty - accrued
        t, cf = self._times_amounts(market)
        df = market.curve(self.discount_curve_id).discount_factor(t)
        macaulay = float((t * cf * df).sum() / dirty) if dirty else float("nan")
        return {
            "dirty_pv": dirty,
            "accrued_interest": accrued,
            "clean_pv": clean,
            "clean_price_per_100": 100.0 * clean / self.notional,
            "dirty_price_per_100": 100.0 * dirty / self.notional,
            "macaulay_duration_years": macaulay,
            "n_future_cashflows": int(len(t)),
            "final_maturity_years": float(t.max()),
        }
