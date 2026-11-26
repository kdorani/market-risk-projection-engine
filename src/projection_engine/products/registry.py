"""Product registry: maps source product types to instrument builders."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date, datetime

import pandas as pd

from .base import Instrument, PricerNotImplemented
from .fixed_rate_bond import FixedRateBond


def _as_date(value) -> date:
    """Accept date, datetime, pandas/numpy timestamps or ISO strings (Arrow may give any)."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _build_fixed_rate_bond(row: Mapping) -> FixedRateBond:
    return FixedRateBond(
        position_id=row["position_id"],
        trade_id=row["trade_id"],
        instrument_id=row["instrument_id"],
        currency=row["currency"],
        notional=float(row["notional"]),
        coupon_rate=float(row["coupon_rate"]),
        coupon_frequency=int(row["coupon_frequency"]),
        issue_date=_as_date(row["issue_date"]),
        maturity_date=_as_date(row["maturity_date"]),
        day_count=row["day_count"],
        business_day_convention=row["business_day_convention"],
        calendar_id=row["calendar_id"],
        discount_curve_id=row["discount_curve_id"],
    )


IMPLEMENTED: dict[str, Callable[[Mapping], Instrument]] = {
    "FIXED_RATE_BOND": _build_fixed_rate_bond,
}

# Product types the engine is designed for but whose pricers are not written yet.
PLANNED = {
    "FLOATING_RATE_BOND": "projection + discount curves, reset schedule, current fixing",
    "INTEREST_RATE_SWAP": "fixed leg + floating leg, multi-curve (OIS discount, index projection)",
    "FX_FORWARD": "two discount curves + FX spot, relative FX shocks",
    "EQUITY": "spot x quantity, relative equity shocks",
    "EQUITY_OPTION": "Black-Scholes, spot x vol grid, relative spot + absolute vol shocks",
}

SUPPORTED_PRODUCT_TYPES = set(IMPLEMENTED) | set(PLANNED)


def build_instrument(row: Mapping) -> Instrument:
    product_type = row["product_type"]
    if product_type in IMPLEMENTED:
        return IMPLEMENTED[product_type](row)
    if product_type in PLANNED:
        raise PricerNotImplemented(f"{product_type}: pricer planned ({PLANNED[product_type]})")
    raise ValueError(f"Unknown product type {product_type!r}")
