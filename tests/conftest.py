from __future__ import annotations

from datetime import date

import numpy as np
import pytest

from projection_engine.conventions import HolidayCalendar
from projection_engine.curves import MarketState, ZeroCurve
from projection_engine.frtb import tenor_label_to_years
from projection_engine.products import FixedRateBond

TENORS = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
ZEROS = [0.0395, 0.0392, 0.0385, 0.0372, 0.0358, 0.0355, 0.0362, 0.0376, 0.0395, 0.0440, 0.0448]
RF_IDS = [f"IR.USD.TSY.ZERO.{t}" for t in TENORS]
VAL_DATE = date(2026, 11, 24)


def make_market(zeros=ZEROS, holidays=(date(2027, 2, 15),)) -> MarketState:
    tenors = np.array([tenor_label_to_years(t) for t in TENORS])
    curve = ZeroCurve("USD_TSY", "USD", tuple(TENORS), tenors, np.asarray(zeros, float))
    cal = HolidayCalendar.from_dates("USNY", holidays)
    return MarketState(VAL_DATE, {"USD_TSY": curve}, {"USNY": cal}, {"USD_TSY": (RF_IDS, np.eye(len(RF_IDS)))})


def make_bond(**overrides) -> FixedRateBond:
    kw = dict(
        position_id="POS-0001", trade_id="TRD-BOND-0001", instrument_id="DEMO-BOND-6.00-2031",
        currency="USD", notional=10_000_000.0, coupon_rate=0.06, coupon_frequency=1,
        issue_date=date(2021, 11, 15), maturity_date=date(2031, 11, 15), day_count="ACT/ACT_ICMA",
        business_day_convention="FOLLOWING", calendar_id="USNY", discount_curve_id="USD_TSY",
    )
    kw.update(overrides)
    return FixedRateBond(**kw)


@pytest.fixture
def market() -> MarketState:
    return make_market()


@pytest.fixture
def bond() -> FixedRateBond:
    return make_bond()


@pytest.fixture
def long_bond() -> FixedRateBond:
    return make_bond(position_id="POS-0003", trade_id="TRD-BOND-0003", instrument_id="DEMO-BOND-4.25-2036",
                     notional=5_000_000.0, coupon_rate=0.0425, coupon_frequency=2,
                     issue_date=date(2026, 8, 15), maturity_date=date(2036, 8, 15))
