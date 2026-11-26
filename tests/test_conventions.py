from datetime import date

import pytest

from projection_engine.conventions import (
    HolidayCalendar,
    add_months,
    generate_periods,
    normalize_day_count,
    year_fraction,
)


def test_add_months_clips_and_end_of_month():
    assert add_months(date(2026, 1, 31), 1) == date(2026, 2, 28)
    assert add_months(date(2026, 2, 28), 1, end_of_month=True) == date(2026, 3, 31)
    assert add_months(date(2026, 11, 15), -6) == date(2026, 5, 15)


def test_business_day_adjustment():
    cal = HolidayCalendar.from_dates("USNY", [date(2027, 2, 15)])  # Presidents' Day
    assert cal.adjust(date(2031, 11, 15), "FOLLOWING") == date(2031, 11, 17)  # Sat -> Mon
    assert cal.adjust(date(2027, 2, 15), "FOLLOWING") == date(2027, 2, 16)  # holiday -> Tue
    assert cal.adjust(date(2026, 10, 31), "MODIFIED_FOLLOWING") == date(2026, 10, 30)  # stays in month
    assert cal.adjust(date(2026, 11, 15), "UNADJUSTED") == date(2026, 11, 15)
    with pytest.raises(ValueError):
        cal.adjust(date(2026, 11, 15), "NEAREST")


def test_day_counts():
    assert year_fraction(date(2026, 1, 31), date(2026, 7, 31), "30/360") == pytest.approx(0.5)
    assert year_fraction(date(2026, 1, 1), date(2027, 1, 1), "ACT/365F") == pytest.approx(1.0)
    assert year_fraction(date(2026, 1, 1), date(2026, 7, 1), "ACT/360") == pytest.approx(181 / 360)
    # ACT/ACT ICMA: 101 of 184 days in a semi-annual period
    yf = year_fraction(date(2026, 8, 15), date(2026, 11, 24), "ACT/ACT_ICMA",
                       period_start=date(2026, 8, 15), period_end=date(2027, 2, 15), frequency=2)
    assert yf == pytest.approx(101 / 368)
    with pytest.raises(ValueError):
        normalize_day_count("BUS/252")


def test_regular_and_stub_schedules():
    periods = generate_periods(date(2025, 11, 15), date(2028, 11, 15), 2)
    assert len(periods) == 6
    assert periods[0] == (date(2025, 11, 15), date(2026, 5, 15))
    assert periods[-1][1] == date(2028, 11, 15)
    stub = generate_periods(date(2026, 1, 10), date(2028, 11, 15), 2)
    assert stub[0] == (date(2026, 1, 10), date(2026, 5, 15))  # short first stub
