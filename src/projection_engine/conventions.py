"""Date conventions: holiday calendars, business-day adjustment, day counts, schedules.

These are the "boring" details that cause most real-world pricing breaks, so they are
implemented explicitly rather than hidden behind integer year counts.
"""

from __future__ import annotations

import calendar as _calendar
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Canonical names
# ---------------------------------------------------------------------------
_DAY_COUNT_ALIASES = {
    "ACT/365F": "ACT/365F",
    "ACT/365": "ACT/365F",
    "ACT/365FIXED": "ACT/365F",
    "ACT/360": "ACT/360",
    "30/360": "30/360",
    "30/360US": "30/360",
    "30U/360": "30/360",
    "BOND": "30/360",
    "ACT/ACT_ICMA": "ACT/ACT_ICMA",
    "ACT/ACTICMA": "ACT/ACT_ICMA",
    "ACT/ACT(ICMA)": "ACT/ACT_ICMA",
    "ACT/ACT-ICMA": "ACT/ACT_ICMA",
}

BUSINESS_DAY_CONVENTIONS = {"UNADJUSTED", "FOLLOWING", "MODIFIED_FOLLOWING", "PRECEDING"}


def normalize_day_count(day_count: str) -> str:
    key = day_count.upper().replace(" ", "")
    if key not in _DAY_COUNT_ALIASES:
        raise ValueError(f"Unsupported day count convention: {day_count!r}")
    return _DAY_COUNT_ALIASES[key]


# ---------------------------------------------------------------------------
# Month arithmetic
# ---------------------------------------------------------------------------
def is_month_end(d: date) -> bool:
    return d.day == _calendar.monthrange(d.year, d.month)[1]


def add_months(d: date, months: int, end_of_month: bool = False) -> date:
    """Add calendar months, clipping the day to the month length (or forcing month end)."""
    year, month0 = divmod(d.month - 1 + months, 12)
    year += d.year
    month = month0 + 1
    last = _calendar.monthrange(year, month)[1]
    return date(year, month, last if end_of_month else min(d.day, last))


# ---------------------------------------------------------------------------
# Holiday calendar
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HolidayCalendar:
    """Weekends plus an explicit holiday list (sourced from the Reference database)."""

    calendar_id: str
    holidays: frozenset[date] = field(default_factory=frozenset)

    @classmethod
    def from_dates(cls, calendar_id: str, dates: Iterable[date]) -> HolidayCalendar:
        return cls(calendar_id, frozenset(dates))

    def is_business_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.holidays

    def adjust(self, d: date, convention: str) -> date:
        conv = convention.upper()
        if conv == "UNADJUSTED":
            return d
        if conv == "FOLLOWING":
            return self._roll(d, +1)
        if conv == "PRECEDING":
            return self._roll(d, -1)
        if conv == "MODIFIED_FOLLOWING":
            following = self._roll(d, +1)
            return following if following.month == d.month else self._roll(d, -1)
        raise ValueError(f"Unsupported business-day convention: {convention!r}")

    def _roll(self, d: date, step: int) -> date:
        while not self.is_business_day(d):
            d += timedelta(days=step)
        return d

    def add_business_days(self, d: date, n: int) -> date:
        step = 1 if n >= 0 else -1
        remaining = abs(n)
        while remaining:
            d += timedelta(days=step)
            if self.is_business_day(d):
                remaining -= 1
        return d

    def business_days(self, start: date, end: date) -> list[date]:
        """Business days in [start, end]."""
        out, d = [], start
        while d <= end:
            if self.is_business_day(d):
                out.append(d)
            d += timedelta(days=1)
        return out


# ---------------------------------------------------------------------------
# Day counts
# ---------------------------------------------------------------------------
def year_fraction(
    start: date,
    end: date,
    day_count: str,
    *,
    period_start: date | None = None,
    period_end: date | None = None,
    frequency: int | None = None,
) -> float:
    """Accrual year fraction between two dates.

    ACT/ACT_ICMA needs the reference coupon period and frequency:
    yf = days(start, end) / (frequency * days(period_start, period_end)).
    """
    dc = normalize_day_count(day_count)
    days = (end - start).days
    if dc == "ACT/365F":
        return days / 365.0
    if dc == "ACT/360":
        return days / 360.0
    if dc == "30/360":
        d1, d2 = min(start.day, 30), end.day
        if d1 == 30:
            d2 = min(d2, 30)
        return (360 * (end.year - start.year) + 30 * (end.month - start.month) + (d2 - d1)) / 360.0
    if dc == "ACT/ACT_ICMA":
        if period_start is None or period_end is None or not frequency:
            raise ValueError("ACT/ACT_ICMA requires period_start, period_end and frequency")
        return days / (frequency * (period_end - period_start).days)
    raise AssertionError(dc)  # pragma: no cover


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------
def generate_periods(
    issue_date: date, maturity_date: date, frequency: int, end_of_month: bool | None = None
) -> list[tuple[date, date]]:
    """Unadjusted accrual periods generated backward from maturity.

    The first period may be a short stub starting at the issue date. If ``end_of_month`` is
    None it is inferred from the maturity date (standard bond EOM rule).
    """
    if frequency not in (1, 2, 4, 12):
        raise ValueError(f"Unsupported coupon frequency: {frequency}")
    if maturity_date <= issue_date:
        raise ValueError("maturity_date must be after issue_date")
    eom = is_month_end(maturity_date) if end_of_month is None else end_of_month
    step = 12 // frequency
    dates = [maturity_date]
    k = 1
    while True:
        d = add_months(maturity_date, -step * k, end_of_month=eom)
        if d <= issue_date:
            break
        dates.append(d)
        k += 1
    dates.append(issue_date)
    dates.sort()
    return list(zip(dates[:-1], dates[1:], strict=True))
