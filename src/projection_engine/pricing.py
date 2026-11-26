import math
from typing import Iterable, Sequence

def fixed_bond_cashflows(notional: float, coupon_rate: float, maturity_years: int,
                         frequency: int = 1) -> list[tuple[float, float]]:
    """Return (time_in_years, cashflow); simplified regular-period schedule."""
    periods = maturity_years * frequency
    coupon = notional * coupon_rate / frequency
    result = []
    for period in range(1, periods + 1):
        t = period / frequency
        cf = coupon + (notional if period == periods else 0.0)
        result.append((t, cf))
    return result

def pv_continuous(cashflows: Sequence[tuple[float, float]], zero_rate: float) -> float:
    """PV using continuous compounding: CF * exp(-r*t)."""
    return sum(cf * math.exp(-zero_rate * t) for t, cf in cashflows)

def full_revaluation_pnl(cashflows, base_rate: float, shock_bp: float) -> tuple[float, float, float]:
    base_pv = pv_continuous(cashflows, base_rate)
    shocked_rate = base_rate + shock_bp / 10000.0
    shocked_pv = pv_continuous(cashflows, shocked_rate)
    return base_pv, shocked_pv, shocked_pv - base_pv

def calculate_dv01(cashflows, base_rate: float, bump_bp: float = 1.0) -> float:
    """Positive DV01 convention: loss for a +1bp yield rise."""
    pv_up = pv_continuous(cashflows, base_rate + bump_bp / 10000.0)
    pv_down = pv_continuous(cashflows, base_rate - bump_bp / 10000.0)
    return (pv_down - pv_up) / (2.0 * bump_bp)

def calculate_convexity(cashflows, base_rate: float, bump_bp: float = 1.0) -> float:
    """Conventional annualized price-yield convexity using decimal yield bump."""
    pv0 = pv_continuous(cashflows, base_rate)
    h = bump_bp / 10000.0
    pv_up = pv_continuous(cashflows, base_rate + h)
    pv_down = pv_continuous(cashflows, base_rate - h)
    return (pv_up + pv_down - 2.0 * pv0) / (pv0 * h * h)

def taylor_pnl(base_pv: float, dv01: float, convexity: float,
               shock_bp: float) -> float:
    """Delta-gamma approximation; DV01 is positive loss per +1bp."""
    shock_decimal = shock_bp / 10000.0
    delta = -dv01 * shock_bp
    gamma = 0.5 * convexity * base_pv * shock_decimal**2
    return delta + gamma

def interpolate_grid(grid: Sequence[tuple[float, float]], target_rate: float) -> float:
    """Linear interpolation; raises if target is outside grid (no silent extrapolation)."""
    ordered = sorted(grid, key=lambda x: x[0])
    if not ordered:
        raise ValueError("Pricing grid is empty")
    if target_rate < ordered[0][0] or target_rate > ordered[-1][0]:
        raise ValueError(
            f"Target rate {target_rate:.6f} outside grid range "
            f"[{ordered[0][0]:.6f}, {ordered[-1][0]:.6f}]"
        )
    for (r0, p0), (r1, p1) in zip(ordered, ordered[1:]):
        if r0 <= target_rate <= r1:
            if target_rate == r0:
                return p0
            if target_rate == r1:
                return p1
            weight = (target_rate - r0) / (r1 - r0)
            return p0 + weight * (p1 - p0)
    return ordered[-1][1]

def grid_pnl(grid, base_rate: float, shock_bp: float, base_pv: float) -> tuple[float, float]:
    shocked_rate = base_rate + shock_bp / 10000.0
    shocked_pv = interpolate_grid(grid, shocked_rate)
    return shocked_pv, shocked_pv - base_pv

def ladder_pnl(key_rate_dv01: Sequence[tuple[int, float]],
               tenor_shocks: Sequence[tuple[int, float]]) -> float:
    """First-order key-rate P&L; DV01 positive loss per +1bp."""
    shock_by_tenor = {tenor: shock for tenor, shock in tenor_shocks}
    return -sum(dv01 * shock_by_tenor.get(tenor, 0.0)
                for tenor, dv01 in key_rate_dv01)
