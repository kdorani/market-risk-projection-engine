"""Generate the synthetic source extracts under data/source/.

Everything is deterministic (fixed seed) so the sample run and the regression test are
reproducible. FRTB and FO sensitivities are produced with the engine's own pricer,
using each system's convention (FRTB forward bump per unit of rate vs FO central DV01), so
the reconciliation step has something realistic to compare. One FO value is deliberately
made stale to show a reconciliation break.

Run from the project root:  python scripts/generate_sample_data.py
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from projection_engine.conventions import HolidayCalendar  # noqa: E402
from projection_engine.curves import MarketState, ZeroCurve  # noqa: E402
from projection_engine.frtb import (  # noqa: E402
    GIRR_VERTEX_LABELS,
    GIRR_VERTEX_TENORS,
    tenor_label_to_years,
)
from projection_engine.methods import bumped_rf_sensitivities, frtb_vertex_deltas  # noqa: E402
from projection_engine.products import build_instrument  # noqa: E402

BUSINESS_DATE = date(2026, 11, 24)
SEED = 20261124
SRC = ROOT / "data" / "source"
CURVE_ID = "USD_TSY"
CALENDAR_ID = "USNY"
TENORS = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
# Synthetic but plausible continuously-compounded USD Treasury zero curve (decimal)
BASE_ZERO = [0.0395, 0.0392, 0.0385, 0.0372, 0.0358, 0.0355, 0.0362, 0.0376, 0.0395, 0.0440, 0.0448]


def rf_id(label: str) -> str:
    return f"IR.USD.TSY.ZERO.{label}"


# ---------------------------------------------------------------------------
# Reference: holiday calendar (US federal holidays, weekend-observed)
# ---------------------------------------------------------------------------
def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    d = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def _observed(d: date) -> date:
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def us_holidays(year: int) -> list[tuple[date, str]]:
    mon, thu = 0, 3
    return [
        (_observed(date(year, 1, 1)), "New Year's Day"),
        (_nth_weekday(year, 1, mon, 3), "Martin Luther King Jr. Day"),
        (_nth_weekday(year, 2, mon, 3), "Presidents' Day"),
        (_last_weekday(year, 5, mon), "Memorial Day"),
        (_observed(date(year, 6, 19)), "Juneteenth"),
        (_observed(date(year, 7, 4)), "Independence Day"),
        (_nth_weekday(year, 9, mon, 1), "Labor Day"),
        (_nth_weekday(year, 10, mon, 2), "Columbus Day"),
        (_observed(date(year, 11, 11)), "Veterans Day"),
        (_nth_weekday(year, 11, thu, 4), "Thanksgiving Day"),
        (_observed(date(year, 12, 25)), "Christmas Day"),
    ]


# ---------------------------------------------------------------------------
# Shocks: historical-style scenario shocks from a level / slope / curvature model
# ---------------------------------------------------------------------------
def _loadings(tenors_y: np.ndarray) -> np.ndarray:
    level = np.array([0.35, 0.55, 0.75, 0.90, 1.00, 1.00, 1.00, 0.97, 0.93, 0.88, 0.86])
    x = np.log(tenors_y)
    slope = 2.0 * (x - np.log(3.0)) / (np.log(30.0) - np.log(1 / 12))
    curvature = np.exp(-((x - np.log(3.0)) ** 2) / (2 * 0.8**2)) - 0.4
    return np.vstack([level, slope, curvature])  # (3, n_tenor)


def _student_t(rng: np.random.Generator, df: float, size) -> np.ndarray:
    return rng.standard_t(df, size=size) / np.sqrt(df / (df - 2.0))  # unit variance


def historical_shocks(rng, n_days, vols, idio, df, drift_level=0.0, jumps=None) -> np.ndarray:
    tenors_y = np.array([tenor_label_to_years(t) for t in TENORS])
    load = _loadings(tenors_y)
    factors = _student_t(rng, df, (n_days, 3)) * np.asarray(vols)
    factors[:, 0] += drift_level
    for day, size in (jumps or {}).items():
        factors[day, 0] += size
    noise = _student_t(rng, df, (n_days, len(TENORS))) * idio
    return np.round(factors @ load + noise, 4)  # bp


def hypothetical_shocks() -> list[tuple[str, np.ndarray]]:
    t = np.array([tenor_label_to_years(x) for x in TENORS])
    logt = np.log(t)
    lin_log = (logt - logt[0]) / (logt[-1] - logt[0])  # 0 at 1M .. 1 at 30Y
    twist = np.interp(t, [2.0, 10.0], [50.0, -50.0])
    belly = np.interp(t, [1.0, 5.0, 20.0], [0.0, 50.0, 0.0])
    flat = np.ones_like(t)
    return [
        ("PARALLEL_UP_10BP", 10 * flat),
        ("PARALLEL_UP_25BP", 25 * flat),
        ("PARALLEL_DOWN_15BP", -15 * flat),
        ("PARALLEL_DOWN_30BP", -30 * flat),
        ("PARALLEL_UP_100BP", 100 * flat),
        ("PARALLEL_DOWN_100BP", -100 * flat),
        ("PARALLEL_UP_200BP", 200 * flat),
        ("PARALLEL_UP_300BP_BEYOND_GRID", 300 * flat),
        ("BEAR_STEEPENER_25_100", 25 + 75 * lin_log),
        ("BULL_FLATTENER_10_60", -10 - 50 * lin_log),
        ("TWIST_2S10S_50", twist),
        ("BELLY_UP_50", belly),
    ]


# ---------------------------------------------------------------------------
# Position data: positions and bond terms
# ---------------------------------------------------------------------------
POSITIONS = [
    # position_id, trade_id, book, desk, instrument, notional, trade_date
    ("POS-0001", "TRD-BOND-0001", "RATES_BOOK_A", "USD_RATES", "DEMO-BOND-6.00-2031", 10_000_000, "2024-03-12"),
    ("POS-0002", "TRD-BOND-0002", "RATES_BOOK_B", "USD_RATES", "DEMO-BOND-3.50-2028", -20_000_000, "2026-02-03"),
    ("POS-0003", "TRD-BOND-0003", "RATES_BOOK_A", "USD_RATES", "DEMO-BOND-4.25-2036", 5_000_000, "2026-09-01"),
]
BOND_TERMS = [
    # instrument, isin, issuer, issue, maturity, coupon, freq
    ("DEMO-BOND-6.00-2031", "DEMO00000001", "DEMO_SOVEREIGN", "2021-11-15", "2031-11-15", 0.0600, 1),
    ("DEMO-BOND-3.50-2028", "DEMO00000002", "DEMO_SOVEREIGN", "2025-11-15", "2028-11-15", 0.0350, 2),
    ("DEMO-BOND-4.25-2036", "DEMO00000003", "DEMO_SOVEREIGN", "2026-08-15", "2036-08-15", 0.0425, 2),
]


def main() -> None:
    rng = np.random.default_rng(SEED)
    bd = BUSINESS_DATE.isoformat()
    for sub in ("position_data", "sensitivities_data", "shocks_data", "reference"):
        (SRC / sub).mkdir(parents=True, exist_ok=True)

    # ---------------- Reference
    hol = [
        {"calendar_id": CALENDAR_ID, "holiday_date": d.isoformat(), "holiday_name": name}
        for y in range(2020, 2038) for d, name in us_holidays(y)
    ]
    pd.DataFrame(hol).to_csv(SRC / "reference" / "holiday_calendars.csv", index=False)
    cal = HolidayCalendar.from_dates(CALENDAR_ID, [date.fromisoformat(h["holiday_date"]) for h in hol])

    curve_rows = [
        {
            "business_date": bd, "curve_id": CURVE_ID, "currency": "USD", "tenor_label": lab,
            "tenor_years": round(tenor_label_to_years(lab), 6), "zero_rate": z,
            "compounding": "CONTINUOUS", "interpolation": "LINEAR_ZERO",
            "curve_source": "SYNTHETIC_DEMO",
        }
        for lab, z in zip(TENORS, BASE_ZERO, strict=True)
    ]
    pd.DataFrame(curve_rows).to_csv(SRC / "reference" / "yield_curves.csv", index=False)

    pd.DataFrame(
        [
            {"source_system": "FO", "source_risk_factor": f"USD.TSY.ZERO.{lab}",
             "rf_id": rf_id(lab), "mapping_status": "ACTIVE", "effective_from": "2024-01-01"}
            for lab in TENORS
        ]
    ).to_csv(SRC / "reference" / "rf_name_mapping.csv", index=False)

    # ---------------- Shocks data
    pd.DataFrame(
        [
            {"rf_id": rf_id(lab), "risk_class": "IR", "rf_type": "ZERO_RATE", "currency": "USD",
             "curve_id": CURVE_ID, "tenor_label": lab, "tenor_years": round(tenor_label_to_years(lab), 6),
             "shock_type": "ABSOLUTE", "shock_unit": "BP",
             "description": f"USD Treasury zero rate {lab}"}
            for lab in TENORS
        ]
    ).to_csv(SRC / "shocks_data" / "risk_factors.csv", index=False)

    var_dates = cal.business_days(date(2025, 11, 1), BUSINESS_DATE)[-260:]
    svar_dates = [d for d in cal.business_days(date(2022, 3, 1), date(2023, 3, 31))][:250]
    var_shocks = historical_shocks(rng, len(var_dates), vols=[5.5, 2.5, 1.5], idio=0.8, df=5.0)
    svar_shocks = historical_shocks(
        rng, len(svar_dates), vols=[10.5, 4.5, 2.5], idio=1.5, df=3.5, drift_level=0.6,
        jumps={70: 28.0, 135: 22.0, 180: -30.0},  # CPI-day style jumps
    )
    hypo = hypothetical_shocks()

    scen_rows = []

    def add(set_id, scen_id, scen_date, name, shocks):
        for lab, s in zip(TENORS, shocks, strict=True):
            scen_rows.append({
                "business_date": bd, "scenario_set": set_id, "scenario_id": scen_id,
                "scenario_date": scen_date, "scenario_name": name, "rf_id": rf_id(lab),
                "shock_value": round(float(s), 4), "shock_unit": "BP",
            })

    for i, (d, s) in enumerate(zip(var_dates, var_shocks, strict=True), start=1):
        add("VAR_1D", f"VAR_{i:03d}", d.isoformat(), f"HIST_1D_{d.isoformat()}", s)
    for i, (d, s) in enumerate(zip(svar_dates, svar_shocks, strict=True), start=1):
        add("SVAR_1D", f"SVAR_{i:03d}", d.isoformat(), f"STRESSED_1D_{d.isoformat()}", s)
    for i, (name, s) in enumerate(hypo, start=1):
        add("STRESS_HYPO", f"HYP_{i:03d}", "", name, s)
    pd.DataFrame(scen_rows).to_csv(SRC / "shocks_data" / "scenario_shocks.csv", index=False)

    pd.DataFrame([
        {"business_date": bd, "scenario_set": "VAR_1D", "description": "1-day historical shocks, most recent 260 business days",
         "window_start": var_dates[0].isoformat(), "window_end": var_dates[-1].isoformat(),
         "horizon_days": 1, "n_scenarios": len(var_dates), "data_origin": "SYNTHETIC"},
        {"business_date": bd, "scenario_set": "SVAR_1D", "description": "1-day shocks from the stressed window (calibrated to resemble the 2022 rate-hike period)",
         "window_start": svar_dates[0].isoformat(), "window_end": svar_dates[-1].isoformat(),
         "horizon_days": 1, "n_scenarios": len(svar_dates), "data_origin": "SYNTHETIC"},
        {"business_date": bd, "scenario_set": "STRESS_HYPO", "description": "Hypothetical parallel and curve-shape stresses",
         "window_start": "", "window_end": "", "horizon_days": 1, "n_scenarios": len(hypo),
         "data_origin": "SYNTHETIC"},
    ]).to_csv(SRC / "shocks_data" / "scenario_sets.csv", index=False)

    # ---------------- Position data
    pos_df = pd.DataFrame([
        {"business_date": bd, "position_id": p, "trade_id": t, "book_id": b, "desk_id": d,
         "product_type": "FIXED_RATE_BOND", "instrument_id": i, "currency": "USD",
         "notional": n, "trade_date": td, "source_system": "POSITION_DATA"}
        for p, t, b, d, i, n, td in POSITIONS
    ])
    pos_df.to_csv(SRC / "position_data" / "positions.csv", index=False)
    terms_df = pd.DataFrame([
        {"business_date": bd, "instrument_id": i, "isin": isin, "issuer": iss, "currency": "USD",
         "issue_date": idt, "maturity_date": mat, "coupon_rate": c, "coupon_frequency": f,
         "day_count": "ACT/ACT_ICMA", "business_day_convention": "FOLLOWING",
         "calendar_id": CALENDAR_ID, "discount_curve_id": CURVE_ID}
        for i, isin, iss, idt, mat, c, f in BOND_TERMS
    ])
    terms_df.to_csv(SRC / "position_data" / "bond_terms.csv", index=False)

    # ---------------- Sensitivities computed with each source system's convention
    tenors_y = np.array([tenor_label_to_years(t) for t in TENORS])
    curve = ZeroCurve(CURVE_ID, "USD", tuple(TENORS), tenors_y, np.array(BASE_ZERO))
    rf_ids = [rf_id(t) for t in TENORS]
    market = MarketState(BUSINESS_DATE, {CURVE_ID: curve}, {CALENDAR_ID: cal},
                         {CURVE_ID: (rf_ids, np.eye(len(rf_ids)))})
    rows = pos_df.merge(terms_df.drop(columns=["business_date", "currency"]), on="instrument_id")

    fo_rows, frtb_rows = [], []
    for row in rows.to_dict("records"):
        inst = build_instrument(row)
        sens = bumped_rf_sensitivities(inst, market, inst.risk_factor_ids(market))
        for s in sens.itertuples():
            label = s.rf_id.rsplit(".", 1)[1]
            dv01 = -s.delta_per_bp
            if row["position_id"] == "POS-0002" and label == "1Y":
                dv01 *= 1.03  # deliberately stale FO snapshot -> reconciliation break
            for stype, value, unit in (("DV01", round(dv01, 2), "USD_PER_BP_LOSS_POSITIVE"),
                                       ("GAMMA", round(s.gamma_per_bp2, 6), "USD_PER_BP2")):
                fo_rows.append({
                    "business_date": bd, "position_id": row["position_id"], "trade_id": row["trade_id"],
                    "risk_factor_name": f"USD.TSY.ZERO.{label}", "sensitivity_type": stype,
                    "sensitivity_value": value, "unit": unit, "pricing_model": "FO_BOND_DCF_V3",
                    "source_system": "FO",
                })
        vd = frtb_vertex_deltas(inst, market, CURVE_ID, GIRR_VERTEX_LABELS, GIRR_VERTEX_TENORS)
        for v in vd.itertuples():
            if abs(v.delta_per_bp) < 1e-6:
                continue
            frtb_rows.append({
                "business_date": bd, "trade_id": row["trade_id"], "position_id": row["position_id"],
                "risk_class": "GIRR", "risk_type": "DELTA", "bucket": "USD", "risk_factor": CURVE_ID,
                "vertex": v.vertex, "sensitivity_type": "GIRR_DELTA",
                "sensitivity_value": round(v.delta_per_bp * 1e4, 2), "unit": "USD_PER_UNIT_RATE",
                "reporting_currency": "USD", "source_system": "FRTB",
            })
    pd.DataFrame(fo_rows).to_csv(SRC / "sensitivities_data" / "fo_sensitivities.csv", index=False)
    pd.DataFrame(frtb_rows).to_csv(SRC / "sensitivities_data" / "frtb_sensitivities.csv", index=False)

    for f in sorted(SRC.rglob("*.csv")):
        print(f"{f.relative_to(ROOT)}: {sum(1 for _ in open(f)) - 1} rows")


if __name__ == "__main__":
    main()
