"""FRTB constants and sensitivity unit conventions used for canonicalisation."""

from __future__ import annotations

import numpy as np

# GIRR delta vertices (Basel Framework MAR21.8): label -> tenor in years
GIRR_VERTICES: dict[str, float] = {
    "0.25Y": 0.25, "0.5Y": 0.5, "1Y": 1.0, "2Y": 2.0, "3Y": 3.0,
    "5Y": 5.0, "10Y": 10.0, "15Y": 15.0, "20Y": 20.0, "30Y": 30.0,
}

GIRR_VERTEX_LABELS = list(GIRR_VERTICES)
GIRR_VERTEX_TENORS = np.array(list(GIRR_VERTICES.values()))

# (source_system, sensitivity_type, unit) -> (canonical measure, multiplier)
# Canonical: DELTA = dPV for +1bp (per bp); GAMMA = d2PV / d(bp)^2.
SENSITIVITY_CONVENTIONS: dict[tuple[str, str, str], tuple[str, float]] = {
    # FRTB MAR21.19: s = (V(r + 0.0001) - V(r)) / 0.0001  -> per unit of rate
    ("FRTB", "GIRR_DELTA", "USD_PER_UNIT_RATE"): ("DELTA", 1e-4),
    # Front-office DV01 quoted as a positive number for a loss when rates rise 1bp
    ("FO", "DV01", "USD_PER_BP_LOSS_POSITIVE"): ("DELTA", -1.0),
    ("FO", "GAMMA", "USD_PER_BP2"): ("GAMMA", 1.0),
}

# Scenario shock units -> multiplier to canonical units (bp for rates, decimal for relative)
SHOCK_UNIT_FACTORS: dict[tuple[str, str], float] = {
    ("ABSOLUTE", "BP"): 1.0,
    ("ABSOLUTE", "DECIMAL"): 1e4,
    ("ABSOLUTE", "PCT"): 100.0,
    ("RELATIVE", "DECIMAL"): 1.0,
    ("RELATIVE", "PCT"): 0.01,
}


def tenor_label_to_years(label: str) -> float:
    """'1M' -> 1/12, '6M' -> 0.5, '0.25Y' -> 0.25, '10Y' -> 10."""
    label = label.strip().upper()
    unit, value = label[-1], float(label[:-1])
    if unit == "Y":
        return value
    if unit == "M":
        return value / 12.0
    if unit == "W":
        return value * 7.0 / 365.0
    if unit == "D":
        return value / 365.0
    raise ValueError(f"Cannot parse tenor label {label!r}")
