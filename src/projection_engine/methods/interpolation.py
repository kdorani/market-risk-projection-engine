"""1-D interpolation with an explicit extrapolation policy and flag."""

from __future__ import annotations

import numpy as np

EXTRAPOLATION_POLICIES = {"LINEAR_FLAG", "FLAT_FLAG", "ERROR"}


def interp_with_flag(
    nodes: np.ndarray, values: np.ndarray, x: np.ndarray, policy: str = "LINEAR_FLAG"
) -> tuple[np.ndarray, np.ndarray]:
    """Piecewise-linear interpolation. Returns (y, extrapolated_mask).

    Outside the node range: LINEAR_FLAG extends the end segments, FLAT_FLAG holds the end
    values, ERROR raises. Extrapolated points are always flagged so they can be reported
    (a silent extrapolation is a classic source of bad tail PnL).
    """
    if policy not in EXTRAPOLATION_POLICIES:
        raise ValueError(f"Unknown extrapolation policy {policy!r}")
    nodes = np.asarray(nodes, dtype=float)
    values = np.asarray(values, dtype=float)
    x = np.atleast_1d(np.asarray(x, dtype=float))
    order = np.argsort(nodes)
    nodes, values = nodes[order], values[order]
    y = np.interp(x, nodes, values)
    lo, hi = x < nodes[0], x > nodes[-1]
    extrapolated = lo | hi
    if extrapolated.any():
        if policy == "ERROR":
            raise ValueError(f"{int(extrapolated.sum())} point(s) outside [{nodes[0]}, {nodes[-1]}]")
        if policy == "LINEAR_FLAG" and len(nodes) >= 2:
            slope_lo = (values[1] - values[0]) / (nodes[1] - nodes[0])
            slope_hi = (values[-1] - values[-2]) / (nodes[-1] - nodes[-2])
            y[lo] = values[0] + slope_lo * (x[lo] - nodes[0])
            y[hi] = values[-1] + slope_hi * (x[hi] - nodes[-1])
    return y, extrapolated
