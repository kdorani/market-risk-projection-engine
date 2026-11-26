"""Method 2: sensitivity-based (Taylor expansion).

PnL_s ~= sum_k delta_k * x_{s,k}  +  1/2 * sum_k gamma_k * y_{s,k}^2

delta_k are first-order sensitivities on their own risk-factor basis (e.g. FRTB vertices),
gamma_k diagonal second-order sensitivities (e.g. front-office key-rate gamma), and x / y are
the scenario shocks mapped onto those bases. Cross-gammas are ignored (a known limitation).

The production path runs this as a Spark join + aggregate (see pipeline/projection.py);
this pure function is the reference implementation used in tests and explanations.
"""

from __future__ import annotations

import numpy as np


def taylor_pnl(
    delta_per_bp: np.ndarray,
    delta_shocks_bp: np.ndarray,
    gamma_per_bp2: np.ndarray | None = None,
    gamma_shocks_bp: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return (total, delta_part, gamma_part), each of shape (n_scenarios,)."""
    delta_part = np.atleast_2d(delta_shocks_bp) @ np.asarray(delta_per_bp, dtype=float)
    if gamma_per_bp2 is None or gamma_shocks_bp is None:
        gamma_part = np.zeros_like(delta_part)
    else:
        gamma_part = 0.5 * (np.atleast_2d(gamma_shocks_bp) ** 2 @ np.asarray(gamma_per_bp2, dtype=float))
    return delta_part + gamma_part, delta_part, gamma_part
