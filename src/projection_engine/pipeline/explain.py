"""Step 11: scenario explain. Shows, for one position and a few scenarios, exactly how each
method arrived at its number (Taylor delta/gamma parts, grid coordinate, ladder contributions)."""

from __future__ import annotations

import pandas as pd

from ..frtb import tenor_label_to_years


def _tenor_order(pairs: list[tuple[str, float]]) -> list[tuple[str, float]]:
    def key(p):
        try:
            return (0, tenor_label_to_years(p[0]))
        except ValueError:
            return (1, p[0])
    return sorted(pairs, key=key)


def _pick_scenarios(fr: pd.DataFrame, specs: list[str]) -> list[tuple[str, str, str]]:
    picks = []
    for spec in specs:
        if spec in ("SVAR_WORST", "VAR_WORST"):
            sset = "SVAR_1D" if spec == "SVAR_WORST" else "VAR_1D"
            sub = fr[fr.scenario_set == sset]
            if sub.empty:
                continue
            row = sub.loc[sub["value"].idxmin()]
            picks.append((spec, sset, row["scenario_id"]))
        else:
            sset, sid = spec.split(":", 1)
            if ((fr.scenario_set == sset) & (fr.scenario_id == sid)).any():
                picks.append((spec, sset, sid))
    return picks


def explain_position(items: pd.DataFrame, shocks_long: pd.DataFrame, position_rfs: list[str],
                     scenario_names: pd.DataFrame, specs: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """items: all vector items for one position (PNL + diagnostics), long format."""
    fr = items[(items.method == "FULL_REVALUATION") & (items["item"] == "PNL")]
    picks = _pick_scenarios(fr, specs)
    shock_rows, method_rows = [], []
    for label, sset, sid in picks:
        name = scenario_names.loc[(scenario_names.scenario_set == sset) & (scenario_names.scenario_id == sid),
                                  "scenario_name"]
        name = name.iloc[0] if len(name) else sid
        tag = f"{sset}:{sid} {name}" if label == f"{sset}:{sid}" else f"{label}: {sset}:{sid} {name}"
        sh = shocks_long[(shocks_long.scenario_set == sset) & (shocks_long.scenario_id == sid)]
        shock_rows.append({"scenario": tag, **{rf.rsplit(".", 1)[-1]: float(sh.loc[sh.rf_id == rf, "shock_value_canonical"].sum())
                                               for rf in position_rfs}})
        sel = items[(items.scenario_set == sset) & (items.scenario_id == sid)].set_index(["method", "item"])["value"]

        def get(method, item, sel=sel):
            return float(sel.get((method, item), float("nan")))

        fr_pnl = get("FULL_REVALUATION", "PNL")
        details = {
            "FULL_REVALUATION": "all cash flows repriced on the shocked curve (benchmark)",
            "TAYLOR": f"delta part {get('TAYLOR', 'DELTA_PNL'):,.2f} + gamma part {get('TAYLOR', 'GAMMA_PNL'):,.2f}",
            "GRID": (f"coordinate {get('GRID', 'COORDINATE_BP'):.3f} bp (DV01-weighted average shock)"
                     f"{'; EXTRAPOLATED beyond grid' if get('GRID', 'EXTRAPOLATED') > 0 else ''}"),
            "LADDER": "; ".join(
                f"{label} {v:,.2f}" for label, v in _tenor_order(
                    [(i.split("|", 1)[1].rsplit(".", 1)[-1], v)
                     for (m, i), v in sel.items() if m == "LADDER" and i.startswith("CONTRIB|")])
            ) + ("; EXTRAPOLATED beyond ladder" if get("LADDER", "EXTRAPOLATED") > 0 else ""),
        }
        for method in ("FULL_REVALUATION", "TAYLOR", "GRID", "LADDER"):
            pnl = get(method, "PNL")
            method_rows.append({"scenario": tag, "method": method, "pnl": pnl,
                                "diff_vs_full_reval": pnl - fr_pnl, "how": details[method]})
    return pd.DataFrame(shock_rows), pd.DataFrame(method_rows)
