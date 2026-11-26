"""Replace the synthetic VAR_1D / SVAR_1D shocks with real US Treasury history.

1. Download "Daily Treasury Par Yield Curve Rates" CSVs (one per year) from
   https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView
   e.g. 2022.csv, 2023.csv for the stressed window and 2025.csv, 2026.csv for the last year.
2. Run from the project root:

   python scripts/convert_treasury_csv.py --csv 2025.csv 2026.csv 2022.csv 2023.csv \
       --business-date 2026-11-24 --svar-start 2022-03-01 --svar-end 2023-02-28

The 1-day changes of par yields are used as absolute zero-rate shocks in bp (a common
simplification; a production engine would bootstrap zero curves first). The hypothetical
STRESS_HYPO set, the base curve and all sensitivities are left unchanged, so the rest of the
sample data stays consistent.
"""

from __future__ import annotations

import argparse
import re
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SHOCKS_DIR = ROOT / "data" / "source" / "shocks_data"
TENORS = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
_COL = re.compile(r"^\s*([\d.]+)\s*(mo|month|months|yr|year|years)\s*$", re.IGNORECASE)


def _label(column: str) -> str | None:
    m = _COL.match(column)
    if not m:
        return None
    n = float(m.group(1))
    unit = "M" if m.group(2).lower().startswith("mo") else "Y"
    return f"{int(n)}{unit}" if n.is_integer() else None


def load_treasury(files: list[Path]) -> pd.DataFrame:
    frames = []
    for f in files:
        df = pd.read_csv(f)
        df["Date"] = pd.to_datetime(df["Date"], format="mixed")
        rename = {c: _label(c) for c in df.columns if _label(c) in TENORS}
        frames.append(df[["Date", *rename]].rename(columns=rename))
    out = pd.concat(frames).drop_duplicates("Date").sort_values("Date").set_index("Date")
    missing = [t for t in TENORS if t not in out.columns]
    if missing:
        raise ValueError(f"Treasury files are missing tenors {missing}")
    return out[TENORS].dropna()


def daily_changes_bp(yields_pct: pd.DataFrame) -> pd.DataFrame:
    return (yields_pct.diff() * 100.0).dropna()  # percent -> bp


def to_long(changes: pd.DataFrame, scenario_set: str, prefix: str, name_prefix: str, bd: str) -> pd.DataFrame:
    rows = []
    for i, (d, row) in enumerate(changes.iterrows(), start=1):
        for tenor in TENORS:
            rows.append({"business_date": bd, "scenario_set": scenario_set, "scenario_id": f"{prefix}_{i:03d}",
                         "scenario_date": d.date().isoformat(), "scenario_name": f"{name_prefix}_{d.date()}",
                         "rf_id": f"IR.USD.TSY.ZERO.{tenor}", "shock_value": round(float(row[tenor]), 4),
                         "shock_unit": "BP"})
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", nargs="+", type=Path, required=True)
    ap.add_argument("--business-date", type=date.fromisoformat, required=True)
    ap.add_argument("--var-days", type=int, default=260)
    ap.add_argument("--svar-start", type=date.fromisoformat, required=True)
    ap.add_argument("--svar-end", type=date.fromisoformat, required=True)
    ap.add_argument("--shocks-dir", type=Path, default=SHOCKS_DIR)
    args = ap.parse_args(argv)

    bd = args.business_date.isoformat()
    chg = daily_changes_bp(load_treasury(args.csv))
    var = chg[chg.index <= pd.Timestamp(args.business_date)].tail(args.var_days)
    svar = chg[(chg.index >= pd.Timestamp(args.svar_start)) & (chg.index <= pd.Timestamp(args.svar_end))]
    if len(var) < args.var_days:
        raise ValueError(f"only {len(var)} daily changes up to {bd}; need {args.var_days}")
    if svar.empty:
        raise ValueError("no data in the stressed window")

    shocks_path, sets_path = args.shocks_dir / "scenario_shocks.csv", args.shocks_dir / "scenario_sets.csv"
    old = pd.read_csv(shocks_path)
    keep = old[~old["scenario_set"].isin(["VAR_1D", "SVAR_1D"])].assign(business_date=bd)
    new = pd.concat([to_long(var, "VAR_1D", "VAR", "UST_1D", bd),
                     to_long(svar, "SVAR_1D", "SVAR", "UST_STRESSED_1D", bd), keep])
    new.to_csv(shocks_path, index=False)

    sets = pd.read_csv(sets_path).assign(business_date=bd)
    for sset, frame, desc in (("VAR_1D", var, "1-day changes of US Treasury par yields, most recent window"),
                              ("SVAR_1D", svar, "1-day changes of US Treasury par yields, stressed window")):
        m = sets["scenario_set"] == sset
        sets.loc[m, ["description", "window_start", "window_end", "n_scenarios", "data_origin"]] = [
            desc, frame.index[0].date().isoformat(), frame.index[-1].date().isoformat(), len(frame),
            "US_TREASURY_PAR_YIELDS"]
    sets.to_csv(sets_path, index=False)
    print(f"VAR_1D: {len(var)} scenarios {var.index[0].date()}..{var.index[-1].date()}; "
          f"SVAR_1D: {len(svar)} scenarios {svar.index[0].date()}..{svar.index[-1].date()}")


if __name__ == "__main__":
    main()
