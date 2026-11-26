import importlib.util
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("convert_treasury_csv", ROOT / "scripts" / "convert_treasury_csv.py")
conv = importlib.util.module_from_spec(spec)
spec.loader.exec_module(conv)

COLUMNS = ["1 Mo", "1.5 Month", "2 Mo", "3 Mo", "4 Mo", "6 Mo", "1 Yr", "2 Yr", "3 Yr", "5 Yr", "7 Yr",
           "10 Yr", "20 Yr", "30 Yr"]


def _treasury_file(path: Path, start: str, periods: int, seed: int) -> Path:
    days = pd.bdate_range(start, periods=periods)
    rng = np.random.default_rng(seed)
    levels = 4.0 + np.cumsum(rng.normal(0, 0.05, size=(periods, len(COLUMNS))), axis=0)
    df = pd.DataFrame(levels.round(2), columns=COLUMNS)
    df.insert(0, "Date", days.strftime("%m/%d/%Y"))
    df.iloc[::-1].to_csv(path, index=False)  # Treasury files are newest-first
    return path


def test_converter_replaces_var_and_svar_sets(tmp_path):
    shocks_dir = tmp_path / "shocks_data"
    shutil.copytree(ROOT / "data" / "source" / "shocks_data", shocks_dir)
    recent = _treasury_file(tmp_path / "recent.csv", "2025-10-01", 300, 1)
    stressed = _treasury_file(tmp_path / "stressed.csv", "2022-02-01", 300, 2)
    conv.main(["--csv", str(recent), str(stressed), "--business-date", "2026-11-24",
               "--svar-start", "2022-03-01", "--svar-end", "2023-02-28", "--shocks-dir", str(shocks_dir)])
    shocks = pd.read_csv(shocks_dir / "scenario_shocks.csv")
    counts = shocks.groupby("scenario_set")["scenario_id"].nunique()
    assert counts["VAR_1D"] == 260 and counts["STRESS_HYPO"] == 12 and counts["SVAR_1D"] > 200
    assert shocks.groupby(["scenario_set", "scenario_id"]).size().eq(11).all()
    sets = pd.read_csv(shocks_dir / "scenario_sets.csv").set_index("scenario_set")
    assert sets.loc["VAR_1D", "data_origin"] == "US_TREASURY_PAR_YIELDS"
    assert sets.loc["SVAR_1D", "n_scenarios"] == counts["SVAR_1D"]
