"""End-to-end tests: run the whole Spark pipeline on the sample data.

Skipped automatically when Java is not available. Run only these with:  pytest -m spark
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pandas as pd
import pytest
import yaml

from projection_engine.config import DEFAULT_CONFIG, PROJECT_ROOT
from projection_engine.pipeline.quality import DataQualityError
from projection_engine.pipeline.runner import run_engine

pytestmark = [
    pytest.mark.spark,
    pytest.mark.skipif(shutil.which("java") is None and not os.environ.get("JAVA_HOME"),
                       reason="Spark needs Java"),
]

GOLDEN = Path(__file__).parent / "golden" / "risk_measures_full_revaluation.csv"


def _config(tmp_path: Path, source_root: Path | None = None) -> Path:
    raw = yaml.safe_load(DEFAULT_CONFIG.read_text())
    raw["paths"] = {"warehouse_dir": str(tmp_path / "warehouse"), "output_dir": str(tmp_path / "output")}
    raw["spark"]["master"] = "local[2]"
    if source_root is not None:
        for system, spec in raw["sources"].items():
            spec["path"] = str(source_root / system.lower())
    path = tmp_path / "run_config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("run")
    return run_engine(_config(tmp), quiet=True), tmp


def test_all_quality_checks_and_vectors_pass(run):
    res, _ = run
    assert (res["dq_results"].status == "PASS").all()
    assert (res["vector_validation"].status == "PASS").all()
    assert (res["position_valuation"].status == "OK").all()


def test_reconciliation_flags_exactly_the_injected_break(run):
    recon = run[0]["sensitivity_reconciliation"]
    breaks = recon[recon.status != "PASS"]
    assert len(breaks) == 1
    b = breaks.iloc[0]
    assert (b.position_id, b.comparison, b.risk_factor, b.measure) == (
        "POS-0002", "FO_vs_ENGINE", "IR.USD.TSY.ZERO.1Y", "DELTA")


def test_methods_agree_with_full_revaluation_for_bonds(run):
    cmp = run[0]["method_comparison"]
    pos1 = cmp[(cmp.node == "POS-0001") & cmp.scenario_set.isin(["VAR_1D", "SVAR_1D"])]
    assert (pos1.var_rel_diff.abs() < 1e-3).all()
    assert (pos1.plat_style_zone == "GREEN").all()
    # POS-0003: Taylor on FRTB vertices carries the 7Y mapping error, grid/ladder do not
    pos3 = cmp[(cmp.node == "POS-0003") & (cmp.scenario_set == "VAR_1D")].set_index("method")
    assert pos3.loc["TAYLOR", "mean_abs_diff"] > 10 * pos3.loc["GRID", "mean_abs_diff"]


def test_portfolio_var_is_diversified(run):
    risk = run[0]["risk_measures"]
    fr = risk[(risk.method == "FULL_REVALUATION") & (risk.scenario_set == "VAR_1D")]
    assert fr[fr.level == "PORTFOLIO"].var_1d.iloc[0] < fr[fr.level == "POSITION"].var_1d.sum()


def test_regression_against_golden_risk_numbers(run):
    risk = run[0]["risk_measures"]
    got = risk[risk.method == "FULL_REVALUATION"][["level", "node", "scenario_set", "var_1d", "es_1d"]]
    if not GOLDEN.exists():  # first run: create the golden file, then review and commit it
        GOLDEN.parent.mkdir(exist_ok=True)
        got.to_csv(GOLDEN, index=False)
        pytest.skip("golden file created")
    exp = pd.read_csv(GOLDEN)
    merged = exp.merge(got, on=["level", "node", "scenario_set"], suffixes=("_exp", "_got"))
    assert len(merged) == len(exp)
    for col in ("var_1d", "es_1d"):
        assert merged[f"{col}_got"].to_numpy() == pytest.approx(merged[f"{col}_exp"].to_numpy(), rel=1e-6)


def test_step_outputs_written(run):
    _, tmp = run
    steps = sorted(p.name for p in (tmp / "output" / "steps").glob("*.csv"))
    assert any(n.startswith("01_") for n in steps) and any(n.startswith("11_") for n in steps)
    assert (tmp / "output" / "run_report.md").exists()
    assert (tmp / "output" / "run_metadata.json").exists()


def test_bad_reference_data_stops_the_run(tmp_path):
    src = tmp_path / "source"
    shutil.copytree(PROJECT_ROOT / "data" / "source", src)
    terms = pd.read_csv(src / "position_data" / "bond_terms.csv")
    terms.loc[0, "discount_curve_id"] = "USD_MISSING_CURVE"
    terms.to_csv(src / "position_data" / "bond_terms.csv", index=False)
    with pytest.raises(DataQualityError, match="discount_curve_exists"):
        run_engine(_config(tmp_path, source_root=src), quiet=True)
