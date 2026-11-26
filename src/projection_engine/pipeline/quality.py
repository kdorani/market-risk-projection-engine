"""Step 2: data-quality checks on staged inputs. ERROR findings stop the run."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from ..frtb import GIRR_VERTICES, SENSITIVITY_CONVENTIONS, SHOCK_UNIT_FACTORS
from ..products import SUPPORTED_PRODUCT_TYPES
from ..schemas import SOURCE_TABLES
from .context import RunContext


class DataQualityError(RuntimeError):
    pass


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    failed_rows: int
    sample: str

    @property
    def status(self) -> str:
        return "PASS" if self.failed_rows == 0 else ("FAIL" if self.severity == "ERROR" else "WARN")


def _sample(df: DataFrame, cols: list[str], n: int = 3) -> str:
    rows = df.select(*cols).limit(n).collect()
    return "; ".join(",".join(str(v) for v in r) for r in rows)


def _bad_rows(table: str, name: str, severity: str, df: DataFrame, bad: Column,
              show: list[str]) -> CheckResult:
    failed = df.filter(bad)
    n = failed.count()
    return CheckResult(table, name, severity, n, _sample(failed, show) if n else "")


def _anti_join(table: str, name: str, severity: str, child: DataFrame, parent: DataFrame,
               keys: list[tuple[str, str]], show: list[str]) -> CheckResult:
    cond = [child[c] == parent[p] for c, p in keys]
    missing = child.join(parent, cond, "left_anti")
    n = missing.count()
    return CheckResult(table, name, severity, n, _sample(missing, show) if n else "")


def _in_list(col: str, values) -> Column:
    return F.col(col).isin(list(values))


def run_quality_checks(ctx: RunContext, t: dict[str, DataFrame]) -> pd.DataFrame:
    res: list[CheckResult] = []
    add = res.append

    # Generic: primary key not null and unique for every staged table
    for table in SOURCE_TABLES:
        df, pk = t[table.name], list(table.primary_key)
        null_pk = F.lit(False)
        for c in pk:
            null_pk = null_pk | F.col(c).isNull()
        add(_bad_rows(table.name, "primary_key_not_null", "ERROR", df, null_pk, pk))
        dup = df.groupBy(*pk).count().filter("count > 1")
        n = dup.count()
        add(CheckResult(table.name, "primary_key_unique", "ERROR", n, _sample(dup, pk) if n else ""))

    pos, terms = t["positions"], t["bond_terms"]
    add(_bad_rows("positions", "product_type_supported", "ERROR", pos,
                  ~_in_list("product_type", SUPPORTED_PRODUCT_TYPES), ["position_id", "product_type"]))
    add(_bad_rows("positions", "notional_non_zero", "ERROR", pos, F.col("notional") == 0, ["position_id"]))
    bonds = pos.filter(F.col("product_type") == "FIXED_RATE_BOND")
    add(_anti_join("positions", "bond_has_terms", "ERROR", bonds, terms,
                   [("instrument_id", "instrument_id")], ["position_id", "instrument_id"]))
    pos_terms = bonds.alias("p").join(terms.alias("b"), "instrument_id")
    add(_bad_rows("positions", "currency_matches_terms", "ERROR", pos_terms,
                  F.col("p.currency") != F.col("b.currency"), ["position_id"]))

    add(_bad_rows("bond_terms", "maturity_after_issue", "ERROR", terms,
                  F.col("maturity_date") <= F.col("issue_date"), ["instrument_id"]))
    add(_bad_rows("bond_terms", "not_matured", "ERROR", terms,
                  F.col("maturity_date") <= F.lit(ctx.cfg.business_date), ["instrument_id", "maturity_date"]))
    add(_bad_rows("bond_terms", "coupon_frequency_valid", "ERROR", terms,
                  ~_in_list("coupon_frequency", [1, 2, 4, 12]), ["instrument_id", "coupon_frequency"]))
    add(_bad_rows("bond_terms", "coupon_rate_in_range", "WARN", terms,
                  (F.col("coupon_rate") < -0.01) | (F.col("coupon_rate") > 0.25), ["instrument_id", "coupon_rate"]))
    add(_bad_rows("bond_terms", "day_count_supported", "ERROR", terms,
                  ~_in_list("day_count", ["ACT/ACT_ICMA", "ACT/365F", "ACT/360", "30/360"]),
                  ["instrument_id", "day_count"]))
    add(_anti_join("bond_terms", "discount_curve_exists", "ERROR", terms, t["yield_curves"],
                   [("discount_curve_id", "curve_id")], ["instrument_id", "discount_curve_id"]))
    add(_anti_join("bond_terms", "calendar_exists", "ERROR", terms, t["holiday_calendars"],
                   [("calendar_id", "calendar_id")], ["instrument_id", "calendar_id"]))

    yc = t["yield_curves"]
    add(_bad_rows("yield_curves", "zero_rate_in_range", "ERROR", yc,
                  (F.col("zero_rate") < -0.05) | (F.col("zero_rate") > 0.25), ["curve_id", "tenor_label", "zero_rate"]))
    add(_bad_rows("yield_curves", "tenor_positive", "ERROR", yc, F.col("tenor_years") <= 0, ["curve_id", "tenor_label"]))

    rf, sh = t["risk_factors"], t["scenario_shocks"]
    known_units = [f"{a}|{b}" for a, b in SHOCK_UNIT_FACTORS]
    add(_bad_rows("risk_factors", "shock_type_unit_known", "ERROR", rf,
                  ~F.concat_ws("|", "shock_type", "shock_unit").isin(known_units), ["rf_id"]))
    add(_anti_join("scenario_shocks", "rf_defined_in_risk_factors", "ERROR", sh, rf, [("rf_id", "rf_id")],
                   ["scenario_id", "rf_id"]))
    sh_rf = sh.alias("s").join(rf.alias("r"), "rf_id")
    add(_bad_rows("scenario_shocks", "unit_matches_rf_definition", "ERROR", sh_rf,
                  F.col("s.shock_unit") != F.col("r.shock_unit"), ["scenario_id", "rf_id"]))
    add(_bad_rows("scenario_shocks", "abs_shock_below_1000bp", "WARN", sh_rf,
                  (F.col("r.shock_type") == "ABSOLUTE") & (F.abs("shock_value") > 1000),
                  ["scenario_id", "rf_id", "shock_value"]))
    n_rf = sh.groupBy("scenario_set").agg(F.countDistinct("rf_id").alias("n_rf"))
    per_scen = sh.groupBy("scenario_set", "scenario_id").agg(F.count("*").alias("n"))
    incomplete = per_scen.join(n_rf, "scenario_set").filter(F.col("n") != F.col("n_rf"))
    n = incomplete.count()
    add(CheckResult("scenario_shocks", "scenario_matrix_complete", "ERROR", n,
                    _sample(incomplete, ["scenario_set", "scenario_id"]) if n else ""))
    sets_present = {r[0] for r in sh.select("scenario_set").distinct().collect()}
    missing_sets = sorted(set(ctx.cfg.scenario_sets) - sets_present)
    add(CheckResult("scenario_shocks", "configured_sets_present", "ERROR", len(missing_sets),
                    ", ".join(missing_sets)))
    counts = sh.groupBy("scenario_set").agg(F.countDistinct("scenario_id").alias("actual"))
    mism = t["scenario_sets"].join(counts, "scenario_set").filter(F.col("n_scenarios") != F.col("actual"))
    n = mism.count()
    add(CheckResult("scenario_sets", "n_scenarios_matches_shocks", "ERROR", n,
                    _sample(mism, ["scenario_set", "n_scenarios", "actual"]) if n else ""))

    conv_keys = [f"{a}|{b}|{c}" for a, b, c in SENSITIVITY_CONVENTIONS]
    frtb, fo = t["frtb_sensitivities"], t["fo_sensitivities"]
    add(_bad_rows("frtb_sensitivities", "unit_convention_known", "ERROR", frtb,
                  ~F.concat_ws("|", "source_system", "sensitivity_type", "unit").isin(conv_keys),
                  ["position_id", "vertex", "unit"]))
    add(_bad_rows("frtb_sensitivities", "girr_vertex_valid", "ERROR", frtb,
                  ~_in_list("vertex", GIRR_VERTICES), ["position_id", "vertex"]))
    add(_anti_join("frtb_sensitivities", "position_exists", "ERROR", frtb, pos,
                   [("position_id", "position_id")], ["position_id"]))
    add(_bad_rows("fo_sensitivities", "unit_convention_known", "ERROR", fo,
                  ~F.concat_ws("|", "source_system", "sensitivity_type", "unit").isin(conv_keys),
                  ["position_id", "risk_factor_name", "unit"]))
    active_map = t["rf_name_mapping"].filter(F.col("mapping_status") == "ACTIVE")
    add(_anti_join("fo_sensitivities", "risk_factor_name_mapped", "ERROR", fo, active_map,
                   [("source_system", "source_system"), ("risk_factor_name", "source_risk_factor")],
                   ["position_id", "risk_factor_name"]))
    if "TAYLOR" in ctx.cfg.methods:
        delta_src = ctx.cfg.taylor["delta_source"]
        src_tbl = frtb if delta_src == "FRTB" else fo
        add(_anti_join("positions", f"has_{delta_src.lower()}_delta_for_taylor", "ERROR", pos, src_tbl,
                       [("position_id", "position_id")], ["position_id"]))

    out = pd.DataFrame([{**r.__dict__, "status": r.status} for r in res])
    return out[["table", "check", "severity", "status", "failed_rows", "sample"]]


def dq_gate(results: pd.DataFrame) -> None:
    failed = results[results["status"] == "FAIL"]
    if len(failed):
        raise DataQualityError("Data-quality errors:\n" + failed.to_string(index=False))

