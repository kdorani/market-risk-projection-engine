"""End-to-end orchestration of the projection engine (steps 01-11)."""

from __future__ import annotations

import json
import platform
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import __version__
from ..config import PROJECT_ROOT, EngineConfig
from ..frtb import tenor_label_to_years
from ..spark_session import get_spark
from .aggregation import (
    aggregate_hierarchy,
    compare_methods,
    diversification,
    risk_measures,
    validate_vectors,
)
from .context import RunContext
from .explain import explain_position
from .ingestion import ingest_sources
from .mapping import map_sensitivity_risk_factors
from .market_data import (
    build_market_state,
    canonical_scenarios,
    collect_shocks,
    scenario_statistics,
)
from .projection import (
    CASHFLOW_SCHEMA,
    EXPOSURE_SCHEMA,
    REVAL_SCHEMA,
    VALUATION_SCHEMA,
    VECTOR_SCHEMA,
    VERTEX_SCHEMA,
    apply_revaluation_tables_fn,
    cashflow_fn,
    exposure_fn,
    full_revaluation_fn,
    instrument_frame,
    revaluation_tables_fn,
    taylor_vectors,
    valuation_fn,
    vertex_fn,
)
from .quality import DataQualityError, dq_gate, run_quality_checks
from .sensitivities import canonical_sensitivities, reconcile_sensitivities


class VectorValidationError(RuntimeError):
    pass


def _with_bd(df: DataFrame, ctx: RunContext) -> DataFrame:
    return df.withColumn("business_date", F.lit(ctx.cfg.business_date))


def _pd(df: DataFrame, drop_lineage: bool = True) -> pd.DataFrame:
    pdf = df.toPandas()
    if drop_lineage:
        pdf = pdf.drop(columns=[c for c in pdf.columns if c.startswith("_") or c in ("run_id",)])
    return pdf


def _git_commit() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=PROJECT_ROOT,
                              capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:
        return ""


def run_engine(config_path: str | Path | None = None, quiet: bool = False) -> dict[str, pd.DataFrame]:
    cfg = EngineConfig.load(config_path)
    sc = cfg.spark
    spark = get_spark(master=sc.get("master", "local[*]"), shuffle_partitions=sc.get("shuffle_partitions", 8),
                      log_level=sc.get("log_level", "WARN"))
    ctx = RunContext.create(cfg, spark, quiet)
    rep, wh = ctx.reporter, ctx.warehouse
    rm, rcfg = cfg.risk_measures, cfg.reporting
    results: dict[str, pd.DataFrame] = {}
    try:
        # ------------------------------------------------------------------ 01 ingestion
        staged, ingest_summary = ingest_sources(ctx)
        rep.step("01", "Ingestion (position, sensitivity, shock and reference sources -> engine staging tables)",
                 "Each upstream table is read with an explicit schema, filtered to the business date, "
                 "tagged with lineage columns and persisted as an engine-owned Parquet table (stg_*).",
                 {"ingestion_summary": ingest_summary,
                  "stg_positions": _pd(staged["positions"]),
                  "stg_bond_terms": _pd(staged["bond_terms"])})

        # ------------------------------------------------------------------ 02 data quality
        dq = run_quality_checks(ctx, staged)
        wh.write("dq_results", _with_bd(spark.createDataFrame(dq.astype({"failed_rows": "int64"})), ctx))
        rep.step("02", "Data-quality checks",
                 "Keys, referential integrity, ranges, units and scenario-matrix completeness. Any ERROR "
                 "check that fails stops the run before pricing.", {"dq_results": dq}, max_rows=100)
        results["dq_results"] = dq
        dq_gate(dq)

        # ------------------------------------------------------------------ 03 market data + valuation
        market, curve_map = build_market_state(staged, cfg.business_date)
        inst = instrument_frame(staged).repartition(max(1, spark.sparkContext.defaultParallelism), "position_id")
        valuation = wh.write("position_valuation", _with_bd(inst.mapInPandas(valuation_fn(market), VALUATION_SCHEMA), ctx))
        valid = valuation.filter(F.col("status") == "OK").select("position_id")
        inst_ok = inst.join(valid, "position_id", "left_semi").cache()
        cashflows = wh.write("bond_cashflows", _with_bd(inst_ok.mapInPandas(cashflow_fn(market), CASHFLOW_SCHEMA), ctx))
        val_pdf = _pd(valuation)
        results["position_valuation"] = val_pdf
        rep.step("03", "Market data and base valuation",
                 "Base zero curve (with its node -> scenario risk-factor mapping), bond schedules with "
                 "business-day-adjusted pay dates and ACT/ACT ICMA accruals, and base PVs "
                 "(dirty = PV of future cash flows; clean = dirty - accrued).",
                 {"base_curve_and_node_mapping": curve_map, "position_valuation": val_pdf,
                  "bond_cashflows": _pd(cashflows).sort_values(["position_id", "pay_date"])})

        # ------------------------------------------------------------------ 04 risk-factor mapping
        exposures = wh.write("map_position_rf", _with_bd(inst_ok.mapInPandas(exposure_fn(market), EXPOSURE_SCHEMA), ctx))
        sens_map, map_exc = map_sensitivity_risk_factors(spark, staged)
        sens_map = wh.write("map_sensitivity_rf", sens_map)
        rep.step("04", "Risk-factor mapping",
                 "Position -> scenario risk factors it is exposed to (engine bump-and-revalue delta and "
                 "gamma per factor), and sensitivity risk factors (FRTB vertices, FO bucket names) "
                 "-> scenario risk factors with interpolation weights.",
                 {"map_position_rf": _sort_by_tenor(_pd(exposures), ["position_id"]),
                  "map_sensitivity_rf": _pd(sens_map).sort_values(["sens_source", "sens_rf_key"]),
                  "mapping_exceptions": map_exc}, max_rows=40)
        if len(map_exc):
            raise DataQualityError(f"Unmapped sensitivity risk factors:\n{map_exc}")

        # ------------------------------------------------------------------ 05 scenarios
        scen = wh.write("scenario_shocks_canonical", canonical_scenarios(spark, staged, cfg.scenario_sets))
        shocks = collect_shocks(scen)
        n_by_set = shocks.index.groupby("scenario_set").size().to_dict()
        sample = (scen.filter(F.col("scenario_set").isin("VAR_1D", "STRESS_HYPO")).toPandas()
                  .pivot_table(index=["scenario_set", "scenario_id", "scenario_name"], columns="rf_id",
                               values="shock_value_canonical").reset_index())
        rf_cols = _tenor_sorted([c for c in sample.columns if str(c).startswith("IR.")])
        sample = sample.rename(columns={c: c.rsplit(".", 1)[-1] for c in rf_cols})
        tenor_order = [c.rsplit(".", 1)[-1] for c in rf_cols]
        sample = pd.concat([sample[sample.scenario_set == "VAR_1D"].tail(8), sample[sample.scenario_set == "STRESS_HYPO"]])
        rep.step("05", "Scenario shocks in canonical units",
                 f"Scenario sets {n_by_set}. Rate shocks are absolute, in bp, on zero rates. "
                 "Shown: per-factor statistics and a sample of the scenario matrix.",
                 {"scenario_statistics": scenario_statistics(scen),
                  "scenario_matrix_sample": sample[["scenario_set", "scenario_id", "scenario_name", *tenor_order]]},
                 max_rows=40)

        # ------------------------------------------------------------------ 06 sensitivities
        canon = wh.write("sensitivities_canonical", canonical_sensitivities(spark, staged))
        vertex = inst_ok.mapInPandas(vertex_fn(market), VERTEX_SCHEMA)
        recon = wh.write("sensitivity_reconciliation", _with_bd(reconcile_sensitivities(
            canon, sens_map, exposures, vertex, cfg.reconciliation["rel_tolerance"],
            cfg.reconciliation["abs_tolerance"]), ctx))
        recon_pdf = _pd(recon)
        results["sensitivity_reconciliation"] = recon_pdf
        rep.step("06", "Sensitivities: unit conversion and reconciliation",
                 "FRTB GIRR delta (per unit of rate, MAR21.19) and FO DV01 (positive = loss) are "
                 "converted to one convention: dPV per +1bp. Each feed is then reconciled with the "
                 "engine's own bump-and-revalue numbers. A BREAK means the feed disagrees with the "
                 "engine by more than tolerance (one FO value is deliberately stale in the sample data).",
                 {"sensitivities_canonical": _pd(canon).sort_values(["position_id", "sens_source", "measure", "sens_rf_key"]),
                  "sensitivity_reconciliation": recon_pdf}, max_rows=60)

        # ------------------------------------------------------------------ 07 grids and ladders
        reval = wh.write("revaluation_tables", _with_bd(inst_ok.mapInPandas(
            revaluation_tables_fn(market, cfg.grid["nodes_bp"], cfg.ladder["shifts_bp"]), REVAL_SCHEMA), ctx))
        exp_pos = rcfg.get("explain_position", "")
        reval_pdf = _pd(reval)
        grid_view = (reval_pdf[(reval_pdf.position_id == exp_pos) & (reval_pdf.table_type == "GRID_NODE")]
                     [["position_id", "axis", "node_bp", "value"]].rename(columns={"value": "pnl"}))
        weight_view = (reval_pdf[(reval_pdf.position_id == exp_pos) & (reval_pdf.table_type == "GRID_WEIGHT")]
                       [["position_id", "rf_id", "value"]].rename(columns={"value": "dv01_weight"}))
        weight_view = weight_view.set_index("rf_id").loc[_tenor_sorted(weight_view.rf_id.tolist())].reset_index()
        lad = reval_pdf[(reval_pdf.position_id == exp_pos) & (reval_pdf.table_type == "LADDER")]
        ladder_view = lad.pivot_table(index="node_bp", columns="rf_id", values="value").reset_index()
        ladder_view = ladder_view[["node_bp", *_tenor_sorted([c for c in ladder_view.columns if str(c).startswith("IR.")])]]
        ladder_view.columns = [c.rsplit(".", 1)[-1] if str(c).startswith("IR.") else c for c in ladder_view.columns]
        rep.step("07", "Pre-computed revaluation tables (grid and ladders)",
                 "Computed once per day by full revaluation, then re-used for every scenario set. Grid: "
                 "PnL at parallel shifts + the DV01 weights that map a scenario onto the grid axis. "
                 f"Ladders: PnL when one risk factor moves alone. Shown for {exp_pos}.",
                 {"grid_nodes": grid_view.sort_values("node_bp"), "grid_coordinate_weights": weight_view,
                  "ladders_by_rf": ladder_view}, max_rows=30)

        # ------------------------------------------------------------------ 08 PnL vectors
        parts = []
        if "FULL_REVALUATION" in cfg.methods:
            parts.append(inst_ok.mapInPandas(full_revaluation_fn(market, shocks), VECTOR_SCHEMA))
        if "TAYLOR" in cfg.methods:
            t = cfg.taylor
            parts.append(taylor_vectors(canon, sens_map, scen, valid, t["delta_source"], t["gamma_source"],
                                        bool(t.get("include_gamma", True))))
        if {"GRID", "LADDER"} & set(cfg.methods):
            parts.append(reval.groupBy("position_id").applyInPandas(
                apply_revaluation_tables_fn(shocks, cfg.grid["extrapolation"], cfg.ladder["extrapolation"],
                                            cfg.methods), VECTOR_SCHEMA))
        items = parts[0]
        for p in parts[1:]:
            items = items.unionByName(p)
        items = _with_bd(items.withColumn(
            "scenario_date", F.when(F.col("scenario_date") == "", None).otherwise(F.col("scenario_date")).cast("date")), ctx)
        items = wh.write("pnl_vector_items", items)
        vectors = wh.write("pnl_vectors", items.filter(F.col("item") == "PNL").withColumnRenamed("value", "pnl").drop("item"))
        wh.write("pnl_diagnostics", items.filter(F.col("item") != "PNL"))
        valid_ids = [r[0] for r in valid.collect()]
        check = validate_vectors(vectors, valid_ids, n_by_set, cfg.methods)
        vec_pdf = vectors.filter(F.col("position_id") == exp_pos).toPandas()
        vec_pdf["scenario_date"] = vec_pdf["scenario_date"].astype("string").fillna("")
        vec_view = vec_pdf.pivot(index=["scenario_set", "scenario_id", "scenario_date"], columns="method",
                                 values="pnl").reset_index().sort_values(["scenario_set", "scenario_id"])
        vec_view = vec_view[["scenario_set", "scenario_id", "scenario_date", *[m for m in cfg.methods if m in vec_view]]]
        rep.step("08", "PnL vectors (one value per position x scenario x method)",
                 "Long format: position_id, scenario_set, scenario_id, method, pnl. Validation checks that "
                 "every valid position has a complete vector for every method and scenario set.",
                 {"vector_validation": check,
                  f"pnl_vectors_{exp_pos}": pd.concat([vec_view[vec_view.scenario_set == s].head(6)
                                                      for s in vec_view.scenario_set.unique()])}, max_rows=40)
        results["vector_validation"] = check
        if (check.status != "PASS").any():
            raise VectorValidationError(check[check.status != "PASS"].to_string(index=False))

        # ------------------------------------------------------------------ 09 aggregation + VaR/SVaR
        agg = wh.write("pnl_vectors_aggregated", _with_bd(
            aggregate_hierarchy(vectors, staged["positions"], rm["hierarchy"]), ctx))
        risk = wh.write("risk_measures", _with_bd(risk_measures(
            agg, rm["var_scenario_sets"], rm["var_confidence"], rm["es_confidence"], rm["quantile_method"],
            rm["scaling_horizon_days"]), ctx))
        risk_pdf = _pd(risk).sort_values(["scenario_set", "level", "node", "method"])
        level_order = {lvl: i for i, lvl in enumerate(rm["hierarchy"])}
        risk_pdf = risk_pdf.sort_values(["scenario_set", "level", "node", "method"],
                                        key=lambda s: s.map(level_order) if s.name == "level" else s)
        results["risk_measures"] = risk_pdf
        names = scen.select("scenario_set", "scenario_id", "scenario_name").distinct().toPandas()
        stress = (agg.filter((F.col("scenario_set") == "STRESS_HYPO") & F.col("level").isin("POSITION", "PORTFOLIO"))
                  .toPandas().merge(names, on=["scenario_set", "scenario_id"]))
        stress_view = stress.pivot_table(index=["level", "node", "scenario_id", "scenario_name"],
                                         columns="method", values="pnl").reset_index()
        stress_view = stress_view[["level", "node", "scenario_id", "scenario_name", *[m for m in cfg.methods if m in stress_view]]]
        rep.step("09", "Aggregation and risk measures (VaR, SVaR, ES, stress)",
                 f"Vectors are summed scenario-by-scenario up the hierarchy {rm['hierarchy']}; VaR is taken "
                 f"on the summed vector ({rm['quantile_method']}, {rm['var_confidence']:.0%}, 1-day; 10-day "
                 "shown with sqrt(10) scaling). Diversification = sum of position VaRs - portfolio VaR.",
                 {"risk_measures": risk_pdf, "diversification": diversification(risk_pdf),
                  "stress_results": stress_view.sort_values(["level", "node", "scenario_id"])}, max_rows=60)

        # ------------------------------------------------------------------ 10 method comparison
        cmp = wh.write("method_comparison", _with_bd(compare_methods(agg, rm["var_scenario_sets"], rm["var_confidence"], rm["quantile_method"]), ctx))
        cmp_pdf = _pd(cmp).sort_values(["scenario_set", "level", "node", "method"],
                                       key=lambda s: s.map(level_order) if s.name == "level" else s)
        results["method_comparison"] = cmp_pdf
        diag = wh.spark.read.parquet(str(cfg.warehouse_dir / "pnl_diagnostics")).filter(
            (F.col("business_date") == F.lit(cfg.business_date)) & (F.col("item") == "EXTRAPOLATED"))
        extrap = diag.groupBy("position_id", "method", "scenario_set").agg(
            F.sum("value").cast("int").alias("n_extrapolated")).filter("n_extrapolated > 0").toPandas()
        rep.step("10", "Method comparison against full revaluation",
                 "Accuracy of Taylor, grid and ladder vs full revaluation on the VaR / SVaR sets: mean/max "
                 "absolute differences, VaR difference, and FRTB PLA-test style statistics (Spearman "
                 "correlation, KS distance; zones use MAR32 thresholds as an analogy only). Stress scenarios "
                 "are compared one by one in step 09. Extrapolation flags show scenarios that fell outside "
                 "the pre-computed grid / ladder range.",
                 {"method_comparison": cmp_pdf, "extrapolation_flags": extrap}, max_rows=80)

        # ------------------------------------------------------------------ 11 explain
        pos_items = items.filter(F.col("position_id") == exp_pos).toPandas()
        pos_items["scenario_date"] = pos_items["scenario_date"].astype("string")
        pos_rfs = _tenor_sorted(exposures.filter(F.col("position_id") == exp_pos).select("rf_id").toPandas()["rf_id"].tolist())
        scen_long = scen.filter(F.col("rf_id").isin(pos_rfs)).select(
            "scenario_set", "scenario_id", "rf_id", "shock_value_canonical").toPandas()
        ex_shocks, ex_methods = explain_position(pos_items, scen_long, pos_rfs, names,
                                                 rcfg.get("explain_scenarios", []))
        rep.step("11", f"Scenario explain for {exp_pos}",
                 "How each method produced its number for selected scenarios (worst stressed day, worst "
                 "VaR day, a curve twist and a beyond-grid stress).",
                 {"explain_shocks_bp": ex_shocks, "explain_methods": ex_methods}, max_rows=40)

        # ------------------------------------------------------------------ run metadata + report
        meta = {
            "run_id": ctx.run_id, "business_date": cfg.business_date.isoformat(),
            "started_at_utc": ctx.started_at, "engine_version": __version__, "git_commit": _git_commit(),
            "python": platform.python_version(), "pyspark": pyspark.__version__,
            "pandas": pd.__version__, "numpy": np.__version__,
            "step_seconds": rep.timings, "warehouse_row_counts": wh.row_counts,
            "source_sha256": ctx.source_checksums, "config": cfg.raw,
        }
        (cfg.output_dir / "run_metadata.json").write_text(json.dumps(meta, indent=2, default=str), encoding="utf-8")
        header = (f"# Projection engine run report\n\n- run_id: `{ctx.run_id}`\n- business date: "
                  f"{cfg.business_date}\n- methods: {', '.join(cfg.methods)}\n- scenario sets: {n_by_set}\n"
                  f"- engine {__version__}, PySpark {pyspark.__version__}, Python {platform.python_version()}")
        report = rep.write_report(header)
        if not quiet:
            print(f"\nRun complete: {ctx.run_id}\nReport: {report}\nWarehouse: {cfg.warehouse_dir}")
        return results
    finally:
        spark.stop()


def _sort_by_tenor(pdf: pd.DataFrame, keys: list[str], rf_col: str = "rf_id") -> pd.DataFrame:
    tenor = pdf[rf_col].map(lambda r: tenor_label_to_years(str(r).rsplit(".", 1)[-1]))
    return pdf.assign(_t=tenor).sort_values([*keys, "_t"]).drop(columns="_t").reset_index(drop=True)


def _tenor_sorted(rf_ids: list[str]) -> list[str]:
    """Sort risk-factor ids like IR.USD.TSY.ZERO.10Y by tenor rather than alphabetically."""
    return sorted(rf_ids, key=lambda r: tenor_label_to_years(r.rsplit(".", 1)[-1]))
