"""Steps 9-10: vector validation, aggregation up the hierarchy, VaR/SVaR/ES and method
comparison against full revaluation."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..risk_measures import (
    expected_shortfall,
    ks_statistic,
    plat_style_zone,
    spearman,
    value_at_risk,
    var_scenario_index,
)

VECTOR_KEYS = ["scenario_set", "scenario_id", "scenario_date"]

RISK_SCHEMA = (
    "level string, node string, method string, scenario_set string, n_scenarios int, "
    "var_1d double, var_10d_sqrt_scaled double, es_1d double, worst_loss double, mean_pnl double, "
    "std_pnl double, var_scenario_id string, var_scenario_date string"
)
COMPARE_SCHEMA = (
    "level string, node string, scenario_set string, method string, n_scenarios int, "
    "mean_abs_diff double, max_abs_diff double, rmse double, var_method double, var_full_reval double, "
    "var_rel_diff double, spearman double, ks double, plat_style_zone string"
)


def validate_vectors(vectors: DataFrame, valid_positions: list[str], n_by_set: dict[str, int],
                     methods: list[str]) -> pd.DataFrame:
    """Completeness and sanity: every valid position x method x set has all scenarios."""
    counts = (
        vectors.groupBy("position_id", "method", "scenario_set")
        .agg(F.count("*").alias("n"), F.countDistinct("scenario_id").alias("n_distinct"),
             F.sum(F.when(F.col("pnl").isNull() | F.isnan("pnl"), 1).otherwise(0)).alias("n_bad"))
        .toPandas()
    )
    expected = pd.MultiIndex.from_product([valid_positions, methods, list(n_by_set)],
                                          names=["position_id", "method", "scenario_set"]).to_frame(index=False)
    out = expected.merge(counts, how="left", on=["position_id", "method", "scenario_set"])
    out["expected"] = out["scenario_set"].map(n_by_set)
    out[["n", "n_distinct", "n_bad"]] = out[["n", "n_distinct", "n_bad"]].fillna(0).astype(int)
    out["status"] = np.where(
        (out["n"] == out["expected"]) & (out["n_distinct"] == out["expected"]) & (out["n_bad"] == 0),
        "PASS", "FAIL")
    return out.sort_values(["position_id", "scenario_set", "method"]).reset_index(drop=True)


def aggregate_hierarchy(vectors: DataFrame, positions: DataFrame, levels: list[str]) -> DataFrame:
    """Sum PnL vectors scenario by scenario at each hierarchy level.
    VaR is never summed: diversification only exists at the vector level."""
    v = vectors.join(F.broadcast(positions.select("position_id", "book_id", "desk_id")), "position_id")
    node_col = {"POSITION": F.col("position_id"), "BOOK": F.col("book_id"),
                "DESK": F.col("desk_id"), "PORTFOLIO": F.lit("ALL")}
    parts = [
        v.groupBy(F.lit(level).alias("level"), node_col[level].alias("node"), "method", *VECTOR_KEYS)
        .agg(F.sum("pnl").alias("pnl"))
        for level in levels
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out


def risk_measures(agg: DataFrame, var_sets: list[str], confidence: float, es_confidence: float,
                  quantile_method: str, horizon_days: int) -> DataFrame:
    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("scenario_id")
        pnl = pdf["pnl"].to_numpy()
        k = var_scenario_index(pnl, confidence)
        var1 = value_at_risk(pnl, confidence, quantile_method)
        return pd.DataFrame([{
            "level": pdf["level"].iloc[0], "node": pdf["node"].iloc[0], "method": pdf["method"].iloc[0],
            "scenario_set": pdf["scenario_set"].iloc[0], "n_scenarios": len(pnl),
            "var_1d": var1, "var_10d_sqrt_scaled": var1 * math.sqrt(horizon_days),
            "es_1d": expected_shortfall(pnl, es_confidence), "worst_loss": float(-pnl.min()),
            "mean_pnl": float(pnl.mean()), "std_pnl": float(pnl.std(ddof=1)),
            "var_scenario_id": pdf["scenario_id"].iloc[k],
            "var_scenario_date": str(pdf["scenario_date"].iloc[k]),
        }])

    return (
        agg.filter(F.col("scenario_set").isin(var_sets))
        .groupBy("level", "node", "method", "scenario_set")
        .applyInPandas(fn, RISK_SCHEMA)
    )


def compare_methods(agg: DataFrame, var_sets: list[str], confidence: float, quantile_method: str) -> DataFrame:
    """Accuracy of each approximation against full revaluation (PLA-test style metrics).
    Only for the historical VaR/SVaR sets: distribution statistics on a dozen hand-picked
    stress scenarios would be meaningless (those are compared scenario by scenario instead)."""

    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        wide = pdf.pivot_table(index="scenario_id", columns="method", values="pnl", aggfunc="sum")
        if "FULL_REVALUATION" not in wide:
            return pd.DataFrame(columns=[c.split()[0] for c in COMPARE_SCHEMA.split(", ")])
        fr = wide["FULL_REVALUATION"].to_numpy()
        var_fr = value_at_risk(fr, confidence, quantile_method)
        rows = []
        for m in [c for c in wide.columns if c != "FULL_REVALUATION"]:
            x = wide[m].to_numpy()
            d = x - fr
            var_m = value_at_risk(x, confidence, quantile_method)
            rho, ks = spearman(x, fr), ks_statistic(x, fr)
            rows.append({
                "level": pdf["level"].iloc[0], "node": pdf["node"].iloc[0],
                "scenario_set": pdf["scenario_set"].iloc[0], "method": m, "n_scenarios": len(d),
                "mean_abs_diff": float(np.abs(d).mean()), "max_abs_diff": float(np.abs(d).max()),
                "rmse": float(np.sqrt((d**2).mean())), "var_method": var_m, "var_full_reval": var_fr,
                "var_rel_diff": (var_m - var_fr) / abs(var_fr) if var_fr else float("nan"),
                "spearman": rho, "ks": ks, "plat_style_zone": plat_style_zone(rho, ks),
            })
        return pd.DataFrame(rows)

    return (agg.filter(F.col("scenario_set").isin(var_sets))
            .groupBy("level", "node", "scenario_set").applyInPandas(fn, COMPARE_SCHEMA))


def diversification(risk: pd.DataFrame) -> pd.DataFrame:
    pos = risk[risk.level == "POSITION"].groupby(["method", "scenario_set"])["var_1d"].sum()
    port = risk[risk.level == "PORTFOLIO"].set_index(["method", "scenario_set"])["var_1d"]
    out = pd.DataFrame({"sum_of_position_var_1d": pos, "portfolio_var_1d": port}).reset_index()
    out["diversification_benefit"] = out["sum_of_position_var_1d"] - out["portfolio_var_1d"]
    out["diversification_pct"] = out["diversification_benefit"] / out["sum_of_position_var_1d"]
    return out.sort_values(["scenario_set", "method"])
