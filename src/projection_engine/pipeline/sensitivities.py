"""Step 6: canonical sensitivities and three-way reconciliation (FRTB vs FO vs engine)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..frtb import SENSITIVITY_CONVENTIONS
from .mapping import frtb_sens_key


def canonical_sensitivities(spark: SparkSession, staged: dict[str, DataFrame]) -> DataFrame:
    """Union the FRTB and FO feeds into one table with canonical units and sign.

    canonical DELTA = dPV per +1bp (negative for a long bond)
    canonical GAMMA = d2PV per bp^2
    """
    conv = spark.createDataFrame(
        [(s, t, u, m, float(f)) for (s, t, u), (m, f) in SENSITIVITY_CONVENTIONS.items()],
        "source_system string, sensitivity_type string, unit string, measure string, factor double",
    )
    frtb_df = staged["frtb_sensitivities"].select(
        "business_date", "position_id", "trade_id", "source_system",
        frtb_sens_key().alias("sens_rf_key"), "sensitivity_type", "sensitivity_value", "unit",
    )
    fo = staged["fo_sensitivities"].select(
        "business_date", "position_id", "trade_id", "source_system",
        F.col("risk_factor_name").alias("sens_rf_key"), "sensitivity_type", "sensitivity_value", "unit",
    )
    return (
        frtb_df.unionByName(fo)
        .join(F.broadcast(conv), ["source_system", "sensitivity_type", "unit"])
        .withColumn("value_canonical", F.col("sensitivity_value") * F.col("factor"))
        .withColumnRenamed("source_system", "sens_source")
        .select("business_date", "position_id", "trade_id", "sens_source", "sens_rf_key", "measure",
                "value_canonical", "sensitivity_type", "sensitivity_value", "unit")
    )


def reconcile_sensitivities(
    canon: DataFrame, sens_map: DataFrame, engine_rf: DataFrame, engine_vertex: DataFrame,
    rel_tol: float, abs_tol: float,
) -> DataFrame:
    """Compare each feed with the engine's own bump-and-revalue numbers.

    FRTB deltas are compared on FRTB vertices (same basis, same forward-bump definition);
    FO deltas/gammas on curve buckets (central differences).
    """
    frtb_df = (
        canon.filter((F.col("sens_source") == "FRTB") & (F.col("measure") == "DELTA"))
        .join(engine_vertex, ["position_id", "sens_rf_key"], "full_outer")
        .select("position_id", F.lit("FRTB_vs_ENGINE").alias("comparison"),
                F.col("sens_rf_key").alias("risk_factor"), F.lit("DELTA").alias("measure"),
                F.col("value_canonical").alias("source_value"), F.col("engine_delta_per_bp").alias("engine_value"))
    )
    fo_map = sens_map.filter(F.col("sens_source") == "FO").select("sens_rf_key", "rf_id")
    fo = canon.filter(F.col("sens_source") == "FO").join(fo_map, "sens_rf_key")
    eng = engine_rf.select("position_id", "rf_id", "delta_per_bp", "gamma_per_bp2")
    fo_cmp = fo.join(eng, ["position_id", "rf_id"], "full_outer").select(
        "position_id", F.lit("FO_vs_ENGINE").alias("comparison"), F.col("rf_id").alias("risk_factor"),
        F.coalesce("measure", F.lit("DELTA")).alias("measure"),
        F.col("value_canonical").alias("source_value"),
        F.when(F.col("measure") == "GAMMA", F.col("gamma_per_bp2")).otherwise(F.col("delta_per_bp")).alias("engine_value"),
    )
    out = frtb_df.unionByName(fo_cmp)
    tol = F.greatest(F.lit(abs_tol) * F.when(F.col("measure") == "GAMMA", F.lit(0.01)).otherwise(F.lit(1.0)),
                     F.lit(rel_tol) * F.abs("engine_value"))
    return (
        out.withColumn("diff", F.col("source_value") - F.col("engine_value"))
        .withColumn("rel_diff", F.when(F.col("engine_value") != 0, F.col("diff") / F.abs("engine_value")))
        .withColumn("tolerance", tol)
        .withColumn(
            "status",
            F.when(F.col("source_value").isNull(), "MISSING_IN_SOURCE")
            .when(F.col("engine_value").isNull(), "MISSING_IN_ENGINE")
            .when(F.abs("diff") <= F.col("tolerance"), "PASS")
            .otherwise("BREAK"),
        )
        .orderBy("position_id", "comparison", "measure", "risk_factor")
    )
