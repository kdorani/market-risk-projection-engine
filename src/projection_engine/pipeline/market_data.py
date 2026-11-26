"""Step 3 helpers: build the driver-side MarketState and canonical scenario shocks.

Curves, calendars and the scenario matrix are small (KBs-MBs) so they are collected to the
driver and shipped to executors with the UDF closures. Positions and PnL vectors, which are
the large tables, stay distributed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..conventions import HolidayCalendar
from ..curves import MarketState, ScenarioShocks, ZeroCurve, linear_interp_weights
from ..frtb import SHOCK_UNIT_FACTORS


def build_market_state(staged: dict[str, DataFrame], business_date) -> tuple[MarketState, pd.DataFrame]:
    yc = staged["yield_curves"].toPandas()
    curves = {}
    for cid, g in yc.groupby("curve_id"):
        g = g.sort_values("tenor_years")
        curves[cid] = ZeroCurve(
            curve_id=cid, currency=g["currency"].iloc[0], tenor_labels=tuple(g["tenor_label"]),
            tenors=g["tenor_years"].to_numpy(float), zero_rates=g["zero_rate"].to_numpy(float),
            compounding=g["compounding"].iloc[0], interpolation=g["interpolation"].iloc[0],
        )
    hol = staged["holiday_calendars"].toPandas()
    calendars = {
        cid: HolidayCalendar.from_dates(cid, g["holiday_date"].tolist()) for cid, g in hol.groupby("calendar_id")
    }
    # Curve node -> scenario risk factor mapping (identity when scenario pillars = curve pillars)
    rf = staged["risk_factors"].toPandas()
    node_rf, rows = {}, []
    for cid, curve in curves.items():
        crf = rf[(rf["curve_id"] == cid) & (rf["rf_type"] == "ZERO_RATE")].sort_values("tenor_years")
        if crf.empty:
            raise ValueError(f"No scenario risk factors defined for curve {cid}")
        rf_ids = crf["rf_id"].tolist()
        w = linear_interp_weights(crf["tenor_years"].to_numpy(float), curve.tenors).T  # n_nodes x n_rf
        node_rf[cid] = (rf_ids, w)
        for i, label in enumerate(curve.tenor_labels):
            for j in np.nonzero(w[i])[0]:
                rows.append({"curve_id": cid, "node_label": label, "node_tenor_years": curve.tenors[i],
                             "zero_rate": curve.zero_rates[i], "rf_id": rf_ids[j], "weight": w[i, j]})
    market = MarketState(business_date, curves, calendars, node_rf)
    return market, pd.DataFrame(rows)


def canonical_scenarios(spark: SparkSession, staged: dict[str, DataFrame], scenario_sets: list[str]) -> DataFrame:
    """Join shocks to their risk-factor definition and convert to canonical units."""
    factors = spark.createDataFrame(
        [(a, b, float(f)) for (a, b), f in SHOCK_UNIT_FACTORS.items()],
        "shock_type string, shock_unit string, unit_factor double",
    )
    rf = staged["risk_factors"].select("rf_id", "shock_type", "shock_unit", "tenor_years")
    return (
        staged["scenario_shocks"].filter(F.col("scenario_set").isin(scenario_sets))
        .join(F.broadcast(rf), ["rf_id", "shock_unit"])
        .join(F.broadcast(factors), ["shock_type", "shock_unit"])
        .withColumn("shock_value_canonical", F.col("shock_value") * F.col("unit_factor"))
        .withColumn("canonical_unit", F.when(F.col("shock_type") == "ABSOLUTE", "BP").otherwise("DECIMAL"))
        .select("business_date", "scenario_set", "scenario_id", "scenario_date", "scenario_name",
                "rf_id", "tenor_years", "shock_type", "shock_value", "shock_unit",
                "shock_value_canonical", "canonical_unit")
    )


def scenario_statistics(scen: DataFrame) -> pd.DataFrame:
    stats = (
        scen.groupBy("scenario_set", "rf_id", "tenor_years")
        .agg(
            F.count("*").alias("n"),
            F.mean("shock_value_canonical").alias("mean_bp"),
            F.stddev("shock_value_canonical").alias("std_bp"),
            F.min("shock_value_canonical").alias("min_bp"),
            F.expr("percentile(shock_value_canonical, 0.01)").alias("p01_bp"),
            F.expr("percentile(shock_value_canonical, 0.99)").alias("p99_bp"),
            F.max("shock_value_canonical").alias("max_bp"),
        )
        .orderBy("scenario_set", "tenor_years")
        .toPandas()
    )
    return stats.drop(columns=["tenor_years"])


def collect_shocks(scen: DataFrame) -> ScenarioShocks:
    pdf = scen.select("scenario_set", "scenario_id", "scenario_date", "rf_id", "shock_value_canonical").toPandas()
    # Same "YYYY-MM-DD" (or "" for hypothetical scenarios) format Spark's cast("string") gives
    pdf["scenario_date"] = pd.to_datetime(pdf["scenario_date"]).dt.strftime("%Y-%m-%d").fillna("")
    return ScenarioShocks.from_long(pdf)
