from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import DoubleType, StringType
from .pricing import (
    fixed_bond_cashflows, pv_continuous, full_revaluation_pnl,
    taylor_pnl, grid_pnl, ladder_pnl
)

def run_bond_projection(tables: dict, output_dir: Path, risk_factor_mapping: DataFrame) -> DataFrame:
    """
    Run one fixed-coupon bond through four methods.
    Spark handles source joins, scenario expansion and output persistence.
    Small reference vectors are collected/broadcast for this deliberately small demo.
    """
    positions = tables["positions"].filter(col("product_type") == "FIXED_RATE_BOND")
    if positions.count() == 0:
        raise ValueError("No FIXED_RATE_BOND position found")

    position = positions.first()
    trade_id = position["trade_id"]
    notional = float(position["notional"])
    coupon_rate = float(position["coupon_rate"])
    maturity = int(position["remaining_maturity_years"])
    frequency = int(position["coupon_frequency"])
    curve_id = position["curve_id"]

    curve = (
        tables["yield_curve_nodes"]
        .filter(col("curve_id") == curve_id)
        .orderBy("tenor_years")
        .collect()
    )
    if not curve:
        raise ValueError(f"No curve nodes found for {curve_id}")
    # Simplification for v1: use the 5Y node as a flat zero rate.
    base_rate = float(next(x["zero_rate"] for x in curve if x["tenor_years"] == maturity))

    cashflows = fixed_bond_cashflows(notional, coupon_rate, maturity, frequency)
    base_pv = pv_continuous(cashflows, base_rate)

    sens = (
        tables["frtb_sensitivities"]
        .filter(col("trade_id") == trade_id)
        .collect()
    )
    dv01_row = next(x for x in sens if x["sensitivity_type"] == "DV01")
    convexity_row = next(x for x in sens if x["sensitivity_type"] == "CONVEXITY")
    dv01 = abs(float(dv01_row["sensitivity_value"]))
    convexity = float(convexity_row["sensitivity_value"])

    mapped_keys = {
        row["risk_factor_key"]
        for row in risk_factor_mapping.filter(col("trade_id") == trade_id)
        .select("risk_factor_key").distinct().collect()
    }
    key_rate_sensitivities = [
        x for x in sens if x["sensitivity_type"] == "KEY_RATE_DV01"
    ]
    unmapped = {x["risk_factor_key"] for x in key_rate_sensitivities} - mapped_keys
    if unmapped:
        raise ValueError(f"Unmapped RDL risk factors for {trade_id}: {sorted(unmapped)}")
    key_rate_rows = [
        (int(x["risk_factor_key"].rsplit("_", 1)[1].replace("Y", "")),
         float(x["sensitivity_value"]))
        for x in key_rate_sensitivities
    ]

    grid_rows = (
        tables["pricing_grid"]
        .filter(col("trade_id") == trade_id)
        .orderBy("grid_rate")
        .collect()
    )
    grid = [(float(x["grid_rate"]), float(x["grid_price"])) for x in grid_rows]

    # v1 scenarios are parallel shifts. The ladder method applies the same
    # scenario shock to each mapped tenor bucket, then weights by key-rate DV01.
    # A later extension can store a separate MDSS shock for each tenor key.
    scenarios = tables["scenario_shocks"].filter(
        col("risk_factor_key") == "USD_OIS_PARALLEL"
    )

    # Build scenario rows with Spark, then calculate each method through UDFs.
    # For SCN_005, the flat shock is zero, but ladder shocks are non-parallel.
    scenario_rows = scenarios.select(
        "business_date", "scenario_id", "scenario_name", "shock_value", "shock_unit"
    )

    cashflows_bc = cashflows
    grid_bc = grid
    key_rate_bc = key_rate_rows
    base_rate_bc = base_rate
    base_pv_bc = base_pv
    dv01_bc = dv01
    convexity_bc = convexity

    def full_udf(shock):
        return full_revaluation_pnl(cashflows_bc, base_rate_bc, float(shock))[2]

    def taylor_udf(shock):
        return taylor_pnl(base_pv_bc, dv01_bc, convexity_bc, float(shock))

    def grid_udf(shock):
        return grid_pnl(grid_bc, base_rate_bc, float(shock), base_pv_bc)[1]

    def ladder_udf(shock):
        tenor_shocks = [(tenor, float(shock)) for tenor, _ in key_rate_bc]
        return ladder_pnl(key_rate_bc, tenor_shocks)

    full_fn = udf(full_udf, DoubleType())
    taylor_fn = udf(taylor_udf, DoubleType())
    grid_fn = udf(grid_udf, DoubleType())
    ladder_fn = udf(ladder_udf, DoubleType())

    expanded = (
        scenario_rows
        .withColumn("trade_id", lit(trade_id))
        .withColumn("position_id", lit(position["position_id"]))
        .withColumn("product_type", lit(position["product_type"]))
        .withColumn("currency", lit(position["currency"]))
        .withColumn("base_pv", lit(float(base_pv)))
        .withColumn("base_rate", lit(float(base_rate)))
        .withColumn("full_revaluation_pnl", full_fn(col("shock_value")))
        .withColumn("taylor_pnl", taylor_fn(col("shock_value")))
        .withColumn("grid_pnl", grid_fn(col("shock_value")))
        .withColumn("ladder_pnl", ladder_fn(col("shock_value")))
    )

    # Convert wide method columns to long-form output.
    from pyspark.sql.functions import expr
    result = expanded.selectExpr(
        "business_date", "trade_id", "position_id", "product_type", "currency",
        "scenario_id", "scenario_name", "shock_value", "shock_unit", "base_pv",
        "base_rate",
        "stack(4, "
        "'FULL_REVALUATION', full_revaluation_pnl, "
        "'TAYLOR', taylor_pnl, "
        "'GRID', grid_pnl, "
        "'LADDER', ladder_pnl) as (methodology, pnl)"
    )
    result = result.withColumn("scenario_pv", col("base_pv") + col("pnl"))
    result.write.mode("overwrite").partitionBy("business_date").parquet(str(output_dir))
    return result
