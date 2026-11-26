from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs as spark_abs, max as spark_max, min as spark_min

def validate_projection_output(result: DataFrame) -> None:
    required = {"trade_id", "scenario_id", "methodology", "pnl", "base_pv", "scenario_pv"}
    missing = required - set(result.columns)
    if missing:
        raise ValueError(f"Output missing columns: {sorted(missing)}")
    if result.filter(col("pnl").isNull()).count():
        raise ValueError("Null P&L values found")
    if result.filter(col("scenario_pv").isNull()).count():
        raise ValueError("Null scenario PV values found")
    expected_methods = {"FULL_REVALUATION", "TAYLOR", "GRID", "LADDER"}
    actual_methods = {r["methodology"] for r in result.select("methodology").distinct().collect()}
    if actual_methods != expected_methods:
        raise ValueError(f"Unexpected methodology set: {actual_methods}")
    duplicates = (
        result.groupBy("trade_id", "scenario_id", "methodology")
        .count().filter(col("count") > 1)
    )
    if duplicates.count():
        raise ValueError("Duplicate trade/scenario/methodology output rows")
