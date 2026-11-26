from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat

def map_bond_risk_factors(positions: DataFrame, curve_nodes: DataFrame) -> DataFrame:
    """
    Demonstration mapping: position currency + curve_id + tenor -> risk-factor key.
    In production, use governed mapping/reference tables rather than string construction.
    """
    return (
        positions.select("trade_id", "position_id", "currency", "curve_id")
        .crossJoin(curve_nodes.select("tenor_years").distinct())
        .withColumn(
            "risk_factor_key",
            concat(col("curve_id"), lit("_"), col("tenor_years").cast("string"), lit("Y"))
        )
    )
