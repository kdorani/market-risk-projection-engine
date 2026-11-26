from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from .config import SOURCE_DIR, WAREHOUSE_DIR

SCHEMAS = {
    "positions": StructType([
        StructField("business_date", StringType(), False),
        StructField("trade_id", StringType(), False),
        StructField("position_id", StringType(), False),
        StructField("product_type", StringType(), False),
        StructField("issuer", StringType(), True),
        StructField("currency", StringType(), False),
        StructField("notional", DoubleType(), False),
        StructField("coupon_rate", DoubleType(), True),
        StructField("coupon_frequency", IntegerType(), True),
        StructField("remaining_maturity_years", IntegerType(), True),
        StructField("curve_id", StringType(), False),
        StructField("day_count", StringType(), True),
        StructField("pricing_method", StringType(), True),
    ]),
    "frtb_sensitivities": StructType([
        StructField("business_date", StringType(), False),
        StructField("trade_id", StringType(), False),
        StructField("risk_factor_key", StringType(), False),
        StructField("sensitivity_type", StringType(), False),
        StructField("sensitivity_value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("source_system", StringType(), False),
    ]),
    "scenario_shocks": StructType([
        StructField("business_date", StringType(), False),
        StructField("scenario_id", StringType(), False),
        StructField("scenario_name", StringType(), False),
        StructField("risk_factor_key", StringType(), False),
        StructField("shock_value", DoubleType(), False),
        StructField("shock_unit", StringType(), False),
    ]),
    "yield_curve_nodes": StructType([
        StructField("business_date", StringType(), False),
        StructField("curve_id", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("tenor_years", IntegerType(), False),
        StructField("zero_rate", DoubleType(), False),
        StructField("compounding", StringType(), False),
    ]),
    "pricing_grid": StructType([
        StructField("business_date", StringType(), False),
        StructField("trade_id", StringType(), False),
        StructField("curve_id", StringType(), False),
        StructField("grid_rate", DoubleType(), False),
        StructField("grid_price", DoubleType(), False),
        StructField("currency", StringType(), False),
    ]),
    "fo_sensitivities": StructType([
        StructField("business_date", StringType(), False),
        StructField("trade_id", StringType(), False),
        StructField("sensitivity_type", StringType(), False),
        StructField("sensitivity_value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("source_system", StringType(), False),
    ]),
}

SOURCE_FILES = {
    "positions": SOURCE_DIR / "position_data" / "positions.csv",
    "fo_sensitivities": SOURCE_DIR / "sensitivities_data" / "fo_sensitivities.csv",
    "frtb_sensitivities": SOURCE_DIR / "sensitivities_data" / "frtb_sensitivities.csv",
    "scenario_shocks": SOURCE_DIR / "shocks_data" / "scenario_shocks.csv",
    "yield_curve_nodes": SOURCE_DIR / "reference" / "yield_curve_nodes.csv",
    "pricing_grid": SOURCE_DIR / "reference" / "pricing_grid.csv",
}

def ingest_sources(spark: SparkSession) -> dict:
    """Read source extracts and persist internal Parquet tables."""
    tables = {}
    for table_name, source_path in SOURCE_FILES.items():
        if not Path(source_path).exists():
            raise FileNotFoundError(f"Missing source extract: {source_path}")
        df = (
            spark.read
            .option("header", True)
            .schema(SCHEMAS[table_name])
            .csv(str(source_path))
        )
        target = WAREHOUSE_DIR / table_name
        (
            df.write
            .mode("overwrite")
            .parquet(str(target))
        )
        tables[table_name] = df
    return tables
