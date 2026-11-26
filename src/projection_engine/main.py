import csv
from pathlib import Path
from pyspark.sql import functions as F
from .spark_session import get_spark
from .ingestion import ingest_sources
from .engine import run_bond_projection
from .validation import validate_projection_output
from .config import OUTPUT_DIR, PROJECT_ROOT

def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = ingest_sources(spark)

        # Demonstrate mapping output and persist it as an internal table.
        from .mapping import map_bond_risk_factors
        mapping = map_bond_risk_factors(tables["positions"], tables["yield_curve_nodes"])
        mapping_path = PROJECT_ROOT / "data" / "warehouse" / "position_risk_factor_mapping"
        mapping.write.mode("overwrite").parquet(str(mapping_path))

        output_path = OUTPUT_DIR / "pnl_vectors"
        result = run_bond_projection(tables, output_path, mapping)
        validate_projection_output(result)

        print("\\n=== POSITION / SCENARIO / METHODOLOGY P&L ===")
        result.orderBy("scenario_id", "methodology").show(100, truncate=False)

        summary = (
            result.groupBy("scenario_id", "scenario_name", "methodology")
            .agg(F.round(F.first("pnl"), 2).alias("pnl"))
            .orderBy("scenario_id", "methodology")
        )
        summary_path = OUTPUT_DIR / "pnl_summary"
        summary.coalesce(1).write.mode("overwrite").option("header", True).csv(str(summary_path))

        print("\\n=== P&L SUMMARY ===")
        summary.show(100, truncate=False)
        print(f"\\nParquet vectors written to: {output_path}")
        print(f"Summary CSV directory: {summary_path}")
        print(f"Internal Parquet tables written under: {PROJECT_ROOT / 'data' / 'warehouse'}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
