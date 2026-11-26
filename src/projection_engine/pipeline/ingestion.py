"""Step 1: read every upstream table (CSV extract or JDBC) into engine staging tables."""

from __future__ import annotations

import hashlib
import os

import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import SourceConfig
from ..schemas import SOURCE_TABLES, SourceTable
from .context import RunContext


def _read_csv(spark: SparkSession, table: SourceTable, src: SourceConfig) -> DataFrame:
    path = src.path / f"{table.source_object}.csv"
    if not path.exists():
        raise FileNotFoundError(f"{table.system}: missing source extract {path}")
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")  # malformed rows fail the run instead of becoming nulls
        .option("dateFormat", "yyyy-MM-dd")
        .schema(table.schema)
        .csv(str(path))
    )


def _read_jdbc(spark: SparkSession, table: SourceTable, src: SourceConfig) -> DataFrame:
    """Database read. Credentials come from environment variables, never from config/git."""
    opts = src.options
    reader = (
        spark.read.format("jdbc")
        .option("url", opts["url"])
        .option("dbtable", f"{opts.get('schema', 'dbo')}.{table.source_object}")
        .option("user", os.environ[opts["user_env"]])
        .option("password", os.environ[opts["password_env"]])
    )
    if "driver" in opts:
        reader = reader.option("driver", opts["driver"])
    df = reader.load()
    return df.select([F.col(f.name).cast(f.dataType).alias(f.name) for f in table.schema])


READERS = {"csv": _read_csv, "jdbc": _read_jdbc}


def _sha256(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest_sources(ctx: RunContext) -> tuple[dict[str, DataFrame], pd.DataFrame]:
    staged: dict[str, DataFrame] = {}
    summary = []
    for table in SOURCE_TABLES:
        src = ctx.cfg.sources[table.system]
        df = READERS[src.type](ctx.spark, table, src)
        raw_rows = df.count()
        if table.has_business_date:
            df = df.filter(F.col("business_date") == F.lit(ctx.cfg.business_date))
        df = (
            df.withColumn("_source_system", F.lit(table.system))
            .withColumn("_source_object", F.lit(table.source_object))
            .withColumn("_load_ts", F.current_timestamp())
        )
        staged[table.name] = ctx.warehouse.write(f"stg_{table.name}", df)
        checksum = ""
        if src.type == "csv":
            checksum = _sha256(src.path / f"{table.source_object}.csv")
            ctx.source_checksums[f"{table.system}/{table.source_object}.csv"] = checksum
        summary.append({
            "source_system": table.system,
            "source_object": table.source_object,
            "staging_table": f"stg_{table.name}",
            "rows_read": raw_rows,
            "rows_for_business_date": ctx.warehouse.row_counts[f"stg_{table.name}"],
            "columns": len(table.schema.fields),
            "primary_key": ", ".join(table.primary_key),
            "sha256_prefix": checksum[:12],
        })
    return staged, pd.DataFrame(summary)
