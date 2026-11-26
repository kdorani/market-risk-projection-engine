"""Local SparkSession for development. On a cluster, the session is provided by the platform."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_PACKAGE_PARENT = str(Path(__file__).resolve().parents[1])  # .../src


def get_spark(app_name: str = "MarketRiskProjectionEngine", master: str = "local[*]",
              shuffle_partitions: int = 8, log_level: str = "WARN") -> SparkSession:
    # Make Python workers use the same interpreter / virtualenv as the driver, and let them
    # import this package even when it is run from the source tree (not pip-installed).
    # On a real cluster ship the code instead: pip-install the wheel on executors or --py-files.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    paths = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if _PACKAGE_PARENT not in paths:
        os.environ["PYTHONPATH"] = os.pathsep.join([_PACKAGE_PARENT, *paths])
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        # Arrow (pandas UDFs) on Java 17+ needs reflective access to direct buffers
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .config("spark.executor.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark
