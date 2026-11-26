from pyspark.sql import SparkSession
from .config import SPARK_APP_NAME

def get_spark(app_name: str = SPARK_APP_NAME) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
