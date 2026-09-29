import os
import socket
from pyspark.sql import SparkSession


def get_spark(app_name: str, cluster: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL") if cluster else "local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if cluster:
        builder = (
            builder.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.memory", "2g")
        )
    return builder.getOrCreate()     