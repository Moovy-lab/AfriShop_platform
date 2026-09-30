"""Spark session factory for local and cluster Delta jobs."""

from __future__ import annotations

import os
import socket

from pyspark.sql import SparkSession


def get_spark(app_name: str = "afrishop-curated", cluster: bool | None = None) -> SparkSession:
    """Return a Spark session configured for Delta Lake and UTC timestamps."""
    use_cluster = cluster if cluster is not None else bool(os.getenv("SPARK_MASTER_URL"))
    master = os.getenv("SPARK_MASTER_URL") if use_cluster else "local[*]"
    if use_cluster and not master:
        raise ValueError("SPARK_MASTER_URL must be set when cluster mode is enabled")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
    )
    if use_cluster:
        builder = builder.config(
            "spark.driver.host", socket.gethostbyname(socket.gethostname())
        ).config("spark.driver.bindAddress", "0.0.0.0")
    return builder.getOrCreate()
