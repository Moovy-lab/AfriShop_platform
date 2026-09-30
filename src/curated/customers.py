"""Curated customers job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from curated._common import numeric_columns, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from pii import hash_column
from spark_session import get_spark


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Hash contact fields, link duplicate accounts, and quarantine ID collisions."""
    df = timestamp_columns(df, "registration_date")
    df = numeric_columns(df, {"birth_year": "integer"})
    for name in ("email_hash", "phone_hash"):
        if name in df.columns:
            df = df.withColumn(name, hash_column(name))
    if "email_hash" in df.columns:
        master_window = Window.partitionBy("email_hash").orderBy(
            F.col("registration_date").asc_nulls_last(), F.col("customer_id").asc()
        )
        df = df.withColumn(
            "customer_master_id",
            F.first("customer_id", ignorenulls=True).over(master_window),
        )
    else:
        df = df.withColumn("customer_master_id", F.col("customer_id"))

    identity = F.concat_ws(
        "|",
        F.coalesce(F.col("email_hash"), F.lit("")),
        F.coalesce(F.col("phone_hash"), F.lit("")),
    )
    collisions = (
        df.groupBy("customer_id")
        .agg(F.countDistinct(identity).alias("_identity_count"))
        .filter(F.col("_identity_count") > 1)
        .select("customer_id")
    )
    df = df.join(collisions.withColumn("_is_collision", F.lit(True)), "customer_id", "left")
    stable = F.sha2(F.to_json(F.struct(*[F.col(name) for name in df.columns])), 256)
    window = Window.partitionBy("customer_id").orderBy(
        F.col("registration_date").asc_nulls_last(), stable.asc()
    )
    df = df.withColumn("_customer_row", F.row_number().over(window))
    collision = F.coalesce(F.col("_is_collision"), F.lit(False))
    collision_duplicate = collision & (F.col("_customer_row") > 1)
    valid = df.filter(F.col("_customer_row") == 1)
    rejected = df.filter(collision_duplicate).withColumn(
        "reason", F.lit("customer_id_collision")
    )
    duplicate_count = df.count() - valid.count() - rejected.count()
    valid = valid.drop("_is_collision", "_customer_row")
    rejected = rejected.drop("_is_collision", "_customer_row")
    return valid, rejected, duplicate_count


def run(csv_source: bool = False) -> None:
    """Run customer transformation."""
    started = time.monotonic()
    spark = get_spark("curated-customers")
    source = read_source(spark, "customers", csv_source)
    rows_in = source.count()
    valid, rejected, duplicates = transform(source)
    rows_valid, rows_quarantined = valid.count(), rejected.count()
    write_delta_merge(spark, valid, "customers", "customer_id")
    write_quarantine(rejected, "customers")
    log_event(
        job="customers", table="customers", event="completed", rows_in=rows_in,
        rows_valid=rows_valid, rows_quarantined=rows_quarantined,
        rows_deduplicated=duplicates, duration_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true")
    run(csv_source=parser.parse_args().csv)
