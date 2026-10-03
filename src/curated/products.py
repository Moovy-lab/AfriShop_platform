"""Curated products job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from curated._common import deduplicate, numeric_columns, reject_by_reason, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from spark_session import get_spark

TABLE = "products"
KEY = "product_id"


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Cast product fields, flag negative margins, and quarantine zero prices."""
    df = timestamp_columns(df, "created_at", "updated_at")
    df = numeric_columns(df, {"unit_cost": "decimal(18,2)", "unit_price": "decimal(18,2)"})
    if "is_perishable" in df.columns:
        df = df.withColumn("is_perishable", F.col("is_perishable").cast("boolean"))
    df = df.withColumn(
        "is_negative_margin",
        (F.col("unit_cost") > F.col("unit_price")) & (F.col("unit_price") > 0),
    )
    return reject_by_reason(df, [("unit_price_zero", F.col("unit_price") == 0)])


def run(csv_source: bool = False) -> None:
    """Run the products transformation and write curated outputs."""
    started = time.monotonic()
    spark = get_spark("curated-products")
    source = read_source(spark, TABLE, csv_source)
    rows_in = source.count()
    source = timestamp_columns(source, "created_at", "updated_at")
    source, duplicates = deduplicate(source, KEY)
    valid, rejected = transform(source)
    rows_valid = valid.count()
    rows_quarantined = rejected.count()
    write_delta_merge(spark, valid, TABLE, KEY)
    write_quarantine(rejected, TABLE)
    log_event(
        job=TABLE,
        table=TABLE,
        event="completed",
        rows_in=rows_in,
        rows_valid=rows_valid,
        rows_quarantined=rows_quarantined,
        rows_deduplicated=duplicates,
        duration_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true", help="Read data/raw/products.csv")
    run(csv_source=parser.parse_args().csv)
