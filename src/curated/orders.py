"""Curated orders job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from curated._common import deduplicate, numeric_columns, reject_by_reason, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from spark_session import get_spark


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Deduplicate orders and quarantine inconsistent order totals."""
    df = timestamp_columns(df, "order_date", "created_at", "updated_at")
    df = numeric_columns(df, {name: "decimal(18,2)" for name in (
        "subtotal_amount", "shipping_amount", "discount_amount", "total_amount"
    )})
    df, duplicates = deduplicate(df, "order_id")
    df = df.withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
    delta = F.abs(
        F.col("total_amount")
        - (F.col("subtotal_amount") + F.col("shipping_amount") - F.col("discount_amount"))
    )
    valid, rejected = reject_by_reason(df, [("order_total_mismatch", delta > F.lit(0.05))])
    return valid, rejected, duplicates


def run(csv_source: bool = False) -> None:
    """Run orders transformation and partition output by order month."""
    started = time.monotonic()
    spark = get_spark("curated-orders")
    source = read_source(spark, "orders", csv_source)
    rows_in = source.count()
    valid, rejected, duplicates = transform(source)
    rows_valid, rows_quarantined = valid.count(), rejected.count()
    write_delta_merge(spark, valid, "orders", "order_id", "order_year_month")
    write_quarantine(rejected, "orders")
    log_event(
        job="orders", table="orders", event="completed", rows_in=rows_in,
        rows_valid=rows_valid, rows_quarantined=rows_quarantined,
        rows_deduplicated=duplicates, duration_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true")
    run(csv_source=parser.parse_args().csv)
