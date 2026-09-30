"""Curated order lines job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from curated._common import deduplicate, numeric_columns, reject_by_reason, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from spark_session import get_spark


def transform(df: DataFrame, product_ids: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Quarantine orphan products and inconsistent extended line amounts."""
    df = timestamp_columns(df)
    df = numeric_columns(
        df,
        {
            "quantity": "integer",
            "unit_price": "decimal(18,2)",
            "line_discount": "decimal(18,2)",
            "line_total": "decimal(18,2)",
        },
    )
    df, duplicates = deduplicate(df, "order_line_id")
    known = product_ids.select("product_id").dropDuplicates()
    df = df.join(known.withColumn("_known_product", F.lit(True)), "product_id", "left")
    orphan = F.col("_known_product").isNull()
    expected = F.col("quantity") * F.col("unit_price") - F.col("line_discount")
    mismatch = F.abs(F.col("line_total") - expected) > F.lit(0.05)
    valid, rejected = reject_by_reason(
        df,
        [("unknown_product_id", orphan), ("line_total_mismatch", mismatch)],
    )
    valid = valid.drop("_known_product")
    rejected = rejected.drop("_known_product")
    return valid, rejected, duplicates


def run(csv_source: bool = False) -> None:
    """Run order line transformation against the complete product source."""
    started = time.monotonic()
    spark = get_spark("curated-order-lines")
    source = read_source(spark, "order_lines", csv_source)
    products = read_source(spark, "products", csv_source)
    rows_in = source.count()
    valid, rejected, duplicates = transform(source, products.select("product_id"))
    rows_valid, rows_quarantined = valid.count(), rejected.count()
    write_delta_merge(spark, valid, "order_lines", "order_line_id")
    write_quarantine(rejected, "order_lines")
    log_event(
        job="order_lines",
        table="order_lines",
        event="completed",
        rows_in=rows_in,
        rows_valid=rows_valid,
        rows_quarantined=rows_quarantined,
        rows_deduplicated=duplicates,
        duration_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true")
    run(csv_source=parser.parse_args().csv)
