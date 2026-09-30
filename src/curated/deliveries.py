"""Curated deliveries job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from curated._common import deduplicate, numeric_columns, reject_by_reason, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from spark_session import get_spark


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Deduplicate deliveries and quarantine impossible delivery timelines."""
    df = timestamp_columns(df, "shipped_at", "delivered_at")
    df = numeric_columns(df, {"delivery_attempts": "integer", "delivery_cost": "decimal(18,2)"})
    df, duplicates = deduplicate(df, "delivery_id")
    reason = F.col("delivered_at").isNotNull() & (
        F.col("delivered_at") < F.col("shipped_at")
    )
    valid, rejected = reject_by_reason(df, [("delivered_before_shipped", reason)])
    return valid, rejected, duplicates


def run(csv_source: bool = False) -> None:
    """Run deliveries transformation."""
    started = time.monotonic()
    spark = get_spark("curated-deliveries")
    source = read_source(spark, "deliveries", csv_source)
    rows_in = source.count()
    valid, rejected, duplicates = transform(source)
    rows_valid, rows_quarantined = valid.count(), rejected.count()
    write_delta_merge(spark, valid, "deliveries", "delivery_id")
    write_quarantine(rejected, "deliveries")
    log_event(
        job="deliveries", table="deliveries", event="completed", rows_in=rows_in,
        rows_valid=rows_valid, rows_quarantined=rows_quarantined,
        rows_deduplicated=duplicates, duration_seconds=time.monotonic() - started,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", action="store_true")
    run(csv_source=parser.parse_args().csv)
