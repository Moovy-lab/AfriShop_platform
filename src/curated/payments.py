"""Curated payments job."""

from __future__ import annotations

import argparse
import time

from pyspark.sql import DataFrame

from curated._common import deduplicate, numeric_columns, reject_by_reason, timestamp_columns
from curated_io import log_event, read_source, write_delta_merge, write_quarantine
from spark_session import get_spark


def transform(df: DataFrame) -> tuple[DataFrame, DataFrame, int]:
    """Retain each payment attempt and remove only duplicate payment identifiers."""
    df = timestamp_columns(df, "payment_date")
    df = numeric_columns(df, {"payment_amount": "decimal(18,2)"})
    df, duplicates = deduplicate(df, "payment_id")
    valid, rejected = reject_by_reason(df, [])
    return valid, rejected, duplicates


def run(csv_source: bool = False) -> None:
    """Run payments transformation."""
    started = time.monotonic()
    spark = get_spark("curated-payments")
    source = read_source(spark, "payments", csv_source)
    rows_in = source.count()
    valid, rejected, duplicates = transform(source)
    rows_valid, rows_quarantined = valid.count(), rejected.count()
    write_delta_merge(spark, valid, "payments", "payment_id")
    write_quarantine(rejected, "payments")
    log_event(
        job="payments",
        table="payments",
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
