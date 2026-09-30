"""Shared helpers for table-specific curated transformations."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F


def timestamp_columns(df: DataFrame, *names: str) -> DataFrame:
    """Cast present timestamp fields using Spark's UTC session timezone."""
    for name in names:
        if name in df.columns:
            df = df.withColumn(name, F.to_timestamp(F.col(name)))
    return df


def numeric_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Cast present numeric fields to their declared Spark types."""
    for name, data_type in mapping.items():
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast(data_type))
    return df


def deduplicate(df: DataFrame, key: str) -> tuple[DataFrame, int]:
    """Keep the newest deterministic record for each primary key."""
    candidates = ("updated_at", "payment_date", "delivered_at", "shipped_at", "created_at")
    order_name = next((name for name in candidates if name in df.columns), None)
    order_col = F.col(order_name).desc_nulls_last() if order_name else F.lit(1)
    stable = F.sha2(F.to_json(F.struct(*[F.col(name) for name in df.columns])), 256)
    window = Window.partitionBy(key).orderBy(order_col, stable.desc())
    before = df.count()
    result = df.withColumn("_curated_row_number", F.row_number().over(window)).filter(
        F.col("_curated_row_number") == 1
    ).drop("_curated_row_number")
    return result, before - result.count()


def reject_by_reason(
    df: DataFrame, reasons: list[tuple[str, Column]]
) -> tuple[DataFrame, DataFrame]:
    """Split rows and concatenate every applicable rejection code."""
    if not reasons:
        return df, df.withColumn("reason", F.lit("")).filter(F.lit(False))
    codes = [F.when(condition, F.lit(code)) for code, condition in reasons]
    reason_array = F.filter(F.array(*codes), lambda value: value.isNotNull())
    tagged = df.withColumn("reason", F.concat_ws("|", reason_array))
    rejected = tagged.filter(F.col("reason") != "")
    valid = tagged.filter(F.col("reason") == "").drop("reason")
    return valid, rejected
