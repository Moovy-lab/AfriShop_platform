"""Shared input, Delta output, quarantine, and structured logging helpers."""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def data_root() -> Path:
    """Return the configurable data directory."""
    return Path(os.getenv("DATA_DIR", "data"))


def read_source(spark: SparkSession, table: str, csv_source: bool = False) -> DataFrame:
    """Read a raw Delta job input from Parquet, falling back to the source CSV."""
    parquet_path = data_root() / "lakehouse" / "raw" / table
    csv_path = data_root() / "raw" / f"{table}.csv"
    if not csv_source and parquet_path.exists():
        return spark.read.parquet(str(parquet_path))
    if csv_path.exists():
        return spark.read.option("header", True).option("inferSchema", False).csv(str(csv_path))
    if parquet_path.exists():
        return spark.read.parquet(str(parquet_path))
    raise FileNotFoundError(f"No raw input found for {table}: {parquet_path} or {csv_path}")


def latest_per_key(df: DataFrame, key: str) -> DataFrame:
    """Keep one deterministic latest row per key."""
    order_col = "updated_at" if "updated_at" in df.columns else key
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_curated_row_number", F.row_number().over(window)).filter(
        F.col("_curated_row_number") == 1
    ).drop("_curated_row_number")


def write_delta_merge(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    key: str,
    partition_by: str | None = None,
) -> None:
    """Upsert a deduplicated dataframe into its curated Delta table."""
    path = str(data_root() / "lakehouse" / "curated" / table)
    source = latest_per_key(df, key)
    if not DeltaTable.isDeltaTable(spark, path):
        writer = source.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(partition_by)
        writer.save(path)
        return
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("target")
        .merge(source.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_quarantine(df: DataFrame, table: str) -> None:
    """Replace a table quarantine snapshot with the current rejected rows."""
    path = str(data_root() / "lakehouse" / "quarantine" / table)
    result = df
    if "_batch_id" in result.columns and "_source_batch" not in result.columns:
        result = result.withColumn("_source_batch", F.col("_batch_id"))
    if "_quarantined_at" not in result.columns:
        result = result.withColumn("_quarantined_at", F.current_timestamp())
    result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


class JsonFormatter(logging.Formatter):
    """Format log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "event_data", {})
        payload.update(
            timestamp=datetime.now(timezone.utc).isoformat(),
            level=record.levelname,
            message=record.getMessage(),
        )
        return json.dumps(payload, ensure_ascii=True, default=str)


def get_logger() -> logging.Logger:
    """Return the configured structured stdout logger."""
    logger = logging.getLogger("afrishop.curated")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(
    *,
    job: str,
    table: str,
    event: str,
    rows_in: int,
    rows_valid: int,
    rows_quarantined: int,
    rows_deduplicated: int,
    duration_seconds: float,
    run_id: str | None = None,
) -> None:
    """Write the required non-sensitive job counters to stdout."""
    get_logger().info(
        event,
        extra={
            "event_data": {
                "job": job,
                "table": table,
                "event": event,
                "rows_in": rows_in,
                "rows_valid": rows_valid,
                "rows_quarantined": rows_quarantined,
                "rows_deduplicated": rows_deduplicated,
                "duration_seconds": round(duration_seconds, 3),
                "run_id": run_id or str(uuid4()),
            }
        },
    )
