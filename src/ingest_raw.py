"""Ingest the six synthetic CSV sources into date-partitioned Parquet."""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

SOURCES = ("orders", "order_lines", "customers", "products", "payments", "deliveries")


class JsonFormatter(logging.Formatter):
    """Format each log event as one JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "level": record.levelname,
                "message": record.getMessage(),
                **getattr(record, "event_data", {}),
            },
            ensure_ascii=True,
            default=str,
        )


def get_logger() -> logging.Logger:
    """Create the ingestion JSON logger once."""
    logger = logging.getLogger("afrishop.ingest_raw")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def ingest_source(
    source: str,
    raw_dir: Path,
    lakehouse_dir: Path,
    ingestion_date: str,
) -> dict[str, Any]:
    """Write one CSV into its deterministic date partition."""
    started = time.monotonic()
    csv_path = raw_dir / f"{source}.csv"
    output_dir = lakehouse_dir / source / f"ingestion_date={ingestion_date}"
    output_file = output_dir / "data.parquet"
    result: dict[str, Any] = {
        "source": source,
        "ingestion_date": ingestion_date,
        "status": "failed",
        "rows": 0,
        "duration_seconds": 0.0,
    }
    frame = pd.read_csv(csv_path)
    frame["_ingested_at"] = pd.Timestamp(f"{ingestion_date}T00:00:00Z")
    frame["_source_file"] = csv_path.name
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary_file = output_dir / "data.parquet.tmp"
    try:
        frame.to_parquet(temporary_file, engine="pyarrow", compression="snappy")
        temporary_file.replace(output_file)
    finally:
        temporary_file.unlink(missing_ok=True)
    result.update(
        status="success",
        rows=len(frame),
        duration_seconds=round(time.monotonic() - started, 3),
        output=str(output_file),
    )
    return result


def run(
    *,
    raw_dir: Path | None = None,
    lakehouse_dir: Path | None = None,
    ingestion_date: str | None = None,
    log_dir: Path | None = None,
    sources: tuple[str, ...] = SOURCES,
) -> list[dict[str, Any]]:
    """Ingest every required source and write a JSON run report."""
    data_dir = Path(os.getenv("DATA_DIR", "data"))
    resolved_raw_dir = raw_dir or data_dir / "raw"
    resolved_lakehouse_dir = lakehouse_dir or data_dir / "lakehouse" / "raw"
    resolved_log_dir = log_dir or Path(os.getenv("INGEST_LOG_DIR", "logs"))
    run_date = ingestion_date or os.getenv("INGESTION_DATE") or date.today().isoformat()
    date.fromisoformat(run_date)
    resolved_log_dir.mkdir(parents=True, exist_ok=True)
    logger = get_logger()

    missing = [source for source in sources if not (resolved_raw_dir / f"{source}.csv").is_file()]
    if missing:
        results = [
            {
                "source": source,
                "ingestion_date": run_date,
                "status": "missing" if source in missing else "not_started",
                "rows": 0,
            }
            for source in sources
        ]
        _write_report(resolved_log_dir, run_date, results)
        logger.error(
            "Required source files are missing", extra={"event_data": {"missing": missing}}
        )
        raise FileNotFoundError(f"Required CSV source files are missing: {', '.join(missing)}")

    results = [
        ingest_source(source, resolved_raw_dir, resolved_lakehouse_dir, run_date)
        for source in sources
    ]
    _write_report(resolved_log_dir, run_date, results)
    logger.info(
        "Raw ingestion completed",
        extra={
            "event_data": {
                "ingestion_date": run_date,
                "source_count": len(results),
                "rows_total": sum(int(item["rows"]) for item in results),
            }
        },
    )
    return results


def _write_report(log_dir: Path, run_date: str, results: list[dict[str, Any]]) -> None:
    """Write a stable JSON report for the logical ingestion date."""
    report = log_dir / f"ingest_raw_{run_date}.json"
    report.write_text(json.dumps(results, indent=2, ensure_ascii=True), encoding="utf-8")


def main() -> int:
    """Run ingestion and return a process exit status."""
    try:
        run()
    except (FileNotFoundError, ValueError, OSError) as exc:
        get_logger().error("Raw ingestion failed", extra={"event_data": {"error": str(exc)}})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
