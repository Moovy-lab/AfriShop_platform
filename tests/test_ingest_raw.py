"""Tests for reproducible raw ingestion."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd
import pytest

from ingest_raw import JsonFormatter, run


def test_ingestion_replay_replaces_same_logical_partition(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    pd.DataFrame({"order_id": ["o1", "o2"]}).to_csv(raw_dir / "orders.csv", index=False)
    output_root = tmp_path / "lakehouse" / "raw"
    logs = tmp_path / "logs"

    first = run(
        raw_dir=raw_dir,
        lakehouse_dir=output_root,
        ingestion_date="2026-01-02",
        log_dir=logs,
        sources=("orders",),
    )
    second = run(
        raw_dir=raw_dir,
        lakehouse_dir=output_root,
        ingestion_date="2026-01-02",
        log_dir=logs,
        sources=("orders",),
    )

    target = output_root / "orders" / "ingestion_date=2026-01-02" / "data.parquet"
    stored = pd.read_parquet(target)
    assert first[0]["rows"] == second[0]["rows"] == len(stored) == 2
    assert stored["order_id"].tolist() == ["o1", "o2"]
    assert json.loads((logs / "ingest_raw_2026-01-02.json").read_text()) == second


def test_missing_required_source_fails_and_writes_json_report(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="orders"):
        run(
            raw_dir=raw_dir,
            lakehouse_dir=tmp_path / "lakehouse",
            ingestion_date="2026-01-02",
            log_dir=tmp_path / "logs",
            sources=("orders",),
        )

    report = json.loads((tmp_path / "logs/ingest_raw_2026-01-02.json").read_text())
    assert report[0]["status"] == "missing"


def test_ingestion_log_formatter_emits_valid_json() -> None:
    record = logging.LogRecord("ingest", logging.INFO, "ingest_raw.py", 1, "loaded", (), None)
    record.event_data = {"source": "orders", "rows": 2}

    payload = json.loads(JsonFormatter().format(record))

    assert payload["message"] == "loaded"
    assert payload["source"] == "orders"
    assert payload["rows"] == 2
