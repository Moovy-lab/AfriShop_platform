"""Tests for source selection, PostgreSQL loading, Airflow, and monitoring contracts."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import curated_io
from delta_utils import dedup_latest
from load_to_postgres import delta_path, jdbc_url, load_table
from monitoring import failure_event
from spark_session import get_spark


class _Reader:
    def parquet(self, path: str) -> str:
        return path


class _Spark:
    read = _Reader()


def test_delta_helper_keeps_latest_record(spark) -> None:
    frame = spark.createDataFrame(
        [("key-1", "2026-01-01"), ("key-1", "2026-01-02")],
        "record_id string, updated_at string",
    )

    latest = dedup_latest(frame, "record_id")

    assert latest.select("updated_at").first()[0] == "2026-01-02"


def test_cluster_spark_requires_master(monkeypatch) -> None:
    monkeypatch.delenv("SPARK_MASTER_URL", raising=False)

    with pytest.raises(ValueError, match="SPARK_MASTER_URL"):
        get_spark(cluster=True)


def test_read_source_uses_most_recent_ingestion_partition(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "lakehouse/raw/orders"
    (root / "ingestion_date=2026-01-01").mkdir(parents=True)
    (root / "ingestion_date=2026-01-03").mkdir(parents=True)
    (root / "ingestion_date=invalid").mkdir()
    monkeypatch.setattr(curated_io, "data_root", lambda: tmp_path)

    selected = curated_io.read_source(_Spark(), "orders")

    assert selected.endswith("ingestion_date=2026-01-03")


def test_jdbc_url_uses_environment_configuration(monkeypatch) -> None:
    monkeypatch.setenv("WAREHOUSE_DB_HOST", "warehouse")
    monkeypatch.setenv("WAREHOUSE_DB_PORT", "5544")
    monkeypatch.setenv("WAREHOUSE_DB_NAME", "analytics")

    assert jdbc_url() == "jdbc:postgresql://warehouse:5544/analytics"


def test_load_table_fails_when_delta_table_is_absent(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("DATA_DIR", str(tmp_path))

    with pytest.raises(FileNotFoundError, match="products"):
        load_table(None, "products", None)

    assert delta_path("products") == tmp_path / "lakehouse/curated/products"


def test_failure_event_is_json_serializable() -> None:
    context = {
        "task_instance": SimpleNamespace(
            dag_id="afrishop_daily_pipeline", task_id="ingest_raw", try_number=2
        ),
        "run_id": "scheduled__2026-01-02",
        "exception": RuntimeError("source missing"),
    }

    payload = failure_event(context)

    assert json.loads(json.dumps(payload)) == {
        "event": "task_failed",
        "dag_id": "afrishop_daily_pipeline",
        "task_id": "ingest_raw",
        "run_id": "scheduled__2026-01-02",
        "try_number": 2,
        "exception": "source missing",
    }


@pytest.mark.integration
def test_dag_imports_and_preserves_pipeline_contract() -> None:
    airflow = pytest.importorskip("airflow")
    from airflow.models import DagBag

    dag_path = Path(__file__).parents[1] / "dags"
    bag = DagBag(dag_folder=str(dag_path), include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("afrishop_daily_pipeline")
    assert dag is not None
    assert dag.task_dict["ingest_raw"].retries == 2
    assert dag.task_dict["ingest_raw"].sla.total_seconds() == 7200
    assert "products" in dag.task_dict["curated_products"].bash_command
    assert "order_lines" in dag.task_dict["curated_order_lines"].bash_command
    assert dag.task_dict["curated_products"].downstream_task_ids == {"curated_customers"}
    task_order = [task.task_id for task in dag.topological_sort()]
    assert task_order.index("curated_products") < task_order.index("curated_order_lines")
    assert airflow.__version__
