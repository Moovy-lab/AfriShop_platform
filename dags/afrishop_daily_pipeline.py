"""Daily AfriShop orchestration from synthetic CSV files through dbt."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from monitoring import failure_event

log = logging.getLogger("afrishop.dag")

AIRFLOW_HOME = "/opt/airflow"
DBT_DIR = f"{AIRFLOW_HOME}/dbt"
CURATED_TABLES = ("products", "customers", "orders", "order_lines", "payments", "deliveries")
SPARK_LOCAL_ENV = {"SPARK_MASTER_URL": ""}


def failure_callback(context: dict[str, Any]) -> None:
    """Log an Airflow task failure as a JSON object."""
    log.error(json.dumps(failure_event(context), ensure_ascii=True))


default_args = {
    "owner": "afrishop",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(hours=2),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="afrishop_daily_pipeline",
    description="Daily AfriShop pipeline: raw, curated, PostgreSQL, and dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["afrishop", "lakehouse"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            "INGESTION_DATE={{ ds }} "
            f"INGEST_LOG_DIR={AIRFLOW_HOME}/logs/ingest "
            f"python {AIRFLOW_HOME}/src/ingest_raw.py"
        ),
    )

    previous = ingest_raw
    for table in CURATED_TABLES:
        task = BashOperator(
            task_id=f"curated_{table}",
            bash_command=f"cd {AIRFLOW_HOME} && python -m curated.{table}",
            env=SPARK_LOCAL_ENV,
            append_env=True,
        )
        previous >> task
        previous = task

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {AIRFLOW_HOME} && python -m load_to_postgres",
    )
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_DIR} && $DBT_BIN run --select staging intermediate",
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && $DBT_BIN snapshot",
    )
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_DIR} && $DBT_BIN run --select marts",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && $DBT_BIN test",
    )

    previous >> load_postgres >> dbt_staging >> dbt_snapshot >> dbt_marts >> dbt_test
