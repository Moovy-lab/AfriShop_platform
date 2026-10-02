"""DAG quotidien AfriShop : CSV -> raw -> curated (Delta) -> Postgres -> dbt.

Chaîne :
    ingest_raw -> curated_<table> (x6, en séquence) -> load_postgres
        -> dbt_staging -> dbt_snapshot -> dbt_marts -> dbt_test

Les commandes tournent dans le conteneur Airflow : chemins absolus obligatoires,
car BashOperator démarre dans un dossier temporaire.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator

log = logging.getLogger("afrishop.dag")

AIRFLOW_HOME = "/opt/airflow"
DBT_DIR = f"{AIRFLOW_HOME}/dbt"
# Ordre imposé : order_lines vérifie les produits, donc products passe en premier
CURATED_TABLES = ["products", "customers", "orders", "order_lines", "payments", "deliveries"]

# Mode Spark local : la VM Docker (3,6 Gi) ne peut pas héberger driver + executors
SPARK_LOCAL_ENV = {"SPARK_MASTER_URL": ""}


def failure_callback(context: dict[str, Any]) -> None:
    """Journalise l'échec d'une tâche au format JSON."""
    ti = context["task_instance"]
    log.error(
        json.dumps(
            {
                "event": "task_failed",
                "dag_id": ti.dag_id,
                "task_id": ti.task_id,
                "run_id": context.get("run_id"),
                "try_number": ti.try_number,
                "exception": str(context.get("exception")),
            }
        )
    )


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
    description="Pipeline quotidien AfriShop : raw, curated, Postgres, dbt",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["afrishop", "lakehouse"],
) as dag:

    # Nécessite la version de ingest_raw.py qui lit DATA_DIR (déjà défini dans le conteneur),
    # INGESTION_DATE (date logique du DAG : permet de rejouer une journée passée)
    # et INGEST_LOG_DIR (dossier de logs accessible en écriture).
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command=(
            "INGESTION_DATE={{ ds }} "
            f"INGEST_LOG_DIR={AIRFLOW_HOME}/logs/ingest "
            f"python {AIRFLOW_HOME}/src/ingest_raw.py"
        ),
    )

    # Une tâche par table, en séquence : plusieurs sessions Spark en parallèle
    # sur le même worker saturent la mémoire.
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

    # $DBT_BIN et DBT_PROFILES_DIR sont définis dans l'environnement du conteneur.
    # Ordre : les snapshots lisent le staging, les dimensions lisent les snapshots.
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