from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


def failure_callback(context):
    print("Une tâche du DAG a échoué.")


with DAG(
    dag_id="afrishop_daily_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    retries=2,
    on_failure_callback=failure_callback,
) as dag:


    

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command="python src/ingest_raw.py",
    )

    curated = BashOperator(
        task_id="curated",
        bash_command="""
            PYTHONPATH=src python -m curated.products &&
            PYTHONPATH=src python -m curated.customers &&
            PYTHONPATH=src python -m curated.orders &&
            PYTHONPATH=src python -m curated.order_lines &&
            PYTHONPATH=src python -m curated.payments &&
            PYTHONPATH=src python -m curated.deliveries
        """,
    )

    load_postgres = BashOperator(
        task_id="load_postgres",
        bash_command="python src/load_to_postgres.py",
    )



    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd dbt && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd dbt && dbt test",
    )
    ingest_raw >> curated >> load_postgres >> dbt_run >> dbt_test