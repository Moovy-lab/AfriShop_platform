"""Charge les tables Delta de la zone curated dans le schéma Postgres `curated`.

Lecture  : $DATA_DIR/lakehouse/curated/<table>   (DATA_DIR vaut /data dans le conteneur)
Écriture : schéma Postgres `curated`, table du même nom, via JDBC.

Le mode « overwrite » est combiné à truncate=true : la table est vidée puis
remplie, sans être supprimée. Les vues dbt (staging) qui en dépendent restent
donc valides, et relancer le chargement ne crée aucun doublon (idempotent).

Usage depuis le conteneur Airflow (dossier /opt/airflow, PYTHONPATH=/opt/airflow/src) :
    python -m load_to_postgres                    # les 6 tables
    python -m load_to_postgres orders products    # une sélection
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

from spark_session import get_spark

TABLES = ["orders", "order_lines", "customers", "products", "payments", "deliveries"]
TARGET_SCHEMA = "curated"


def get_logger() -> logging.Logger:
    """Logger au format JSON (python-json-logger, 2.x ou 3.x)."""
    logger = logging.getLogger("load_to_postgres")
    if not logger.handlers:
        try:
            from pythonjsonlogger.json import JsonFormatter
        except ImportError:
            from pythonjsonlogger.jsonlogger import JsonFormatter
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def jdbc_url() -> str:
    host = os.getenv("WAREHOUSE_DB_HOST", "postgres")
    port = os.getenv("WAREHOUSE_DB_PORT", "5433")
    name = os.environ["WAREHOUSE_DB_NAME"]
    return f"jdbc:postgresql://{host}:{port}/{name}"


def delta_path(table: str) -> Path:
    return Path(os.getenv("DATA_DIR", "data")) / "lakehouse" / "curated" / table


def load_table(spark: SparkSession, table: str, log: logging.Logger) -> int:
    """Charge une table Delta dans Postgres. Retourne le nombre de lignes écrites."""
    src = delta_path(table)
    if not (src / "_delta_log").exists():
        raise FileNotFoundError(
            f"Table Delta introuvable : {src}. Lancer d'abord le job curated '{table}'."
        )

    started = time.time()
    df = spark.read.format("delta").load(str(src))
    rows = df.count()

    (
        df.write.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", f"{TARGET_SCHEMA}.{table}")
        .option("user", os.environ["WAREHOUSE_DB_USER"])
        .option("password", os.environ["WAREHOUSE_DB_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .option("batchsize", 5000)
        .mode("overwrite")
        .save()
    )

    log.info(
        "table loaded",
        extra={
            "table": table,
            "rows_written": rows,
            "target": f"{TARGET_SCHEMA}.{table}",
            "duration_s": round(time.time() - started, 2),
        },
    )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Charge le curated Delta dans Postgres")
    parser.add_argument("tables", nargs="*", help=f"parmi : {', '.join(TABLES)} (défaut : toutes)")
    args = parser.parse_args(argv)
    tables = args.tables or TABLES
    unknown = [t for t in tables if t not in TABLES]
    if unknown:
        parser.error(f"table(s) inconnue(s) : {unknown}. Valeurs possibles : {TABLES}")

    log = get_logger()
    spark = get_spark("load_to_postgres")
    failures: list[str] = []

    for table in tables:
        try:
            load_table(spark, table, log)
        except Exception as exc:  # une table en échec ne bloque pas les autres
            failures.append(table)
            log.error("table load failed", extra={"table": table, "error": str(exc)})

    spark.stop()
    if failures:
        log.error("load finished with errors", extra={"failed_tables": failures})
        return 1
    log.info("load finished", extra={"tables": tables})
    return 0


if __name__ == "__main__":
    sys.exit(main())
