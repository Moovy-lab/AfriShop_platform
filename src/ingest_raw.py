"""
Ingestion des CSV bruts vers le lakehouse (Parquet partitionné).
Usage: python src/ingest_raw.py
"""
import json
import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd
import sys
import os
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

# --- Configuration ---
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
LAKEHOUSE_DIR = DATA_DIR / "lakehouse" / "raw"
LOGS_DIR = Path(os.getenv("INGEST_LOG_DIR", "logs"))
INGESTION_DATE = os.getenv("INGESTION_DATE", date.today().isoformat())

# Liste des sources attendues
SOURCES = ["orders", "order_lines", "customers",
           "products", "payments", "deliveries"]

# --- Logging ---
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / f"ingest_raw_{INGESTION_DATE}.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def ingest_source(source: str) -> dict:
    """Ingère un CSV source vers Parquet partitionné."""
    start = time.time()
    csv_path = RAW_DIR / f"{source}.csv"
    result = {
        "source": source,
        "ingestion_date": INGESTION_DATE,
        "status": "unknown",
        "rows": 0,
        "duration_sec": 0.0,
        "error": None,
    }

    if not csv_path.exists():
        logger.warning(f"[{source}] CSV introuvable : {csv_path}")
        result["status"] = "skipped"
        return result

    try:
        logger.info(f"[{source}] Lecture de {csv_path}")
        df = pd.read_csv(csv_path)

        # Ajout d'une colonne de traçabilité
        df["_ingested_at"] = pd.Timestamp.now("UTC")
        df["_source_file"] = csv_path.name

        # Chemin de sortie partitionné
        out_dir = LAKEHOUSE_DIR / source / f"ingestion_date={INGESTION_DATE}"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Écrasement de la partition 
        out_file = out_dir / "data.parquet"
        if out_file.exists():
            out_file.unlink()

        df.to_parquet(out_file, engine="pyarrow", compression="snappy")

        result["status"] = "success"
        result["rows"] = len(df)
        logger.info(f"[{source}] ✅ {len(df)} lignes → {out_file}")

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        logger.error(f"[{source}] ❌ Erreur : {e}")

    result["duration_sec"] = round(time.time() - start, 3)
    return result


def main():
    logger.info("=" * 60)
    logger.info(f"Démarrage ingestion RAW — date={INGESTION_DATE}")
    logger.info("=" * 60)

    results = [ingest_source(s) for s in SOURCES]

    # Écriture du log JSON
    log_file = LOGS_DIR / f"ingest_raw_{INGESTION_DATE}.json"
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    # Résumé
    total_rows = sum(r["rows"] for r in results)
    failed = [r["source"] for r in results if r["status"] == "failed"]
    logger.info(f"Total lignes ingérées : {total_rows}")
    if failed:
        logger.error(f"Sources en échec : {failed}")
        raise SystemExit(1)
    logger.info("✅ Ingestion terminée avec succès")


if __name__ == "__main__":
    main()