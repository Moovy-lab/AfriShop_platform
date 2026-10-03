# Runbook AfriShop

## Demarrage et arret

Depuis la racine, `make up` cree la configuration locale si necessaire, construit les images, puis demarre la pile. Verifier ensuite `docker compose ps` et ouvrir Airflow sur le port `AIRFLOW_WEB_PORT`. Activer le DAG `afrishop_daily_pipeline` dans l'interface avant de le lancer.

Arreter les services sans supprimer les donnees avec `make down`. `make clean` supprime aussi les volumes Compose et donc la base PostgreSQL; cette commande n'est pas une commande d'arret courante.

## Rejouer une date logique

Le DAG passe `{{ ds }}` a `INGESTION_DATE`. Pour creer un run avec une date logique explicite :

```bash
docker compose exec airflow-scheduler airflow dags trigger afrishop_daily_pipeline --exec-date 2026-01-02T00:00:00+00:00
```

Pour rejouer un intervalle, utiliser Airflow backfill :

```bash
docker compose exec airflow-scheduler airflow dags backfill afrishop_daily_pipeline --start-date 2026-01-02 --end-date 2026-01-02
```

La partition Raw pour la meme date est remplacee atomiquement, avec les memes lignes et la meme valeur `_ingested_at`. Les jobs Curated utilisent des MERGE Delta par cle, ce qui evite d'ajouter les memes cles lors d'un rejeu.

## Inspection de la quarantaine

Les rejets Delta sont stockes sous `/data/lakehouse/quarantine/<table>` dans Airflow, soit `data/lakehouse/quarantine/<table>` sur l'hote. Lire la table avec Spark :

```bash
docker compose exec airflow-worker bash -lc 'cd /opt/airflow && python -c "from spark_session import get_spark; s=get_spark(\"inspect-quarantine\", cluster=False); s.read.format(\"delta\").load(\"/data/lakehouse/quarantine/products\").groupBy(\"reason\").count().show(truncate=False); s.stop()"'
```

Les tables de quarantaine sont des instantanes remplaces a chaque traitement, pas un journal append-only. Exporter un instantane avant le run suivant si une conservation d'audit est requise.

## Retour arriere

1. Identifier le numero de version Delta a restaurer dans `_delta_log` ou avec l'historique Delta.
2. Restaurer chaque table affectee depuis Spark, en remplacant le chemin et la version :

   ```python
   from delta.tables import DeltaTable

   DeltaTable.forPath(spark, "/data/lakehouse/curated/products").restoreToVersion(12)
   ```

3. Recharger PostgreSQL depuis les tables restaurees : `docker compose exec airflow-worker bash -lc 'cd /opt/airflow && python -m load_to_postgres'`.
4. Reconstruire les modeles et snapshots dbt selon le perimetre touche. Executer `dbt run --full-refresh` seulement si les modeles incrementaux le demandent; les tables marts actuelles sont materialisees en table.
5. Executer `dbt test` puis verifier les volumes en base et les journaux Airflow.

La restauration Delta est par table. Aligner toutes les tables dependantes sur une version coherente avant de relancer le flux.

## Tableau d'incidents

| Incident | Symptomes | Cause probable | Action |
|---|---|---|---|
| Ingestion | `ingest_raw` echoue avec une source manquante | Un des six CSV requis manque dans `data/raw` | Verifier noms et montage des fichiers, puis relancer la date logique |
| OOM Spark | Worker tue ou job Spark interrompu | Memoire Docker insuffisante ou plusieurs jobs executent Spark en parallele | Verifier la memoire Docker, relancer une tache a la fois et consulter les logs worker |
| JDBC | `load_postgres` echoue a la connexion ou a l'ecriture | PostgreSQL indisponible, variables warehouse invalides ou table Delta absente | Verifier `docker compose ps`, la configuration locale et la table source Delta |
| Echec dbt test | `dbt_test` termine avec des tests en erreur | Valeurs ou relations non conformes dans les tables source | Examiner le resultat du test dbt, les sources curated et la quarantaine avant de relancer |
| Echec GX | Validation Great Expectations non nulle | Suite GX externe non synchronisee avec le schema ou les donnees | Examiner le rapport sous `gx_reports`; GX n'est pas actuellement une tache du DAG |
| DAG en pause | Aucun run planifie ne demarre | Airflow cree les nouveaux DAG en pause | Activer `afrishop_daily_pipeline` dans Airflow, puis declencher un run manuel |

## Option de ressources Spark

Le DAG force le mode local Spark. Les services `spark-master` et `spark-worker` restent actifs dans Compose aujourd'hui; les placer sous un profil `cluster` est une option a decider, car cela requiert aussi d'adapter le comportement du DAG et le stockage partage.
