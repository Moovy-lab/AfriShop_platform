# Pipeline de données Curated

## 1. Objectif et perimetre

Ce document présente la couche Curated mise en place pour AfriShop. Elle lit les données issues de l'ingestion Raw, harmonise certains types, applique des contrôles propres à chaque table, dirige les lignes rejetées vers la quarantaine et alimente des tables Delta Curated par mise à jour ou insertion selon leur clé.

L'implémentation couvre six domaines : produits, clients, commandes, lignes de commande, paiements et livraisons. Elle comprend également la configuration Spark partagée, la sélection des sources, les entrées-sorties Delta, les journaux structurés et le hachage salé des coordonnées client.

## 2. Déroulement du traitement

Chaque traitement suit les grandes etapes suivantes :

1. Créer ou réutiliser une session Spark configurée pour Delta Lake et le fuseau UTC.
2. Lire le jeu de données Parquet Raw lorsqu'il est disponible ; sinon, lire le fichier CSV correspondant. L'option `--csv` force l'utilisation du CSV lorsqu'il existe.
3. Convertir les dates et les colonnes numériques prises en charge, puis appliquer les règles métier.
4. Séparer les lignes valides des lignes rejetées. Chaque rejet reçoit un code dans la colonne `reason` ; plusieurs codes sont assemblés avec `|`.
5. Insérer ou mettre à jour les lignes valides dans la table Delta Curated et remplacer l'instantané de quarantaine de la table.
6. Émettre dans la sortie standard un événement JSON avec les volumes, la durée, la table, le traitement et l'identifiant d'exécution.

Les composants partagés se trouvent dans `src/spark_session.py`, `src/curated_io.py`, `src/pii.py` et `src/curated/_common.py`. Les transformations propres aux tables et leurs points d'entrée sont dans `src/curated/`.

## 3. Regles appliquees par table

| Table | Cle | Transformations et controles qualite |
|---|---|---|
| `products` | `product_id` | Convertit le coût et le prix en décimal ; convertit `is_perishable` si la colonne existe ; calcule `is_negative_margin` ; place en quarantaine les lignes dont le prix vaut zéro (`unit_price_zero`). |
| `customers` | `customer_id` | Convertit la date d'inscription et l'année de naissance ; normalise et hache avec un sel les champs email et téléphone ; relie les clients partageant le même email haché via `customer_master_id` ; place en quarantaine les enregistrements conflictuels partageant un identifiant client (`customer_id_collision`). |
| `orders` | `order_id` | Convertit les dates et montants ; conserve la version la plus récente selon la date disponible ; dérive `order_year_month` ; place en quarantaine les totaux différant de sous-total + livraison - remise de plus de 0,05 (`order_total_mismatch`). |
| `order_lines` | `order_line_id` | Convertit la quantité et les montants ; déduplique ; place en quarantaine les produits inconnus (`unknown_product_id`) et les montants de ligne différant de quantité x prix unitaire - remise de plus de 0,05 (`line_total_mismatch`). Les deux motifs peuvent apparaître sur une même ligne. |
| `payments` | `payment_id` | Convertit la date et le montant du paiement ; déduplique les identifiants de paiement répétés. Les tentatives distinctes ayant des identifiants différents sont conservées. Aucun autre motif de quarantaine n'est actuellement configuré. |
| `deliveries` | `delivery_id` | Convertit les horodatages et les mesures de livraison ; déduplique ; place en quarantaine les livraisons horodatées avant leur expédition (`delivered_before_shipped`). |

La déduplication choisit la valeur la plus récente parmi les horodatages disponibles (`updated_at`, `payment_date`, `delivered_at`, `shipped_at`, `created_at`). Un hachage stable sert à départager les ex aequo. Les mises à jour Curated conservent également une seule ligne déterministe par clé.

## 4. Organisation du stockage

Le répertoire racine des données est `data` par défaut. Il peut être remplacé avec `DATA_DIR`.

| Jeu de données | Chemin par défaut |
|---|---|
| Entree Parquet Raw | `data/lakehouse/raw/<table>` |
| Repli CSV Raw | `data/raw/<table>.csv` |
| Sortie Delta Curated | `data/lakehouse/curated/<table>` |
| Instantane Delta de quarantaine | `data/lakehouse/quarantine/<table>` |

Les données Curated sont écrites avec Delta `MERGE` : les lignes dont la clé existe sont mises à jour et les nouvelles clés sont insérées. La table des commandes est partitionnée par `order_year_month` lors de sa création initiale. Chaque traitement remplace l'instantané de quarantaine de sa table. Celui-ci représente donc les rejets de la dernière exécution et ne constitue pas un historique append-only.

## 5. Configuration et protection des données personnelles

Les variables d'environnement utilisees par cette implementation sont :

| Variable | Role |
|---|---|
| `DATA_DIR` | Répertoire racine des jeux Raw, Curated et de quarantaine ; vaut `data` par défaut. |
| `SPARK_MASTER_URL` | Active le mode cluster et indique le maître Spark. En son absence, Spark utilise `local[*]`. |
| `SPARK_SHUFFLE_PARTITIONS` | Nombre de partitions de shuffle Spark SQL ; vaut `4` par défaut. |
| `PII_HASH_SALT` | Secret obligatoire et non vide utilisé pour hacher les coordonnées client. |

Avant de traiter les données clients, configurez un `PII_HASH_SALT` robuste et privé dans l'environnement local ou dans un gestionnaire de secrets. Ne placez pas un sel de production dans le dépôt, les journaux ou ce document. `.env.example` ne contient qu'une valeur indicative. Le hachage supprime les espaces de début et de fin, convertit les valeurs en minuscules, puis calcule un condensat SHA-256 sur la concaténation du sel et de la valeur normalisée. Les valeurs nulles restent nulles. Le même sel doit être conservé de manière sécurisée pour que les hachages restent comparables entre les exécutions.

## 6. Execution des traitements

Depuis la racine du dépôt, utilisez l'environnement Python du projet et ses dépendances. Définissez `PYTHONPATH=src` afin que les modules soient résolus correctement.

```bash
PYTHONPATH=src python -m curated.products
PYTHONPATH=src python -m curated.customers
PYTHONPATH=src python -m curated.orders
PYTHONPATH=src python -m curated.order_lines
PYTHONPATH=src python -m curated.payments
PYTHONPATH=src python -m curated.deliveries
```

Ajoutez `--csv` à la commande d'un module pour privilégier le CSV correspondant. Le traitement des lignes de commande lit aussi les produits afin de vérifier les identifiants référencés ; la source Raw des produits doit donc être disponible. Pour traiter les clients en local, exportez `PII_HASH_SALT` avant de lancer le job customers.

## 7. Journaux et suivi operationnel

À la fin de chaque traitement, un objet JSON est écrit dans la sortie standard. Il contient `timestamp`, `level`, `job`, `table`, `event`, `rows_in`, `rows_valid`, `rows_quarantined`, `rows_deduplicated`, `duration_seconds` et `run_id`. Les journaux sont destinés à l'orchestrateur et ne contiennent pas les lignes sources ou celles de la quarantaine.

Suivez les volumes traités et la répartition des motifs de quarantaine afin de repérer les changements d'entrée et les problèmes de qualité. Comme la sortie de quarantaine est remplacée à chaque exécution, conservez les journaux de l'orchestrateur ou exportez séparément les instantanés si un historique d'audit est nécessaire.

## 8. Tests et validation

Les tests unitaires couvrent les règles des six transformations, le choix des doublons, l'idempotence des mises à jour produits, les intégrations CSV/Parquet produits lorsque les sources sont présentes, ainsi que le comportement du hachage PII. Depuis la racine du dépôt, exécutez :

```bash
PYTHONPATH=src pytest -q tests/test_curated_jobs.py tests/test_pii.py
```

Pour la validation de livraison, 12 tests ont réussi ; Black, isort, Ruff, la compilation Python et `git diff --check` ont également réussi. Une exécution d'intégration sur les données Raw disponibles a traité les volumes suivants :

| Table | Lignes en entree | Lignes valides | Lignes en quarantaine |
|---|---:|---:|---:|
| Produits | 12 000 | 11 881 | 119 |
| Clients | 81 200 | 81 190 | 10 |
| Commandes | 52 000 | 50 000 | 500 |
| Lignes de commande | 107 517 | 105 409 | 2 108 |
| Paiements | 46 141 | 46 141 | 0 |
| Livraisons | 41 218 | 39 358 | 1 860 |

L'intégration complète a utilisé Spark 3.5.0 avec Delta Lake 3.1.0, la combinaison compatible disponible dans l'environnement de test. La combinaison cible du projet, Spark 3.5.3 et Delta Lake 3.2.1, n'a pas été validée pendant cette exécution ; elle reste à vérifier dans l'environnement de déploiement avant la mise en production.

## 9. Limites actuelles

- La quarantaine est un instantané remplaçable par table, et non un journal d'audit append-only.
- Les journaux donnent les volumes globaux et la durée, mais ne comptent pas actuellement les rejets par motif.
- L'implémentation attend les chemins ou noms de fichiers indiqués ci-dessus ainsi que les colonnes métier utilisées par chaque transformation.
- Les six traitements sont des points d'entrée Python indépendants. La planification, les tentatives, les dépendances et les alertes relèvent de la couche d'orchestration.


## Validation avec l image cible

Le 3 octobre 2026, l image `afrishop/airflow:2.9.3` a ete construite avec Python 3.11, Spark 3.5.3 et Delta Lake 3.2.1. La commande `pytest -q` dans cette image a termine avec 26 tests reussis et 2 echecs; la couverture mesuree est de 83,57 %, au-dessus du seuil de 60 %. Un echec vient du cas parametre `products` de la nouvelle verification d idempotence; le second concerne le DagBag, qui tente de joindre PostgreSQL absent du run isole. Ces resultats ne constituent pas une execution de reference end-to-end.

`dbt parse --no-partial-parse` a aussi reussi avec dbt Core 1.8.10 et dbt-postgres 1.8.2, sans base. Aucun run complet du DAG, chargement PostgreSQL, test dbt contre les donnees, catalogue dbt ni capture UI n a ete produit pendant cette verification. Les volumes historiques plus haut dans ce document proviennent d un autre environnement et restent a revalider dans un run unique sur l image cible.
