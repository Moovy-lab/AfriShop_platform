# Captures a fournir

Deposer ici les captures reelles suivantes apres un run de reference. Ne pas fabriquer ni anonymiser les captures avec des donnees non synthetiques.

1. `airflow-dag-success.png`: interface Airflow, DAG `afrishop_daily_pipeline`, run termine et taches vertes. Afficher les dates d'execution sans exposer les identifiants ou secrets.
2. `dbt-lineage.png`: page dbt docs avec le graphe staging -> intermediate/snapshots -> marts.

Dans Airflow, ouvrir le DAG puis Grid ou Graph, selectionner le run termine et faire une capture lisible. Pour dbt, executer `make docs`, servir `dbt/target` avec `python -m http.server 8085 --directory dbt/target`, ouvrir `http://localhost:8085` et capturer le graphe. Ajouter les images dans ce dossier puis les integrer au README.
