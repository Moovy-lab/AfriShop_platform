# Changelog

Toutes les modifications notables de ce projet sont documentees ici. Le format suit [Keep a Changelog](https://keepachangelog.com/fr/1.1.0/) et le versionnage suit SemVer.

## [1.0.0] - Unreleased

### Added
- Pipeline local de donnees synthetiques avec Airflow, Spark, Delta Lake, PostgreSQL et dbt.
- Tests et controles de qualite, documentation d'exploitation et dictionnaire de donnees.

### Changed
- Ingestion Raw rejouable par date logique et partition remplacee de maniere idempotente.

## Publication d'une version

Mettre a jour cette section et valider la CI sur la branche de livraison avant de creer le tag annote `v1.0.0` : `git tag -a v1.0.0 -m "Release v1.0.0"`. Pousser un tag requiert une validation explicite du responsable du depot.
