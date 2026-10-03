# Dictionnaire de donnees

Les six fichiers d'entree sont des CSV synthetiques dans `data/raw/`. L'ingestion Pandas conserve les types inferes par le CSV, puis les traitements Curated convertissent les champs numeriques et temporels documentes ci-dessous. Les colonnes source Raw incluent `_ingested_at` (timestamp UTC stable pour la date logique) et `_source_file` (string).

## Sources et tables Curated

| Source / table | Colonnes CSV (types Raw) | Types ou derivees Curated |
|---|---|---|
| `orders` | `order_id`, `order_number`, `customer_id`, `order_status`, `channel`, `country_code`, adresses, `promo_code`, `currency`: string; `order_date`, `created_at`, `updated_at`: date/timestamp; `subtotal_amount`, `shipping_amount`, `discount_amount`, `total_amount`: decimal(18,2) | `order_year_month`: string; cles et attributs conserves; montant en decimal(18,2) |
| `order_lines` | `order_line_id`, `order_id`, `product_id`, `product_sku`, `seller_id`: string; `quantity`: integer; `unit_price`, `line_discount`, `line_total`: decimal(18,2) | Memes champs types; `reason`: string uniquement en quarantaine |
| `customers` | identifiants, hashes contact, noms, genre, pays, ville, segment, moyen de paiement, fidelite et statut: string; `birth_year`: integer; `registration_date`: date/timestamp | `email_hash`, `phone_hash`: SHA-256 hex string; `customer_master_id`: string |
| `products` | identifiants, SKU, libelles, categorie, marque, vendeur et statut: string; `unit_cost`, `unit_price`: decimal(18,2); `weight_grams`: entier; `is_perishable`: boolean; dates: timestamp | `is_negative_margin`: boolean |
| `payments` | identifiants, moyen, fournisseur, devise, statut, reference: string; `payment_amount`: decimal(18,2); `payment_date`: timestamp | Valeurs dedupliquees par `payment_id` |
| `deliveries` | identifiants, transporteur, suivi et statut: string; dates: timestamp; `delivery_attempts`: integer; `delivery_cost`: decimal(18,2) | Valeurs dedupliquees par `delivery_id` |

Chaque table Curated est une table Delta sous `data/lakehouse/curated/<table>`. Les colonnes `reason`, `_source_batch` et `_quarantined_at` sont ajoutees a la quarantaine selon le job. Les codes disponibles sont `unit_price_zero`, `customer_id_collision`, `order_total_mismatch`, `unknown_product_id`, `line_total_mismatch` et `delivered_before_shipped`. Les paiements n'ont pas de regle de rejet specifique configuree.

## Modeles dbt

| Couche | Modeles | Principales colonnes et types logiques |
|---|---|---|
| Staging | `stg_orders`, `stg_order_lines`, `stg_customers`, `stg_products`, `stg_payments`, `stg_deliveries`, `stg_sellers`, `stg_refunds` | Colonnes normalisees des sources; identifiants string, dates timestamp/date, montants decimal, devises string |
| Intermediate | `int_payments_final`, `int_deliveries_by_order`, `int_order_totals`, `int_orders_enriched` | Agregats par commande; comptes integer, montants decimal, indicateurs boolean, cles string |
| Snapshots | `snap_customer`, `snap_product` | Colonnes du modele source, `dbt_valid_from` et `dbt_valid_to` timestamp, `dbt_scd_id` et `dbt_updated_at` string/timestamp |
| Marts | `fact_order_line`, `dim_customer`, `dim_product`, `dim_date`, `dim_payment_method`, `dim_delivery_status`, `agg_customer_rfm`, `agg_product_rotation`, `agg_category_return_rate` | Cles string, dates date, comptes integer, indicateurs boolean, mesures numeric/decimal et `currency` string |

Le schema exact de chaque modele doit etre genere a partir de la base apres un run de reference avec `make docs` (commande dbt docs generate). Le catalogue dbt resultant documente les types PostgreSQL resolus par le warehouse; il n'est pas inclus ici car aucun catalogue n'a ete genere pendant cette revision.

Les aggregations financieres conservent la devise dans leurs cles de groupement. Il n'y a pas de conversion ni de somme entre XOF, GHS et NGN.
